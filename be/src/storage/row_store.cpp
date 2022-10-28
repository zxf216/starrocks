// This file is made available under Elastic License 2.0.

#include "storage/row_store.h"

#include "column/chunk.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "storage/olap_define.h"
#include "storage/rocksdb_status_adapter.h"
#include "storage/row_store_encoder.h"

using rocksdb::DB;
using rocksdb::Options;
using rocksdb::WriteOptions;

namespace starrocks {

inline void EncodeFixed64(char* buf, uint64_t value) {
    memcpy(buf, &value, sizeof(value));
}

inline uint64_t DecodeFixed64(const char* ptr) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result)); // gcc optimizes this to a plain load
    return result;
}

inline rocksdb::Slice StripTimestampFromUserKey(const rocksdb::Slice& user_key, size_t ts_sz) {
    assert(user_key.size() >= ts_sz);
    return rocksdb::Slice(user_key.data(), user_key.size() - ts_sz);
}

inline rocksdb::Slice ExtractTimestampFromUserKey(const rocksdb::Slice& user_key, size_t ts_sz) {
    assert(user_key.size() >= ts_sz);
    return rocksdb::Slice(user_key.data() + user_key.size() - ts_sz, ts_sz);
}

class ComparatorWithU64TsImpl : public rocksdb::Comparator {
public:
    ComparatorWithU64TsImpl()
            : rocksdb::Comparator(/*ts_sz=*/sizeof(uint64_t)), cmp_without_ts_(rocksdb::BytewiseComparator()) {
        assert(cmp_without_ts_);
        assert(cmp_without_ts_->timestamp_size() == 0);
    }
    const char* Name() const override { return "ComparatorWithU64Ts"; }
    void FindShortSuccessor(std::string*) const override {}
    void FindShortestSeparator(std::string*, const rocksdb::Slice&) const override {}
    int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
        int ret = CompareWithoutTimestamp(a, b);
        size_t ts_sz = timestamp_size();
        if (ret != 0) {
            return ret;
        }
        // Compare timestamp.
        // For the same user key with different timestamps, larger (newer) timestamp
        // comes first.
        return -CompareTimestamp(ExtractTimestampFromUserKey(a, ts_sz), ExtractTimestampFromUserKey(b, ts_sz));
    }
    using Comparator::CompareWithoutTimestamp;
    int CompareWithoutTimestamp(const rocksdb::Slice& a, bool a_has_ts, const rocksdb::Slice& b,
                                bool b_has_ts) const override {
        const size_t ts_sz = timestamp_size();
        assert(!a_has_ts || a.size() >= ts_sz);
        assert(!b_has_ts || b.size() >= ts_sz);
        rocksdb::Slice lhs = a_has_ts ? StripTimestampFromUserKey(a, ts_sz) : a;
        rocksdb::Slice rhs = b_has_ts ? StripTimestampFromUserKey(b, ts_sz) : b;
        return cmp_without_ts_->Compare(lhs, rhs);
    }
    int CompareTimestamp(const rocksdb::Slice& ts1, const rocksdb::Slice& ts2) const override {
        assert(ts1.size() == sizeof(uint64_t));
        assert(ts2.size() == sizeof(uint64_t));
        uint64_t lhs = DecodeFixed64(ts1.data());
        uint64_t rhs = DecodeFixed64(ts2.data());
        if (lhs < rhs) {
            return -1;
        } else if (lhs > rhs) {
            return 1;
        } else {
            return 0;
        }
    }

private:
    const rocksdb::Comparator* cmp_without_ts_{nullptr};
};

RowStore::RowStore(std::string db_path) : _db_path(std::move(db_path)) {}

RowStore::~RowStore() {
    for (auto& handle : _handles) {
        delete handle;
    }
    if (_db != nullptr) {
        _db->Close();
        delete _db;
        _db = nullptr;
    }
}

Status RowStore::init() {
    Options options;
    options.IncreaseParallelism();
    options.create_missing_column_families = true;
    options.create_if_missing = true;
    /*
    if (support_mvcc) {
        // use prefix filter
        options.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(key_prefix_len_));
        // Enable prefix bloom for mem tables
        options.memtable_prefix_bloom_size_ratio = 0.1;
        // Setup bloom filter
        rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        table_options.whole_key_filtering = false;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    }*/
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs(RS_NUM_COLUMN_FAMILY_INDEX);
    cf_descs[0].name = ROWSTORE_NO_MVCC_CF;
    cf_descs[1].name = ROWSTORE_MVCC_CF;
    static ComparatorWithU64TsImpl comp_with_u64_ts;
    cf_descs[1].options.comparator = &comp_with_u64_ts;
    static_assert(RS_NUM_COLUMN_FAMILY_INDEX == 2);

    // open rocksdb
    rocksdb::Status s = DB::Open(options, _db_path, cf_descs, &_handles, &_db);
    if (!s.ok()) {
        return to_status(s);
    }
    if (_db == nullptr) {
        return Status::InternalError("RowStore rocksdb open failed");
    }
    return Status::OK();
}

Status RowStore::gc_version(int64_t version) {
    char buf[8];
    SliceUniquePtr ts_ptr = ver_to_slice(version, buf);
    // TODO(yixin) : upgrade rocksdb to v6.29.2 and use this async function
    //rocksdb::Status s = _db->IncreaseFullHistoryTsLow(_handles[RS_MVCC_INDEX], ts_ptr->ToString());
    rocksdb::CompactRangeOptions option;
    option.full_history_ts_low = ts_ptr.get();
    rocksdb::Status s = _db->CompactRange(option, _handles[RS_MVCC_INDEX], nullptr, nullptr);
    return to_status(s);
}

SliceUniquePtr RowStore::ver_to_slice(int64_t version, char* buf) {
    EncodeFixed64(buf, (uint64_t)version);
    return std::move(std::make_unique<rocksdb::Slice>(buf, 8));
}

void RowStore::multi_get_ver(const std::vector<std::string>& keys, const int64_t version,
                             std::vector<std::string>& values, std::vector<Status>& rets) {
    CHECK(_db != nullptr) << "invalid db";
    rocksdb::ColumnFamilyHandle* handle = _handles[RS_MVCC_INDEX];
    std::vector<rocksdb::ColumnFamilyHandle*> handles(keys.size(), handle);
    std::vector<rocksdb::Slice> key_slices;
    key_slices.reserve(keys.size());
    for (const auto& key : keys) {
        key_slices.emplace_back(key);
    }
    rocksdb::ReadOptions read_option;
    char buf[8];
    SliceUniquePtr ts_ptr = ver_to_slice(version, buf);
    read_option.timestamp = ts_ptr.get();
    std::vector<rocksdb::Status> ss = _db->MultiGet(read_option, handles, key_slices, &values, nullptr);
    rets.reserve(ss.size());
    for (const auto& s : ss) {
        rets.push_back(to_status(s));
    }
}

Status RowStore::get_chunk_ver(const std::vector<std::string>& keys, const vectorized::Schema& schema,
                               const int64_t version, vectorized::Chunk* chunk) {
    std::vector<std::string> values;
    std::vector<Status> rets;
    multi_get_ver(keys, version, values, rets);
    for (const auto& s : rets) {
        if (!s.ok()) {
            LOG(INFO) << "multi_get_ver: err: " << s.get_error_msg() << " version " << version;
            return s;
        }
    }
    RowStoreEncoder::kvs_to_chunk(keys, values, schema, chunk);
    return Status::OK();
}

Status RowStore::batch_put(rocksdb::WriteBatch& wb) {
    CHECK(_db != nullptr) << "invalid db";
    WriteOptions write_options;
    write_options.sync = true;
    rocksdb::Status s = _db->Write(write_options, &wb);
    return to_status(s);
}

void RowStore::multi_get(const std::vector<std::string>& keys, std::vector<std::string>& values,
                         std::vector<Status>& rets) {
    CHECK(_db != nullptr) << "invalid db";
    rocksdb::ColumnFamilyHandle* handle = _handles[RS_NO_MVCC_INDEX];
    std::vector<rocksdb::ColumnFamilyHandle*> handles(keys.size(), handle);
    std::vector<rocksdb::Slice> key_slices;
    key_slices.reserve(keys.size());
    for (const auto& key : keys) {
        key_slices.emplace_back(key);
    }
    std::vector<rocksdb::Status> ss = _db->MultiGet(rocksdb::ReadOptions(), handles, key_slices, &values, nullptr);
    rets.reserve(ss.size());
    for (const auto& s : ss) {
        rets.push_back(to_status(s));
    }
}

Status RowStore::get_chunk(const std::vector<std::string>& keys, const vectorized::Schema& schema,
                           vectorized::Chunk* chunk) {
    std::vector<std::string> values;
    std::vector<Status> rets;
    multi_get(keys, values, rets);
    for (const auto& s : rets) {
        if (!s.ok()) {
            return s;
        }
    }
    RowStoreEncoder::kvs_to_chunk(keys, values, schema, chunk);
    return Status::OK();
}

} // namespace starrocks
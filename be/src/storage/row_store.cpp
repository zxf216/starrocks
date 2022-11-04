// This file is made available under Elastic License 2.0.

#include "storage/row_store.h"

#include "column/chunk.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "storage/chunk_helper.h"
#include "storage/olap_define.h"
#include "storage/rocksdb_status_adapter.h"
#include "storage/row_store_encoder.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/tablet_schema.h"

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
    std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(8UL << 30); // 1GB
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(15, false));
    Options options;
    options.IncreaseParallelism();
    options.create_missing_column_families = true;
    options.create_if_missing = true;
    //options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
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
    cf_descs[0].options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    cf_descs[1].name = ROWSTORE_MVCC_CF;
    static ComparatorWithU64TsImpl comp_with_u64_ts;
    cf_descs[1].options.comparator = &comp_with_u64_ts;
    cf_descs[1].options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
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

Status RowStore::multi_get_ver(const int64_t version, const PTabletRowstoreMultigetRequest* request,
                               PTabletRowstoreMultigetResult* response) {
    CHECK(_db != nullptr) << "invalid db";
    rocksdb::ColumnFamilyHandle* handle = _handles[RS_MVCC_INDEX];
    std::vector<rocksdb::ColumnFamilyHandle*> handles(request->keys_size(), handle);
    std::vector<rocksdb::Slice> key_slices(request->keys_size());
    key_slices.reserve(request->keys_size());
    for (const auto& key : request->keys()) {
        key_slices.emplace_back(key);
    }
    rocksdb::ReadOptions read_option;
    std::vector<std::string> values(request->keys_size());
    char buf[8];
    SliceUniquePtr ts_ptr = ver_to_slice(version, buf);
    read_option.timestamp = ts_ptr.get();
    std::vector<rocksdb::Status> ss = _db->MultiGet(read_option, handles, key_slices, &values, nullptr);
    for (int i = 0; i < values.size(); i++) {
        if (!ss[i].ok()) {
            return to_status(ss[i]);
        }
        response->add_vals(values[i]);
    }
    return Status::OK();
}

Status RowStore::scan_ver(const int64_t version, PTabletRowstoreScanResult* response) {
    CHECK(_db != nullptr) << "invalid db";
    rocksdb::ReadOptions read_option;
    char buf[8];
    SliceUniquePtr ts_ptr = ver_to_slice(version, buf);
    read_option.timestamp = ts_ptr.get();
    rocksdb::Iterator* it = _db->NewIterator(read_option);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        response->add_keys(it->key().ToString());
        response->add_vals(it->value().ToString());
    }
    return to_status(it->status());
}

Status RowStore::get_chunk_ver(const std::vector<std::string>& keys, const TabletSchema& tablet_schema,
                               const vectorized::Schema& schema, const int64_t version, vectorized::Chunk* chunk) {
    std::vector<std::string> values;
    std::vector<Status> rets;
    std::string default_value;
    multi_get_ver(keys, version, values, rets);
    for (int i = 0; i < keys.size(); i++) {
        auto& s = rets[i];
        if (s.is_not_found()) {
            // use default value
            if (default_value.empty()) {
                if (!_build_default_value(tablet_schema, schema, default_value).ok()) {
                    return s;
                }
            }
            values[i] = default_value;
        } else if (!s.ok()) {
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

void RowStore::parell_multi_get(const std::vector<std::string>& keys, std::vector<std::string>& values,
                                std::vector<Status>& rets) {
    CHECK(_db != nullptr) << "invalid db";
    std::vector<std::thread> workers;
    std::vector<std::vector<std::string>> val_vec;
    std::vector<std::vector<Status>> status_vec;
    rocksdb::ColumnFamilyHandle* handle = _handles[RS_NO_MVCC_INDEX];
    std::vector<rocksdb::ColumnFamilyHandle*> handles(keys.size(), handle);
    const int batch_num = 200;
    val_vec.resize(keys.size() / batch_num + 1);
    status_vec.resize(keys.size() / batch_num + 1);
    for (int i = 0; i * batch_num < keys.size(); i++) {
        workers.emplace_back(
                [&](int index) {
                    int s = index * batch_num;
                    int e = ((index + 1) * batch_num - 1) >= keys.size() ? (keys.size() - 1)
                                                                         : ((index + 1) * batch_num - 1);
                    std::vector<rocksdb::Slice> key_slices;
                    key_slices.reserve(e - s + 1);
                    for (int j = s; j <= e; j++) {
                        key_slices.emplace_back(keys[j]);
                    }
                    std::vector<rocksdb::Status> ss =
                            _db->MultiGet(rocksdb::ReadOptions(), handles, key_slices, &val_vec[index], nullptr);
                    for (auto& each : ss) {
                        status_vec[index].push_back(to_status(each));
                    }
                },
                i);
    }
    for (int i = 0; i < workers.size(); i++) {
        workers[i].join();
        values.insert(values.end(), val_vec[i].begin(), val_vec[i].end());
        rets.insert(rets.end(), status_vec[i].begin(), status_vec[i].end());
    }
}

Status RowStore::_build_default_value(const TabletSchema& tablet_schema, const vectorized::Schema& schema,
                                      std::string& default_value) {
    auto chunk = ChunkHelper::new_chunk(schema, 1);
    for (int i = 0; i < tablet_schema.num_columns(); i++) {
        if (i < schema.num_key_fields()) continue;
        const auto& col = tablet_schema.column(i);
        if (!col.has_default_value()) {
            return Status::NotFound("_build_default_value failed");
        } else {
            const TypeInfoPtr& type_info = get_type_info(col);
            std::unique_ptr<DefaultValueColumnIterator> default_value_iter =
                    std::make_unique<DefaultValueColumnIterator>(col.has_default_value(), col.default_value(),
                                                                 col.is_nullable(), type_info, col.length(), 1);
            ColumnIteratorOptions iter_opts;
            RETURN_IF_ERROR(default_value_iter->init(iter_opts));
            size_t sz = 1;
            default_value_iter->next_batch(&sz, chunk->get_column_by_index(i).get());
        }
    }
    std::vector<std::string> tmp_strs;
    RowStoreEncoder::chunk_to_values(schema, *chunk, 0, 1, tmp_strs);
    default_value.swap(tmp_strs[0]);
    return Status::OK();
}

Status RowStore::get_chunk(const std::vector<std::string>& keys, const TabletSchema& tablet_schema,
                           const vectorized::Schema& schema, vectorized::Chunk* chunk) {
    std::vector<std::string> values;
    std::vector<Status> rets;
    std::string default_value;
    multi_get(keys, values, rets);
    for (int i = 0; i < keys.size(); i++) {
        auto& s = rets[i];
        if (s.is_not_found()) {
            // use default value
            if (default_value.empty()) {
                if (!_build_default_value(tablet_schema, schema, default_value).ok()) {
                    return s;
                }
            }
            values[i] = default_value;
        } else if (!s.ok()) {
            return s;
        }
    }
    RowStoreEncoder::kvs_to_chunk(keys, values, schema, chunk);
    return Status::OK();
}

} // namespace starrocks
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

RowStore::RowStore(std::string db_path) : _db_path(std::move(db_path)) {}

RowStore::~RowStore() {
    if (_db != nullptr) {
        _db->Close();
        delete _db;
        _db = nullptr;
    }
}

Status RowStore::init(const bool support_mvcc) {
    Options options;
    options.IncreaseParallelism();
    options.create_if_missing = true;
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
    }

    // open rocksdb
    rocksdb::Status s = DB::Open(options, _db_path, &_db);
    if (!s.ok()) {
        return to_status(s);
    }
    if (_db == nullptr) {
        return Status::InternalError("RowStore rocksdb open failed");
    }
    return Status::OK();
}

Status RowStore::batch_put(std::vector<std::string>& keys, const std::vector<std::string>& values, int64_t version) {
    CHECK(keys.size() == values.size()) << "rowstore batch_put invalid kv pairs";
    CHECK(_db != nullptr) << "invalid db";
    rocksdb::WriteBatch wb;
    for (int i = 0; i < keys.size(); i++) {
        RowStoreEncoder::combine_key_with_ver(keys[i], RS_PUT_OP, version);
        wb.Put(keys[i], values[i]);
    }
    WriteOptions write_options;
    write_options.sync = true;
    rocksdb::Status s = _db->Write(write_options, &wb);
    return to_status(s);
}

Status RowStore::batch_put(rocksdb::WriteBatch& wb) {
    if (_db == nullptr) {
        LOG(INFO) << "[ROWSTORE] batch_put invalid db size : " << wb.Count();
    }
    CHECK(_db != nullptr) << "invalid db";
    WriteOptions write_options;
    write_options.sync = true;
    LOG(INFO) << "[ROWSTORE] batch_put size : " << wb.Count();
    rocksdb::Status s = _db->Write(write_options, &wb);
    return to_status(s);
}

void RowStore::multi_get(const std::vector<std::string>& keys, std::vector<std::string>& values,
                         std::vector<Status>& rets) {
    std::vector<rocksdb::Slice> key_slices;
    key_slices.reserve(keys.size());
    for (const auto& key : keys) {
        key_slices.emplace_back(key);
    }
    std::vector<rocksdb::Status> ss = _db->MultiGet(rocksdb::ReadOptions(), key_slices, &values, nullptr);
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
    LOG(INFO) << "[ROWSTORE] get_chunk key size: " << keys.size() << " val size: " << values.size()
              << " chunk: " << chunk->debug_columns();
    return Status::OK();
}

} // namespace starrocks
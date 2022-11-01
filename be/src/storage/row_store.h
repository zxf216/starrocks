// This file is made available under Elastic License 2.0.

#pragma once

#include <rocksdb/write_batch.h>

#include <functional>
#include <map>
#include <string>
#include <string_view>

#include "common/status.h"
#include "storage/olap_common.h"

namespace rocksdb {
class DB;
class ColumnFamilyHandle;
} // namespace rocksdb

namespace starrocks {
class PTabletRowstoreScanResult;
class PTabletRowstoreMultigetRequest;
class PTabletRowstoreMultigetResult;
class TabletSchema;

namespace vectorized {
class Chunk;
class Schema;
class Column;
} // namespace vectorized

using ColumnFamilyHandle = rocksdb::ColumnFamilyHandle;
using WriteBatch = rocksdb::WriteBatch;
using SliceUniquePtr = std::unique_ptr<rocksdb::Slice>;

enum RowStoreColumnFamilyIndex {
    RS_NO_MVCC_INDEX = 0,
    RS_MVCC_INDEX = 1,
    RS_NUM_COLUMN_FAMILY_INDEX = 2,
};

static const std::string ROWSTORE_NO_MVCC_CF = "default";
static const std::string ROWSTORE_MVCC_CF = "mvcc_cf";

class RowStore {
public:
    explicit RowStore(std::string db_path);

    virtual ~RowStore();

    Status init();
    Status batch_put(rocksdb::WriteBatch& wb);
    rocksdb::ColumnFamilyHandle* cf_handle(RowStoreColumnFamilyIndex index) { return _handles[index]; }
    static SliceUniquePtr ver_to_slice(int64_t ver, char* buf);
    Status gc_version(int64_t ver);

    /// with mvcc
    // write key value pairs to rowstore with version
    void multi_get_ver(const std::vector<std::string>& keys, const int64_t version, std::vector<std::string>& values,
                       std::vector<Status>& rets);
    Status get_chunk_ver(const std::vector<std::string>& keys, const TabletSchema& tablet_schema,
                         const vectorized::Schema& schema, const int64_t version, vectorized::Chunk* chunk);
    Status scan_ver(const int64_t version, PTabletRowstoreScanResult* response);
    Status multi_get_ver(const int64_t version, const PTabletRowstoreMultigetRequest* request,
                         PTabletRowstoreMultigetResult* response);

    ///  without mvcc
    // write key value pairs to rowstore

    void multi_get(const std::vector<std::string>& keys, std::vector<std::string>& values, std::vector<Status>& rets);

    Status get_chunk(const std::vector<std::string>& keys, const TabletSchema& tablet_schema,
                     const vectorized::Schema& schema, vectorized::Chunk* chunk);
    Status get_chunk(const std::vector<std::string>& keys, const TabletSchema& tablet_schema,
                     const vectorized::Schema& schema, std::vector<uint32_t>& read_column_ids,
                     std::vector<std::unique_ptr<vectorized::Column>>& dest);

private:
    Status _build_default_value(const TabletSchema& tablet_schema, const vectorized::Schema& schema,
                                std::string& default_value);

private:
    std::string _db_path;
    rocksdb::DB* _db;
    std::vector<rocksdb::ColumnFamilyHandle*> _handles;
    //uint32_t key_prefix_len_{10};
};

} // namespace starrocks
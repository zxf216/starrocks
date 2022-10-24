
// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/row_store_encoder.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/schema.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/row_store.h"

using namespace std;

namespace starrocks {

static unique_ptr<vectorized::Schema> create_schema(const vector<pair<FieldType, bool>>& types) {
    vectorized::Fields fields;
    for (int i = 0; i < types.size(); i++) {
        string name = StringPrintf("col%d", i);
        auto fd = new vectorized::Field(i, name, types[i].first, false);
        fd->set_is_key(types[i].second);
        fd->set_aggregate_method(OLAP_FIELD_AGGREGATION_NONE);
        fields.emplace_back(fd);
    }
    return unique_ptr<vectorized::Schema>(new vectorized::Schema(std::move(fields)));
}

TEST(RowStoreEncoderTest, testEncodeInt) {
    auto schema = create_schema({{OLAP_FIELD_TYPE_INT, true},
                                 {OLAP_FIELD_TYPE_LARGEINT, true},
                                 {OLAP_FIELD_TYPE_INT, false},
                                 {OLAP_FIELD_TYPE_LARGEINT, false}});
    const int n = 100;
    auto pchunk = ChunkHelper::new_chunk(*schema, n);
    for (int i = 0; i < n; i++) {
        vectorized::Datum tmp;
        tmp.set_int32(i * 2343);
        vectorized::Datum tmp2;
        tmp2.set_int128(i * 2343);
        pchunk->columns()[0]->append_datum(tmp);
        pchunk->columns()[1]->append_datum(tmp2);
        pchunk->columns()[2]->append_datum(tmp);
        pchunk->columns()[3]->append_datum(tmp2);
    }
    std::vector<std::string> keys, values;
    RowStoreEncoder::chunk_to_keys(*schema, *pchunk, 0, n, keys);
    RowStoreEncoder::chunk_to_values(*schema, *pchunk, 0, n, values);
    ASSERT_EQ(keys.size(), n);
    ASSERT_EQ(values.size(), n);
    auto dchunk = pchunk->clone_empty_with_schema();
    RowStoreEncoder::kvs_to_chunk(keys, values, *schema, dchunk.get());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    ASSERT_EQ(pchunk->num_rows(), n);
    ASSERT_EQ(pchunk->num_columns(), 4);
    ASSERT_EQ(dchunk->num_columns(), 4);
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(1)->get(i).get_int128(),
                  dchunk->get_column_by_index(1)->get(i).get_int128());
        ASSERT_EQ(pchunk->get_column_by_index(2)->get(i).get_int32(),
                  dchunk->get_column_by_index(2)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(3)->get(i).get_int128(),
                  dchunk->get_column_by_index(3)->get(i).get_int128());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(1)->get(i).get_int128());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(2)->get(i).get_int32());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(3)->get(i).get_int128());
    }
}

TEST(RowStoreEncoderTest, testEncodeMix) {
    auto schema = create_schema({{OLAP_FIELD_TYPE_INT, true},
                                 {OLAP_FIELD_TYPE_VARCHAR, true},
                                 {OLAP_FIELD_TYPE_INT, false},
                                 {OLAP_FIELD_TYPE_BOOL, false}});
    const int n = 100;
    auto pchunk = ChunkHelper::new_chunk(*schema, n);
    for (int i = 0; i < n; i++) {
        vectorized::Datum tmp;
        string tmpstr = StringPrintf("slice000%d", i * 17);
        if (i % 5 == 0) {
            // set some '\0'
            tmpstr[rand() % tmpstr.size()] = '\0';
        }
        tmp.set_int32(i * 2343);
        pchunk->columns()[0]->append_datum(tmp);
        tmp.set_slice(tmpstr);
        pchunk->columns()[1]->append_datum(tmp);
        tmp.set_int32(i * 2343);
        pchunk->columns()[2]->append_datum(tmp);
        tmp.set_uint8(i % 2);
        pchunk->columns()[3]->append_datum(tmp);
    }
    std::vector<std::string> keys, values;
    RowStoreEncoder::chunk_to_keys(*schema, *pchunk, 0, n, keys);
    RowStoreEncoder::chunk_to_values(*schema, *pchunk, 0, n, values);
    ASSERT_EQ(keys.size(), n);
    ASSERT_EQ(values.size(), n);
    auto dchunk = pchunk->clone_empty_with_schema();
    RowStoreEncoder::kvs_to_chunk(keys, values, *schema, dchunk.get());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    ASSERT_EQ(pchunk->num_rows(), n);
    ASSERT_EQ(pchunk->num_columns(), 4);
    ASSERT_EQ(dchunk->num_columns(), 4);
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(1)->get(i).get_slice(),
                  dchunk->get_column_by_index(1)->get(i).get_slice());
        ASSERT_EQ(pchunk->get_column_by_index(2)->get(i).get_int32(),
                  dchunk->get_column_by_index(2)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(3)->get(i).get<uint8>(),
                  dchunk->get_column_by_index(3)->get(i).get<uint8>());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(i * 2343, dchunk->get_column_by_index(2)->get(i).get_int32());
        if (i % 5 != 0) {
            string tmpstr = StringPrintf("slice000%d", i * 17);
            ASSERT_EQ(tmpstr, dchunk->get_column_by_index(1)->get(i).get_slice().to_string());
        }
    }
}

TEST(RowStoreEncoderTest, testEncodeVersion) {
    for (int i = 0; i < 1000; i++) {
        string tmpstr = StringPrintf("slice000%d", i * 17);
        int8_t op = i % 2;
        int64_t ver = i * 1000;
        string buf = tmpstr;
        RowStoreEncoder::combine_key_with_ver(buf, op, ver);
        int8_t dop = 0;
        int64_t dver = 0;
        string dstr;
        RowStoreEncoder::split_key_with_ver(buf, dstr, dop, dver);
        ASSERT_EQ(op, dop);
        ASSERT_EQ(ver, dver);
        ASSERT_EQ(tmpstr, dstr);
    }
}

TEST(RowStoreEncoderTest, testMVCCRowStore) {
    std::string _root_path = "./ut_dir/kv_store_test";
    fs::remove_all(_root_path);
    fs::create_directories(_root_path);
    auto rs = std::make_unique<RowStore>(_root_path);
    Status s = rs->init();
    std::cout << "init " << s.get_error_msg() << std::endl;
    ASSERT_TRUE(s.ok());
    // write data with ver
    for (int ver = 1; ver <= 5; ver++) {
        rocksdb::WriteBatch wb(0, 0, 8);
        int64_t ver1 = ver * 100;
        for (int i = 0; i < 10; i++) {
            wb.Put(rs->cf_handle(RS_MVCC_INDEX), "key_" + std::to_string(i),
                   "val_" + std::to_string(i) + "_" + std::to_string(ver1));
        }

        char buf[8];
        SliceUniquePtr ptr = RowStore::ver_to_slice(ver1, buf);
        wb.AssignTimestamp(*ptr);
        Status s = rs->batch_put(wb);
        std::cout << "batch_put " << s.get_error_msg() << std::endl;
        ASSERT_TRUE(s.ok());
    }
    // read data with ver
    for (int ver = 1; ver <= 5; ver++) {
        rocksdb::WriteBatch wb;
        int64_t ver1 = ver * 100;
        std::vector<std::string> keys;
        for (int i = 0; i < 10; i++) {
            keys.push_back("key_" + std::to_string(i));
        }
        std::vector<std::string> vals;
        std::vector<Status> rets;
        rs->multi_get_ver(keys, ver1, vals, rets);
        ASSERT_EQ(vals.size(), keys.size());
        ASSERT_EQ(vals.size(), rets.size());
        for (int i = 0; i < vals.size(); i++) {
            ASSERT_TRUE(rets[i].ok());
        }
        for (int i = 0; i < vals.size(); i++) {
            ASSERT_EQ(vals[i], "val_" + std::to_string(i) + "_" + std::to_string(ver1));
        }
    }
    // gc version 100 & 200
    s = rs->gc_version(201);
    ASSERT_TRUE(s.ok());
    {
        // read 200
        std::vector<std::string> keys;
        for (int i = 0; i < 10; i++) {
            keys.push_back("key_" + std::to_string(i));
        }
        std::vector<std::string> vals;
        std::vector<Status> rets;
        rs->multi_get_ver(keys, 200, vals, rets);
        ASSERT_EQ(vals.size(), keys.size());
        ASSERT_EQ(vals.size(), rets.size());
        for (int i = 0; i < vals.size(); i++) {
            ASSERT_FALSE(rets[i].ok());
            ASSERT_TRUE(rets[i].is_not_found());
        }
    }
    {
        // read 300
        std::vector<std::string> keys;
        for (int i = 0; i < 10; i++) {
            keys.push_back("key_" + std::to_string(i));
        }
        std::vector<std::string> vals;
        std::vector<Status> rets;
        rs->multi_get_ver(keys, 300, vals, rets);
        ASSERT_EQ(vals.size(), keys.size());
        ASSERT_EQ(vals.size(), rets.size());
        for (int i = 0; i < vals.size(); i++) {
            ASSERT_TRUE(rets[i].ok());
        }
    }
}

} // namespace starrocks
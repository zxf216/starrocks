// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {

// op
#define RS_DEL_OP 0
#define RS_PUT_OP 1
// only use 56bit version
#define VER_MASK 0xFFFFFFFFFFFFFF
#define OP_MASK 0xFF
// key format:
// | raw_key | version 56bit(reserse) | OP 8bit | raw_key length 32bit |

inline int64_t encode_version(const int8_t op, const int64_t version) {
    return ((VER_MASK - (version & VER_MASK)) << 8) | ((int64_t)op & OP_MASK);
}

inline void decode_version(const int64_t raw_version, int8_t& op, int64_t& version) {
    version = VER_MASK - ((raw_version >> 8) & VER_MASK);
    op = (int8_t)(raw_version & OP_MASK);
}

class RowStoreEncoder {
public:
    static void chunk_to_keys(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset,
                              size_t len, std::vector<std::string>& keys);
    static void chunk_to_values(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset,
                                size_t len, std::vector<std::string>& values);
    static void encode_chunk_to_full_row_column(const vectorized::Schema& schema, const vectorized::Chunk& chunk,
                                                vectorized::BinaryColumn* dest_column);
    static void extract_columns_from_full_row_column(const vectorized::Schema& schema,
                                                     const vectorized::BinaryColumn& full_row_column,
                                                     const std::vector<uint32_t>& read_column_ids,
                                                     std::vector<std::unique_ptr<vectorized::Column>>& dest);
    static void encode_columns_to_full_row_column(const vectorized::Schema& schema,
                                                  const std::vector<vectorized::Column*>& columns,
                                                  vectorized::BinaryColumn& dest);
    static Status kvs_to_chunk(const std::vector<std::string>& keys, const std::vector<std::string>& values,
                               const vectorized::Schema& schema, vectorized::Chunk* dest);
    static void combine_key_with_ver(std::string& key, const int8_t op, const int64_t version);
    static Status split_key_with_ver(const std::string& ckey, std::string& key, int8_t& op, int64_t& version);
    static void pk_column_to_keys(vectorized::Schema& pkey_schema, vectorized::Column* column,
                                  std::vector<std::string>& keys);
};

} // namespace starrocks
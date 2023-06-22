/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/List/List.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/CRCHashFunction.hpp>
#include <x86intrin.h>
namespace NES::Nautilus::Interface {

HashFunction::HashValue CRCHashFunction::init() { return SEED; }

/**
 * @brief https://github.com/martinus/robin-hood-hashing/blob/fb1483621fda28d4afb31c0097c1a4a457fdd35b/src/include/robin_hood.h#L692
 * @param data
 * @param length
 * @return
 */
uint64_t hashBytesCRC(void* data, uint64_t length) {
    static constexpr uint64_t m = UINT64_C(0xc6a4a7935bd1e995);
    static constexpr uint64_t seed = UINT64_C(0xe17a1465);
    static constexpr unsigned int r = 47;

    auto const* const data64 = static_cast<const uint64_t*>(data);
    uint64_t h = seed ^ (length * m);

    size_t const n_blocks = length / 8;
    for (size_t i = 0; i < n_blocks; ++i) {
        auto k = *(data64 + i);

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    auto const* const data8 = reinterpret_cast<uint8_t const*>(data64 + n_blocks);
    switch (length & 7U) {
        case 7:
            h ^= static_cast<uint64_t>(data8[6]) << 48U;
            // FALLTHROUGH
        case 6:
            h ^= static_cast<uint64_t>(data8[5]) << 40U;
            // FALLTHROUGH
        case 5:
            h ^= static_cast<uint64_t>(data8[4]) << 32U;
            // FALLTHROUGH
        case 4:
            h ^= static_cast<uint64_t>(data8[3]) << 24U;
            // FALLTHROUGH
        case 3:
            h ^= static_cast<uint64_t>(data8[2]) << 16U;
            // FALLTHROUGH
        case 2:
            h ^= static_cast<uint64_t>(data8[1]) << 8U;
            // FALLTHROUGH
        case 1:
            h ^= static_cast<uint64_t>(data8[0]);
            h *= m;
            // FALLTHROUGH
        default: break;
    }

    h ^= h >> r;

    // final step
    h *= m;
    h ^= h >> r;
    return h;
}

template<typename T>
uint64_t hashValueCRC(uint64_t seed, T value) {
    uint64_t k = (uint64_t) value;
    uint64_t result1 = _mm_crc32_u64(seed, k);
    uint64_t result2 = _mm_crc32_u64(0x04c11db7, k);
    return ((result2 << 32) | result1) * 0x2545F4914F6CDD1Dull;
}

uint64_t hashTextValueCRC(uint64_t seed, TextValue* value) {
    // Combine two hashes by XORing them
    // As done by duckDB https://github.com/duckdb/duckdb/blob/09f803d3ad2972e36b15612c4bc15d65685a743e/src/include/duckdb/common/types/hash.hpp#L42
    return seed ^ hashBytesCRC((void*) value->c_str(), value->length());
}

HashFunction::HashValue CRCHashFunction::calculate(HashValue& hash, Value<>& value) {
    if (value->isType<Int8>()) {
        return FunctionCall("_ZN3NES8Nautilus9Interface12hashValueCRCIaEEmmT_", hashValueCRC<typename Int8::RawType>, hash, value.as<Int8>());
    } else if (value->isType<Int16>()) {
        return FunctionCall("_ZN3NES8Nautilus9Interface12hashValueCRCIsEEmmT_", hashValueCRC<typename Int16::RawType>, hash, value.as<Int16>());
    } else if (value->isType<Int32>()) {
        return FunctionCall("_ZN3NES8Nautilus9Interface12hashValueCRCIiEEmmT_", hashValueCRC<typename Int32::RawType>, hash, value.as<Int32>());
    } else if (value->isType<Int64>()) {
        return FunctionCall("_ZN3NES8Nautilus9Interface12hashValueCRCIlEEmmT_", hashValueCRC<typename Int64::RawType>, hash, value.as<Int64>());
    } else if (value->isType<UInt8>()) {
        return FunctionCall("_ZN3NES8Nautilus9Interface12hashValueCRCIhEEmmT_", hashValueCRC<typename UInt8::RawType>, hash, value.as<UInt8>());
    } else if (value->isType<UInt16>()) {
        return FunctionCall("_ZN3NES8Nautilus9Interface12hashValueCRCItEEmmT_", hashValueCRC<typename UInt16::RawType>, hash, value.as<UInt16>());
    } else if (value->isType<UInt32>()) {
        return FunctionCall("_ZN3NES8Nautilus9Interface12hashValueCRCIjEEmmT_", hashValueCRC<typename UInt32::RawType>, hash, value.as<UInt32>());
    } else if (value->isType<UInt64>()) {
        return FunctionCall("_ZN3NES8Nautilus9Interface12hashValueCRCImEEmmT_", hashValueCRC<typename UInt64::RawType>, hash, value.as<UInt64>());
    } else if (value->isType<Float>()) {
        return FunctionCall("_ZN3NES8Nautilus9Interface12hashValueCRCIfEEmmT_", hashValueCRC<typename Float::RawType>, hash, value.as<Float>());
    } else if (value->isType<Double>()) {
        return FunctionCall("_ZN3NES8Nautilus9Interface12hashValueCRCIdEEmmT_", hashValueCRC<typename Double::RawType>, hash, value.as<Double>());
    } else if (value->isType<Text>()) {
        return FunctionCall("hashTextValue", hashTextValueCRC, hash, value.as<Text>()->getReference());
    } else if (value->isType<List>()) {
        auto list = value.as<List>();
        for (auto listValue : list.getValue()) {
            calculate(hash, listValue);
        }
        return hash;
    }
    NES_NOT_IMPLEMENTED();
}

}// namespace NES::Nautilus::Interface
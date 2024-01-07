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
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>

namespace NES::Nautilus::Interface {

HashFunction::HashValue MurMur3HashFunction::init() { return SEED; }

/**
 * @brief Hash Function that implements murmurhas3 by Robin-Hood-Hashing:
 * https://github.com/martinus/robin-hood-hashing/blob/fb1483621fda28d4afb31c0097c1a4a457fdd35b/src/include/robin_hood.h#L748
 * @param x
 * @return
 */
uint64_t hashInt(uint64_t x) {
    x ^= x >> 33U;
    x *= UINT64_C(0xff51afd7ed558ccd);
    x ^= x >> 33U;
    x *= UINT64_C(0xc4ceb9fe1a85ec53);
    x ^= x >> 33U;
    return x;
}

/**
 * @brief https://github.com/martinus/robin-hood-hashing/blob/fb1483621fda28d4afb31c0097c1a4a457fdd35b/src/include/robin_hood.h#L692
 * @param data
 * @param length
 * @return
 */
uint64_t hashBytes(void* data, uint64_t length) {
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
uint64_t hashValue(uint64_t seed, T value) {
    // Combine two hashes by XORing them
    // As done by duckDB https://github.com/duckdb/duckdb/blob/09f803d3ad2972e36b15612c4bc15d65685a743e/src/include/duckdb/common/types/hash.hpp#L42
    return seed ^ hashInt(value);
}

uint64_t hashTextValue(uint64_t seed, TextValue* value) {
    // Combine two hashes by XORing them
    // As done by duckDB https://github.com/duckdb/duckdb/blob/09f803d3ad2972e36b15612c4bc15d65685a743e/src/include/duckdb/common/types/hash.hpp#L42
    return seed ^ hashBytes((void*) value->c_str(), value->length());
}

HashFunction::HashValue MurMur3HashFunction::calculate(Value<>& value) {
    auto hash = init();
    return calculate(hash, value);
}

HashFunction::HashValue MurMur3HashFunction::calculate(HashValue& hash, Value<>& value) {
    if (value->isType<Int8>()) {
        return FunctionCall("hashValueI8", hashValue<typename Int8::RawType>, hash, value.as<Int8>());
    } else if (value->isType<Int16>()) {
        return FunctionCall("hashValueI16", hashValue<typename Int16::RawType>, hash, value.as<Int16>());
    } else if (value->isType<Int32>()) {
        return FunctionCall("hashValueI32", hashValue<typename Int32::RawType>, hash, value.as<Int32>());
    } else if (value->isType<Int64>()) {
        return FunctionCall("hashValueI64", hashValue<typename Int64::RawType>, hash, value.as<Int64>());
    } else if (value->isType<UInt8>()) {
        return FunctionCall("hashValueUI8", hashValue<typename UInt8::RawType>, hash, value.as<UInt8>());
    } else if (value->isType<UInt16>()) {
        return FunctionCall("hashValueUI16", hashValue<typename UInt16::RawType>, hash, value.as<UInt16>());
    } else if (value->isType<UInt32>()) {
        return FunctionCall("hashValueUI32", hashValue<typename UInt32::RawType>, hash, value.as<UInt32>());
    } else if (value->isType<UInt64>()) {
        return FunctionCall("hashValueUI64", hashValue<typename UInt64::RawType>, hash, value.as<UInt64>());
    } else if (value->isType<Float>()) {
        return FunctionCall("hashValueF", hashValue<typename Float::RawType>, hash, value.as<Float>());
    } else if (value->isType<Double>()) {
        return FunctionCall("hashValueD", hashValue<typename Double::RawType>, hash, value.as<Double>());
    } else if (value->isType<Text>()) {
        return FunctionCall("hashTextValue", hashTextValue, hash, value.as<Text>()->getReference());
    } else if (value->isType<List>()) {
        auto list = value.as<List>();
        for (auto listValue : list.getValue()) {
            calculate(hash, listValue);
        }
        return hash;
    }
    NES_NOT_IMPLEMENTED();
}

HashFunction::HashValue MurMur3HashFunction::calculateWithState(HashFunction::HashValue&, Value<>&, Value<MemRef>&) {
    NES_THROW_RUNTIME_ERROR("This does not hash the value. Please use calculate().");
}

}// namespace NES::Nautilus::Interface
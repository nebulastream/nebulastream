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

#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::Nautilus::Interface {

uint64_t hashInt(uint64_t value, uint64_t h3Seeds[]) {
    uint64_t hash = 0;
    auto numberOfKeyBits = sizeof(uint64_t) * 8;
    for (auto i = 0UL; i < numberOfKeyBits; ++i) {
        bool isBitSet = (value >> i) & 1;
        if (isBitSet) {
            hash = hash ^ h3Seeds[i];
        }
    }

    return hash;
}

template<typename T>
uint64_t hashValue(uint64_t seed, T value, void* h3SeedsPtr) {
    NES_ASSERT2_FMT(h3SeedsPtr != nullptr, "h3SeedsPtr can not be NULL!");

    // Combine two hashes by XORing them
    // As done by duckDB https://github.com/duckdb/duckdb/blob/09f803d3ad2972e36b15612c4bc15d65685a743e/src/include/duckdb/common/types/hash.hpp#L42
    return seed ^ hashInt(value, static_cast<uint64_t*>(h3SeedsPtr));
}


HashFunction::HashValue H3Hash::init() {
    return 0UL;
}

HashFunction::HashValue H3Hash::calculate(HashFunction::HashValue& hash, Value<>& value) {
    auto h3SeedMemRef = Value<MemRef>((int8_t*)h3Seeds.data());

    if (value->isType<Int8>()) {
        return FunctionCall("hashValueI8", hashValue<typename Int8::RawType>, hash, value.as<Int8>(), h3SeedMemRef);
    } else if (value->isType<Int16>()) {
        return FunctionCall("hashValueI16", hashValue<typename Int16::RawType>, hash, value.as<Int16>(), h3SeedMemRef);
    } else if (value->isType<Int32>()) {
        return FunctionCall("hashValueI32", hashValue<typename Int32::RawType>, hash, value.as<Int32>(), h3SeedMemRef);
    } else if (value->isType<Int64>()) {
        return FunctionCall("hashValueI64", hashValue<typename Int64::RawType>, hash, value.as<Int64>(), h3SeedMemRef);
    } else if (value->isType<UInt8>()) {
        return FunctionCall("hashValueUI8", hashValue<typename UInt8::RawType>, hash, value.as<UInt8>(), h3SeedMemRef);
    } else if (value->isType<UInt16>()) {
        return FunctionCall("hashValueUI16", hashValue<typename UInt16::RawType>, hash, value.as<UInt16>(), h3SeedMemRef);
    } else if (value->isType<UInt32>()) {
        return FunctionCall("hashValueUI32", hashValue<typename UInt32::RawType>, hash, value.as<UInt32>(), h3SeedMemRef);
    } else if (value->isType<UInt64>()) {
        return FunctionCall("hashValueUI64", hashValue<typename UInt64::RawType>, hash, value.as<UInt64>(), h3SeedMemRef);
    } else if (value->isType<Float>()) {
        return FunctionCall("hashValueF", hashValue<typename Float::RawType>, hash, value.as<Float>(), h3SeedMemRef);
    } else if (value->isType<Double>()) {
        return FunctionCall("hashValueD", hashValue<typename Double::RawType>, hash, value.as<Double>(), h3SeedMemRef);
    }

    NES_NOT_IMPLEMENTED();
}

H3Hash::H3Hash(const std::vector<uint64_t> &h3Seeds, uint64_t numberOfKeyBits)  : h3Seeds(h3Seeds) {
    uint64_t mask = getBitMask(numberOfKeyBits);
    for (auto& seed : this->h3Seeds) {
        seed &= mask;
    }
}

    uint64_t H3Hash::getBitMask(uint64_t numberOfKeyBits) const {
        switch (numberOfKeyBits) {
            case  8: return 0xFF;
            case 16: return 0xFFFF;
            case 32: return 0xFFFFFFFF;
            case 64: return 0xFFFFFFFFFFFFFFFF;
            default: NES_THROW_RUNTIME_ERROR("getBitMask got a numberOfKeyBits that was not expected!");
        }
    }
} // namespace NES::Nautilus::Interface

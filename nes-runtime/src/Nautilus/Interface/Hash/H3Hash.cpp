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

#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArrayRef.hpp>
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

    // We do not want to cast, but rather just copy the bytes as-is
    uint64_t newValue = 0;
    std::memcpy(&newValue, &value, sizeof(T));

    // Combine two hashes by XORing them
    // As done by duckDB https://github.com/duckdb/duckdb/blob/09f803d3ad2972e36b15612c4bc15d65685a743e/src/include/duckdb/common/types/hash.hpp#L42
    return seed ^ hashInt(newValue, static_cast<uint64_t*>(h3SeedsPtr));
}

HashFunction::HashValue H3Hash::init() { return (uint64_t) 0UL; }

HashFunction::HashValue H3Hash::calculateWithState(HashFunction::HashValue& hash, Value<>& value, Value<MemRef>& state) {

    // As the bitwise operations are not supported on floating points, we have to change the value to an unsigned int
    // This is okay, as we are only interested in the bits as-is and not the represented value
    Nautilus::Value<> tmpValue(0);
    if (value->isType<Double>()) {
        tmpValue = Value<UInt64>(std::bit_cast<uint64_t>(value.as<Double>().getValue().getValue()));
    } else if (value->isType<Float>()) {
        tmpValue = Value<UInt32>(std::bit_cast<uint32_t>(value.as<Float>().getValue().getValue()));
    } else {
        tmpValue = value;
    }


    for (Value<UInt8> i((uint8_t) 0); i < numberOfKeyBits; i = i + 1) {
        auto isBitSet = (tmpValue >> i) & 1;
        auto h3SeedMemRef = (state + (entrySizeH3HashSeed * i)).as<MemRef>();
        auto h3Seed = h3SeedMemRef.load<UInt64>();
        hash = hash ^ (isBitSet * (h3Seed));
    }

    return hash;
}

HashFunction::HashValue H3Hash::calculate(HashFunction::HashValue&, Value<>&) {
    NES_THROW_RUNTIME_ERROR("Wrong function call! Please use calculateWithState() as H3 requires a seed vector");
}
H3Hash::H3Hash(uint64_t numberOfKeyBits) : entrySizeH3HashSeed(sizeof(uint64_t)), numberOfKeyBits(numberOfKeyBits) {}

}// namespace NES::Nautilus::Interface

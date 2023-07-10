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

#include <Nautilus/Interface/Fixed2DArray/Fixed2DArrayRef.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>

namespace NES::Nautilus::Interface {

template<typename IN, typename OUT>
OUT customBitCastProxy(IN f) {
    OUT ret;
    std::memcpy(&ret, &f, sizeof(IN));
    return ret;
}

HashFunction::HashValue H3Hash::init() { return 0_u64; }

HashFunction::HashValue H3Hash::calculateWithState(HashFunction::HashValue& hash, Value<>& value, Value<MemRef>& state) {

    // As the bitwise operations are not supported on floating points, we have to change the value to an unsigned int
    // This is okay, as we are only interested in the bits as-is and not the represented value
    if (value->isType<Double>()) {
        value = FunctionCall("customBitCastProxy",
                             customBitCastProxy<typename Double::RawType, typename UInt64::RawType>,
                             value.as<Double>());
    } else if (value->isType<Float>()) {
        value = FunctionCall("customBitCastProxy",
                             customBitCastProxy<typename Float::RawType, typename UInt32::RawType>,
                             value.as<Float>());
    }

    for (auto i = 0_u64; i < numberOfKeyBits; i = i + 1) {
        auto isBitSet = (value >> i) & 1;
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

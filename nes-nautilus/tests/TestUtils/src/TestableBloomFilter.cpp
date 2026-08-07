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

#include <TestableBloomFilter.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <memory>
#include <DataTypes/VarVal.hpp>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/Hash/HashFunction.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <nautilus/Engine.hpp>
#include <DataStructureTestUtils.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::TestUtils
{

namespace
{
uint64_t divRoundUp(const uint64_t dividend, const uint64_t divisor)
{
    return static_cast<uint64_t>(std::ceil(static_cast<double>(dividend) / static_cast<double>(divisor)));
}
}

/// NOLINTBEGIN(performance-unnecessary-value-param)
TestableBloomFilter::TestableBloomFilter(Nautilus::Interface::BloomFilterParams params, EngineMode mode)
    : params{params}
    , bits(divRoundUp(params.bitCount, 64), 0)
    , engine{std::make_unique<nautilus::engine::NautilusEngine>(makeEngine(mode))}
    , addFn{engine->registerFunction(std::function(
          [params](nautilus::val<uint64_t*> bitsPtr, nautilus::val<uint64_t> key)
          {
              const MurMur3HashFunction hashFunc;
              const HashFunction& hashFunction = hashFunc;
              auto hash = hashFunction.calculate(VarVal{key});
              const Nautilus::Interface::BloomFilterRef bloomFilter{bitsPtr, params};
              bloomFilter.add(hash);
          }))}
    , mightContainFn{engine->registerFunction(std::function(
          [params](nautilus::val<uint64_t*> bitsPtr, nautilus::val<uint64_t> key) -> nautilus::val<bool>
          {
              const MurMur3HashFunction hashFunc;
              const HashFunction& hashFunction = hashFunc;
              auto hash = hashFunction.calculate(VarVal{key});
              const Nautilus::Interface::BloomFilterRef bloomFilter{bitsPtr, params};
              return bloomFilter.mightContain(hash);
          }))}
{
}

/// NOLINTEND(performance-unnecessary-value-param)

void TestableBloomFilter::add(uint64_t key)
{
    addFn(bits.data(), key);
}

void TestableBloomFilter::add(uint64_t* externalBits, uint64_t key)
{
    addFn(externalBits, key);
}

bool TestableBloomFilter::mightContain(uint64_t key)
{
    return mightContainFn(bits.data(), key);
}

bool TestableBloomFilter::mightContain(uint64_t* externalBits, uint64_t key)
{
    return mightContainFn(externalBits, key);
}

void TestableBloomFilter::clear()
{
    std::ranges::fill(bits, uint64_t{0});
}

}

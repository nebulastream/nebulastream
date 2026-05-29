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

#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <nautilus/Engine.hpp>
#include <DataStructureTestUtils.hpp>

namespace NES::TestUtils
{

/// Wraps the Nautilus-compiled add/mightContain pair. Engine setup and function registration happen
/// once per instance so the hot loop is just function-call overhead.
class TestableBloomFilter
{
public:
    TestableBloomFilter(Nautilus::Interface::BloomFilterParams params, EngineMode mode);

    TestableBloomFilter(const TestableBloomFilter&) = delete;
    TestableBloomFilter& operator=(const TestableBloomFilter&) = delete;
    TestableBloomFilter(TestableBloomFilter&&) = default;
    TestableBloomFilter& operator=(TestableBloomFilter&&) = default;
    ~TestableBloomFilter() = default;

    void add(uint64_t key);

    void add(uint64_t* externalBits, uint64_t key);

    bool mightContain(uint64_t key);

    bool mightContain(uint64_t* externalBits, uint64_t key);

    void clear();

private:
    Nautilus::Interface::BloomFilterParams params;
    std::vector<uint64_t> bits;
    std::unique_ptr<nautilus::engine::NautilusEngine> engine;
    nautilus::engine::CompiledFunction<void(uint64_t*, uint64_t)> addFn;
    nautilus::engine::CompiledFunction<bool(uint64_t*, uint64_t)> mightContainFn;
};

}

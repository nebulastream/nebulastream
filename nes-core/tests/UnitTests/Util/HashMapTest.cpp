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

#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <Util/Experimental/HashMap.hpp>
#include <Util/Experimental/Hash.hpp>
#include <cstring>
#include <gtest/gtest.h>
#include <unistd.h>
#ifdef _POSIX_THREADS
#define HAS_POSIX_THREAD
#include <pthread.h>
#else
#error "Unsupported architecture"
#endif

namespace NES {
class HashMapTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("HashMapTest.log", NES::LOG_DEBUG);

        NES_INFO("HashMapTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("HashMapTest test class TearDownTestCase."); }
};


TEST_F(HashMapTest, putEntry) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto map = Experimental::Hashmap(bm, 8, 8, 100);
    auto e1 = Experimental::Hashmap::Entry(nullptr, 10);
    map.entries[0] = &e1;
    auto e2 = map.entries[0];
    ASSERT_EQ(&e1, e2);
}

TEST_F(HashMapTest, insertKey) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto map = Experimental::Hashmap(bm, 8, 8, 100);
    auto entry1 = map.findOrCreate<uint64_t, false>(10, 10);
    auto entry2 = map.findOrCreate<uint64_t, false>(10, 10);
    ASSERT_EQ(entry1, entry2);
}

TEST_F(HashMapTest, insertSmallNumberOfUniqueKey) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto map = Experimental::Hashmap(bm, 8, 8, 100);
    auto hasher = Experimental::CRC32Hash();
    for (int i = 0; i < 100; i++) {
        auto hash = hasher(i, Experimental::Hash<Experimental::CRC32Hash>::SEED);
        auto entry1 = map.findOrCreate<uint64_t, false>(i, hash);
        auto value = (uint64_t*) map.getValuePtr(entry1);
        *value = 42ULL;
    }

    for (int i = 0; i < 100; i++) {
        auto hash = hasher(i, Experimental::Hash<Experimental::CRC32Hash>::SEED);
        auto entry1 = map.findOneEntry<uint64_t, false>(i, hash);
        auto value = (uint64_t*) map.getValuePtr(entry1);
        ASSERT_EQ(*value, 42ULL);
    }
}

TEST_F(HashMapTest, updateSmallNumberOKeys) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto map = Experimental::Hashmap(bm, 8, 8, 100);
    auto hasher = Experimental::CRC32Hash();
    for (int i = 0; i < 10; i++) {
        auto hash = hasher(i, Experimental::Hash<Experimental::CRC32Hash>::SEED);
        auto entry1 = map.findOrCreate<uint64_t, false>(i, hash);
        auto value = (uint64_t*) map.getValuePtr(entry1);
        *value = 1ULL;
    }

    for (int i = 0; i < 1000; i++) {
        auto key = i % 10;
        auto hash = hasher(key, Experimental::Hash<Experimental::CRC32Hash>::SEED);
        auto entry1 = map.findOrCreate<uint64_t, false>(key, hash);
        auto value = (uint64_t*) map.getValuePtr(entry1);
        *value = *value + 1;
    }

    for (int i = 0; i < 10; i++) {
        auto hash = hasher(i, Experimental::Hash<Experimental::CRC32Hash>::SEED);
        auto entry1 = map.findOneEntry<uint64_t, false>(i, hash);
        auto value = (uint64_t*) map.getValuePtr(entry1);
        ASSERT_EQ(*value, 101ULL);
    }
}

TEST_F(HashMapTest, insertLargeNumberOfKeys) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto map = Experimental::Hashmap(bm, 8, 8, 100);
    auto hasher = Experimental::CRC32Hash();
    for (int i = 0; i < 100000; i++) {
        auto hash = hasher(i, Experimental::Hash<Experimental::CRC32Hash>::SEED);
        auto entry1 = map.findOrCreate<uint64_t, false>(i, hash);
        auto value = (uint64_t*) map.getValuePtr(entry1);
        *value = 42ULL;
    }

    for (int i = 0; i < 100000; i++) {
        auto hash = hasher(i, Experimental::Hash<Experimental::CRC32Hash>::SEED);
        auto entry1 = map.findOneEntry<uint64_t, false>(i, hash);
        auto value = (uint64_t*) map.getValuePtr(entry1);
        ASSERT_EQ(*value, 42ULL);
    }
}

TEST_F(HashMapTest, insertMultipleValues) {
    struct MapValues {
        uint32_t v1;
        uint64_t v2;
    };
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto map = Experimental::Hashmap(bm, 8, sizeof(MapValues), 100);
    auto hasher = Experimental::CRC32Hash();
    for (int i = 0; i < 100000; i++) {
        auto hash = hasher(i, Experimental::Hash<Experimental::CRC32Hash>::SEED);
        auto entry1 = map.findOrCreate<uint64_t, false>(i, hash);
        auto value = (MapValues*) map.getValuePtr(entry1);
        value->v1 = i * 2;
        value->v2 = i * 42;
    }

    for (int i = 0; i < 100000; i++) {
        auto hash = hasher(i, Experimental::Hash<Experimental::CRC32Hash>::SEED);
        auto entry1 = map.findOneEntry<uint64_t, false>(i, hash);
        auto value = (MapValues*) map.getValuePtr(entry1);
        ASSERT_EQ(value->v1, i * 2ULL);
        ASSERT_EQ(value->v2, i * 42ULL);
    }
}
}// namespace NES
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

#include <BaseIntegrationTest.hpp>
#include <Nautilus/Interface/FixedPage/FixedPage.hpp>
#include <Nautilus/Interface/FixedPage/FixedPageRef.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Common.hpp>

namespace NES::Nautilus::Interface {
struct __attribute__((packed)) CustomClass {
    uint64_t id;
    int32_t val1;
    double val2;
};

class FixedPageTest : public Testing::BaseUnitTest {
  public:
    std::unique_ptr<std::pmr::memory_resource> allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FixedPageTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup FixedPageTest test class.");
    }

    void SetUp() override { Testing::BaseUnitTest::SetUp(); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down FixedPageTest test class."); }

    /**
     * @brief Stores all items on the FixedPage and verifies the number of items on the FixedPage
     * @tparam Item
     * @param fixedPage
     * @param allItems
     * @param entrySize
     * @param maxItemsOnPage
     */
    template<typename Item>
    void runStoreTest(FixedPage& fixedPage,
                      const std::vector<Item>& allItems,
                      const uint64_t sizeOfRecord,
                      const uint64_t maxItemsOnPage) {
        ASSERT_EQ(sizeOfRecord, sizeof(Item));
        auto fixedPageRef = FixedPageRef(Value<MemRef>((int8_t*) &fixedPage));

        for (auto& it : allItems) {
            auto ptrMemRef = fixedPageRef.allocateEntry(0_u64);
            auto* ptr = ptrMemRef.getValue().value;

            if (ptr == nullptr) {
                ASSERT_EQ(fixedPage.size(), maxItemsOnPage);
                return;
            }

            std::memcpy(ptr, &it, sizeOfRecord);
        }

        ASSERT_EQ(fixedPage.size(), allItems.size());
    }

    /**
     * @brief Iterates through all entries on the FixedPage and verifies their content
     * @tparam Item
     * @param fixedPage
     * @param allItems
     * @param entrySize
     * @param expectedItemsOnPage
     */
    template<typename Item>
    void runRetrieveTest(FixedPage& fixedPage,
                         const std::vector<Item>& allItems,
                         const uint64_t sizeOfRecord,
                         const uint64_t expectedItemsOnPage) {
        auto fixedPageRef = FixedPageRef(Value<MemRef>((int8_t*) &fixedPage));
        auto pos = 0_u64;

        for (auto it : fixedPageRef) {
            auto* ptr = it.getValue().value;
            ASSERT_TRUE(std::memcmp(ptr, &allItems[pos++], sizeOfRecord) == 0);
        }

        ASSERT_EQ(pos, expectedItemsOnPage);
    }

    /**
     * @brief Creates FixedPage and runs tests
     * @tparam Item
     * @param pageSize
     * @param sizeOfRecord
     * @param allItems
     * @param maxItemsOnPage
     */
    template<typename Item>
    void createFixedPageAndRunTests(const uint64_t pageSize,
                                    const uint64_t sizeOfRecord,
                                    const std::vector<Item>& allItems,
                                    const uint64_t maxItemsOnPage) {
        auto dataPtr = reinterpret_cast<uint8_t*>(allocator->allocate(pageSize));
        FixedPage fixedPage(dataPtr, sizeOfRecord, pageSize);

        runStoreTest<Item>(fixedPage, allItems, sizeOfRecord, maxItemsOnPage);
        runRetrieveTest<Item>(fixedPage, allItems, sizeOfRecord, maxItemsOnPage);
    }
};

TEST_F(FixedPageTest, storeAndRetrieveValues) {
    const auto sizeOfRecord = sizeof(uint64_t);
    const auto pageSize = FixedPage::PAGE_SIZE;
    const auto numItems = 512_u64;

    std::vector<uint64_t> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        return n++;
    });

    createFixedPageAndRunTests(pageSize, sizeOfRecord, allItems, numItems);
}

TEST_F(FixedPageTest, storeAndRetrieveTooManyValuesForOnePage) {
    const auto sizeOfRecord = sizeof(uint64_t);
    const auto pageSize = FixedPage::PAGE_SIZE;
    const auto numItems = 1024_u64;
    const auto numItemsOnPage = pageSize / sizeOfRecord;

    std::vector<uint64_t> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        return n++;
    });

    createFixedPageAndRunTests(pageSize, sizeOfRecord, allItems, numItemsOnPage);
}

TEST_F(FixedPageTest, storeAndRetrieveValuesNonDefaultPageSize) {
    const auto sizeOfRecord = sizeof(uint64_t);
    const auto pageSize = sizeOfRecord;
    const auto numItems = 1_u64;

    std::vector<uint64_t> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        return n++;
    });

    createFixedPageAndRunTests(pageSize, sizeOfRecord, allItems, numItems);
}

TEST_F(FixedPageTest, storeAndRetrieveTooManyValuesForOnePageNonDefaultPageSize) {
    const auto sizeOfRecord = sizeof(uint64_t);
    const auto pageSize = sizeOfRecord;
    const auto numItems = 2_u64;
    const auto numItemsOnPage = pageSize / sizeOfRecord;

    std::vector<uint64_t> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        return n++;
    });

    createFixedPageAndRunTests(pageSize, sizeOfRecord, allItems, numItemsOnPage);
}

TEST_F(FixedPageTest, storeAndRetrieveValuesNonDefaultDataType) {
    const auto sizeOfRecord = sizeof(CustomClass);
    const auto pageSize = FixedPage::PAGE_SIZE;
    const auto numItems = 128_u64;

    std::vector<CustomClass> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        n++;
        return CustomClass(n, n, n / 7);
    });

    createFixedPageAndRunTests(pageSize, sizeOfRecord, allItems, numItems);
}

TEST_F(FixedPageTest, storeAndRetrieveTooManyValuesForOnePageNonDefaultDataType) {
    const auto sizeOfRecord = sizeof(CustomClass);
    const auto pageSize = FixedPage::PAGE_SIZE;
    const auto numItems = 256_u64;
    const auto numItemsOnPage = pageSize / sizeOfRecord;

    std::vector<CustomClass> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        n++;
        return CustomClass(n, n, n / 7);
    });

    createFixedPageAndRunTests(pageSize, sizeOfRecord, allItems, numItemsOnPage);
}

TEST_F(FixedPageTest, storeAndRetrieveValuesNonDefaultPageSizeAndDataType) {
    const auto sizeOfRecord = sizeof(CustomClass);
    const auto pageSize = 10 * sizeOfRecord;
    const auto numItems = 8_u64;

    std::vector<CustomClass> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        n++;
        return CustomClass(n, n, n / 7);
    });

    createFixedPageAndRunTests(pageSize, sizeOfRecord, allItems, numItems);
}

TEST_F(FixedPageTest, storeAndRetrieveTooManyValuesForOnePageNonDefaultPageSizeAndDataType) {
    const auto sizeOfRecord = sizeof(CustomClass);
    const auto pageSize = 10 * sizeOfRecord;
    const auto numItems = 16_u64;
    const auto numItemsOnPage = pageSize / sizeOfRecord;

    std::vector<CustomClass> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        n++;
        return CustomClass(n, n, n / 7);
    });

    createFixedPageAndRunTests(pageSize, sizeOfRecord, allItems, numItemsOnPage);
}

TEST_F(FixedPageTest, bloomFilterCheckTest) {
    const auto sizeOfRecord = sizeof(uint64_t);
    const auto pageSize = FixedPage::PAGE_SIZE;
    const auto numItems = pageSize / sizeOfRecord;
    auto dataPtr = reinterpret_cast<uint8_t*>(allocator->allocate(pageSize));
    FixedPage fixedPage(dataPtr, sizeOfRecord, pageSize);

    for (uint64_t i = 0; i < numItems; ++i) {
        uint64_t hash = Util::murmurHash(i);
        if (fixedPage.append(hash) == nullptr) {
            NES_ERROR("Could not insert tuple and thus the hash {} for {} in the FixedPage and consequently, the BloomFilter!", hash, i);
            ASSERT_TRUE(false);
        }
    }

    for (uint64_t i = 0; i < numItems; ++i) {
        uint64_t hash = Util::murmurHash(i);
        if (!fixedPage.bloomFilterCheck(hash)) {
            NES_ERROR("Could not find hash {} for {} in bloom filter!", hash, i);
            ASSERT_TRUE(false);
        }
    }

}

}// namespace NES::Nautilus::Interface

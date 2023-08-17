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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus::Interface {

class PagedVectorTest : public Testing::TestWithErrorHandling {
  public:
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    std::unique_ptr<std::pmr::memory_resource> allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PagedVectorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PagedVectorTest test class.");
    }
    void SetUp() override { Testing::TestWithErrorHandling::SetUp(); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down PagedVectorTest test class."); }

    template<typename Item>
    void runStoreTest(PagedVector& pagedVector,
                      const uint64_t entrySize,
                      const uint64_t pageSize,
                      const std::vector<Item>& allItems,
                      uint64_t expectedNumberOfEntries) {
        const uint64_t capacityPerPage = pageSize / entrySize;
        const uint64_t numberOfPages = std::ceil((double) expectedNumberOfEntries / capacityPerPage);
        auto pagedVectorRef = PagedVectorRef(Value<MemRef>((int8_t*) &pagedVector), entrySize);

        for (auto& item : allItems) {
            auto entryMemRef = pagedVectorRef.allocateEntry();
            auto* entryPtr = entryMemRef.getValue().value;
            std::memcpy(entryPtr, &item, entrySize);
        }

        ASSERT_EQ(pagedVector.getNumberOfEntries(), expectedNumberOfEntries);
        ASSERT_EQ(pagedVector.getNumberOfPages(), numberOfPages);

        // As we do lazy allocation, we do not create a new page if the last tuple fit on the page
        bool lastTupleFitsOntoLastPage = (expectedNumberOfEntries % capacityPerPage) == 0;
        const uint64_t numTuplesLastPage =
            lastTupleFitsOntoLastPage ? capacityPerPage : (expectedNumberOfEntries % capacityPerPage);
        ASSERT_EQ(pagedVector.getNumberOfEntriesOnCurrentPage(), numTuplesLastPage);
    }

    template<typename Item>
    void runRetrieveTest(const PagedVector& pagedVector, const uint64_t entrySize, const std::vector<Item>& allItems) {
        auto pagedVectorRef = PagedVectorRef(Value<MemRef>((int8_t*) &pagedVector), entrySize);
        ASSERT_EQ(pagedVector.getNumberOfEntries(), allItems.size());

        auto itemPos = 0_u64;
        for (auto it : pagedVectorRef) {
            auto* ptr = it.getValue().value;
            ASSERT_TRUE(std::memcmp(ptr, &allItems[itemPos++], entrySize) == 0);
        }

        ASSERT_EQ(itemPos, allItems.size());
    }

    template<typename Item>
    void insertAndAppendAllPages(const uint64_t entrySize,
                                 const uint64_t pageSize,
                                 const std::vector<std::vector<Item>>& allItemsAndVectors,
                                 const std::vector<Item>& expectedItemsAfterAppendAll) {

        // Inserting data into each PagedVector and checking for correctness
        std::vector<std::unique_ptr<PagedVector>> allPagedVectors;
        for (auto& allItems : allItemsAndVectors) {
            allPagedVectors.emplace_back(
                std::make_unique<PagedVector>(std::make_unique<Runtime::NesDefaultMemoryAllocator>(), entrySize, pageSize));
            runStoreTest<Item>(*allPagedVectors.back(), entrySize, pageSize, allItems, allItems.size());
            runRetrieveTest<Item>(*allPagedVectors.back(), entrySize, allItems);
        }

        // Now appending and deleting all PagedVectors except the first one
        auto& firstPagedVec = allPagedVectors[0];
        if (allItemsAndVectors.size() > 1) {
            for (uint64_t i = 1; i < allPagedVectors.size(); ++i) {
                auto& otherPagedVec = allPagedVectors[i];
                firstPagedVec->appendAllPages(*otherPagedVec);
                EXPECT_EQ(otherPagedVec->getNumberOfPages(), 0);
                EXPECT_EQ(otherPagedVec->getNumberOfEntries(), 0);
                EXPECT_EQ(otherPagedVec->getNumberOfEntriesOnCurrentPage(), 0);
            }

            allPagedVectors.erase(allPagedVectors.begin() + 1, allPagedVectors.end());
        }

        // After we have appended all paged and everything except the first one, we expect the size to be one
        EXPECT_EQ(allPagedVectors.size(), 1);
        runRetrieveTest<Item>(*firstPagedVec, entrySize, expectedItemsAfterAppendAll);
    }
};

TEST_F(PagedVectorTest, storeAndRetrieveValues) {
    const auto entrySize = sizeof(uint64_t);
    const auto pageSize = PagedVector::PAGE_SIZE;
    const auto numItems = 1234_u64;
    std::vector<uint64_t> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        return n++;
    });

    PagedVector pagedVector(std::move(allocator), entrySize, pageSize);
    runStoreTest<uint64_t>(pagedVector, entrySize, pageSize, allItems, allItems.size());
    runRetrieveTest<uint64_t>(pagedVector, entrySize, allItems);
}

TEST_F(PagedVectorTest, storeAndRetrieveValuesNonDefaultPageSize) {
    const auto entrySize = sizeof(uint64_t);
    const auto pageSize = (10 * entrySize) + 2;
    const auto numItems = 12340_u64;
    std::vector<uint64_t> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        return n++;
    });

    PagedVector pagedVector(std::move(allocator), entrySize, pageSize);
    runStoreTest<uint64_t>(pagedVector, entrySize, pageSize, allItems, allItems.size());
    runRetrieveTest<uint64_t>(pagedVector, entrySize, allItems);
}

TEST_F(PagedVectorTest, storeAndRetrieveValuesWithCustomItems) {
    struct __attribute__((packed)) CustomClass {
        uint64_t id;
        int32_t val1;
        double val2;
    };
    const auto entrySize = sizeof(uint64_t);
    const auto pageSize = PagedVector::PAGE_SIZE;
    const auto numItems = 12349_u64;
    std::vector<CustomClass> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        n++;
        return CustomClass(n, n, n / 7);
    });

    PagedVector pagedVector(std::move(allocator), entrySize, pageSize);
    runStoreTest<CustomClass>(pagedVector, entrySize, pageSize, allItems, allItems.size());
    runRetrieveTest<CustomClass>(pagedVector, entrySize, allItems);
}

TEST_F(PagedVectorTest, storeAndRetrieveValuesAfterMoveFromTo) {
    const auto entrySize = sizeof(uint64_t);
    const auto pageSize = PagedVector::PAGE_SIZE;
    const auto numItems = 1234_u64;
    std::vector<uint64_t> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable {
        return n++;
    });

    PagedVector pagedVector(std::move(allocator), entrySize, pageSize);
    runStoreTest<uint64_t>(pagedVector, entrySize, pageSize, allItems, allItems.size());

    pagedVector.moveFromTo(0, 30);
    pagedVector.moveFromTo(10, 40);
    pagedVector.moveFromTo(100, 130);
    allItems[30] = allItems[0];
    allItems[40] = allItems[10];
    allItems[130] = allItems[100];

    runRetrieveTest<uint64_t>(pagedVector, entrySize, allItems);
}

TEST_F(PagedVectorTest, appendAllPagesTwoVectors) {
    const auto entrySize = sizeof(uint64_t);
    const auto pageSize = entrySize * 5;
    const auto numItems = 1230_u64;
    std::vector<uint64_t> allItemsVec1, allItemsVec2, allItemsAfterAppend;
    std::generate_n(std::back_inserter(allItemsVec1), numItems, [n = 0]() mutable {
        return n++;
    });
    std::generate_n(std::back_inserter(allItemsVec2), numItems, [n = allItemsVec1.size()]() mutable {
        return n++;
    });
    allItemsAfterAppend.insert(allItemsAfterAppend.end(), allItemsVec1.begin(), allItemsVec1.end());
    allItemsAfterAppend.insert(allItemsAfterAppend.end(), allItemsVec2.begin(), allItemsVec2.end());

    insertAndAppendAllPages<uint64_t>(entrySize, pageSize, {allItemsVec1, allItemsVec2}, allItemsAfterAppend);
}

TEST_F(PagedVectorTest, appendAllPagesMultipleVectors) {
    const auto entrySize = sizeof(uint64_t);
    const uint64_t pageSize = (10 * entrySize) + 2;
    const auto numItems = 12304_u64;
    std::vector<uint64_t> allItemsVec1, allItemsVec2, allItemsVec3, allItemsVec4, allItemsAfterAppend;
    std::generate_n(std::back_inserter(allItemsVec1), numItems, [n = 0]() mutable {
        return n++;
    });
    std::generate_n(std::back_inserter(allItemsVec2), numItems, [n = allItemsVec1.size()]() mutable {
        return n++;
    });
    std::generate_n(std::back_inserter(allItemsVec3), numItems, [n = allItemsVec2.size()]() mutable {
        return n++;
    });
    std::generate_n(std::back_inserter(allItemsVec4), numItems, [n = allItemsVec3.size()]() mutable {
        return n++;
    });

    allItemsAfterAppend.insert(allItemsAfterAppend.end(), allItemsVec1.begin(), allItemsVec1.end());
    allItemsAfterAppend.insert(allItemsAfterAppend.end(), allItemsVec2.begin(), allItemsVec2.end());
    allItemsAfterAppend.insert(allItemsAfterAppend.end(), allItemsVec3.begin(), allItemsVec3.end());
    allItemsAfterAppend.insert(allItemsAfterAppend.end(), allItemsVec4.begin(), allItemsVec4.end());

    insertAndAppendAllPages<uint64_t>(entrySize,
                                      pageSize,
                                      {allItemsVec1, allItemsVec2, allItemsVec3, allItemsVec4},
                                      allItemsAfterAppend);
}
}// namespace NES::Nautilus::Interface
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
#include <gtest/gtest.h>
#include <algorithm>
#include <memory>
namespace NES::Nautilus::Interface {

class PagedVectorTest : public Testing::NESBaseTest {
  public:
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    std::unique_ptr<std::pmr::memory_resource> allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();


    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PagedVectorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PagedVectorTest test class.");
    }
    void SetUp() override { Testing::NESBaseTest::SetUp(); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down PagedVectorTest test class."); }


    template<typename Item>
    void runStoreTest(PagedVector& pagedVector, const uint64_t entrySize, const uint64_t pageSize,
                      const std::vector<Item>& allItems, uint64_t expectedNumberOfEntries) {
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
        const uint64_t numTuplesLastPage = lastTupleFitsOntoLastPage ? capacityPerPage : (expectedNumberOfEntries % capacityPerPage);
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
};

TEST_F(PagedVectorTest, storeAndRetrieveValues) {
    const auto entrySize = sizeof(uint64_t);
    const auto pageSize = PagedVector::PAGE_SIZE;
    const auto numItems = 1234_u64;
    std::vector<uint64_t> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable { return n++; });


    PagedVector pagedVector(std::move(allocator), entrySize, pageSize);
    runStoreTest<uint64_t>(pagedVector, entrySize, pageSize, allItems, allItems.size());
    runRetrieveTest<uint64_t>(pagedVector, entrySize, allItems);
}

TEST_F(PagedVectorTest, storeAndRetrieveValuesNonDefaultPageSize) {
    const auto entrySize = sizeof(uint64_t);
    const auto pageSize = (10 * entrySize) + 2;
    const auto numItems = 12340_u64;
    std::vector<uint64_t> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable { return n++; });


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
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable { n++; return CustomClass(n, n, n / 7); });


    PagedVector pagedVector(std::move(allocator), entrySize, pageSize);
    runStoreTest<CustomClass>(pagedVector, entrySize, pageSize, allItems, allItems.size());
    runRetrieveTest<CustomClass>(pagedVector, entrySize, allItems);
}

TEST_F(PagedVectorTest, storeAndRetrieveValuesAfterMoveFromTo) {
    const auto entrySize = sizeof(uint64_t);
    const auto pageSize = PagedVector::PAGE_SIZE;
    const auto numItems = 1234_u64;
    std::vector<uint64_t> allItems;
    std::generate_n(std::back_inserter(allItems), numItems, [n = 0]() mutable { return n++; });


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
    std::vector<uint64_t> allItemsVec1;
    std::generate_n(std::back_inserter(allItemsVec1), numItems, [n = 0]() mutable { return n++; });

    // Inserting data into the first PagedVector
    PagedVector pagedVector1(std::move(allocator), entrySize, pageSize);
    runStoreTest<uint64_t>(pagedVector1, entrySize, pageSize, allItemsVec1, allItemsVec1.size());
    runRetrieveTest<uint64_t>(pagedVector1, entrySize, allItemsVec1);

    // Inserting the data into the second PagedVector
    auto allocator2 = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    std::vector<uint64_t> allItemsVec2;
    std::generate_n(std::back_inserter(allItemsVec2), numItems, [n = 1000 * 1000]() mutable { return n++; });
    PagedVector pagedVector2(std::move(allocator2), entrySize, pageSize);
    runStoreTest<uint64_t>(pagedVector2, entrySize, pageSize, allItemsVec2, allItemsVec2.size());
    runRetrieveTest<uint64_t>(pagedVector2, entrySize, allItemsVec2);

    // Duplicating allItems, as we have inserted it into pagedVector1 and pagedVector2
    std::vector<uint64_t> duplicatedItems;
    duplicatedItems.insert(duplicatedItems.end(), allItemsVec1.begin(), allItemsVec1.end());
    duplicatedItems.insert(duplicatedItems.end(), allItemsVec2.begin(), allItemsVec2.end());

    // Appending all pages and checking for correctness
    pagedVector1.appendAllPages(pagedVector2);
    ASSERT_EQ(pagedVector2.getNumberOfPages(), 0);
    runRetrieveTest<uint64_t>(pagedVector1, entrySize, duplicatedItems);

    // Now we check, if we can insert again and can retrieve all (already + new) items
    std::vector<uint64_t> allItemsAfterAppendingPages;
    std::generate_n(std::back_inserter(allItemsAfterAppendingPages), numItems, [n = 0]() mutable { return n++; });
    runStoreTest<uint64_t>(pagedVector1, entrySize, pageSize, allItemsAfterAppendingPages, duplicatedItems.size() + allItemsAfterAppendingPages.size());
    duplicatedItems.insert(duplicatedItems.end(), allItemsAfterAppendingPages.begin(), allItemsAfterAppendingPages.end());
    runRetrieveTest<uint64_t>(pagedVector1, entrySize, duplicatedItems);
}

TEST_F(PagedVectorTest, appendAllPagesMultipleVectors) {
    const uint64_t numberOfPagedVectors = 4;
    const uint64_t numItemsPerPagedVector = 1232;
    const uint64_t entrySize = 32;
    const uint64_t pageSize = (10 * entrySize) + 2;
    const uint64_t capacityPerPage = pageSize / entrySize;
    const uint64_t numberOfPages = std::ceil((double) numItemsPerPagedVector / capacityPerPage);
    std::vector<std::unique_ptr<PagedVector>> allPagedVectors;

    for (auto cntPagedVector = 0_u64; cntPagedVector < numberOfPagedVectors; ++cntPagedVector) {
        auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
        auto pagedVector = std::make_unique<PagedVector>(std::move(allocator), entrySize, pageSize);
        auto pagedVectorRef = PagedVectorRef(Value<MemRef>((int8_t*) pagedVector.get()), (uint64_t) entrySize);

        for (auto i = 0UL; i < numItemsPerPagedVector; i++) {
            Value<UInt64> val((uint64_t) i);
            auto ref = pagedVectorRef.allocateEntry();
            ref.store(val);
        }

        ASSERT_EQ(pagedVector->getNumberOfEntries(), numItemsPerPagedVector);
        ASSERT_EQ(pagedVector->getPageSize(), pageSize);
        ASSERT_EQ(pagedVector->getNumberOfPages(), numberOfPages);

        allPagedVectors.emplace_back(std::move(pagedVector));
    }
    ASSERT_EQ(allPagedVectors.size(), numberOfPagedVectors);

    // Now we appendAllPages to the first pagedVector
    for (uint64_t i = 1; i < allPagedVectors.size(); ++i) {
        allPagedVectors[0]->appendAllPages(*allPagedVectors[i]);
    }
    allPagedVectors.erase(allPagedVectors.begin() + 1, allPagedVectors.end());

    // Checking for correctness
    ASSERT_EQ(allPagedVectors.size(), 1);
    ASSERT_EQ(allPagedVectors[0]->getNumberOfEntries(), numItemsPerPagedVector * numberOfPagedVectors);
    ASSERT_EQ(allPagedVectors[0]->getPageSize(), pageSize);
    ASSERT_EQ(allPagedVectors[0]->getNumberOfPages(), numberOfPages * numberOfPagedVectors);

    uint64_t i = 0;
    auto pagedVectorRef = PagedVectorRef(Value<MemRef>((int8_t*) allPagedVectors[0].get()), (uint64_t) entrySize);
    for (auto it : pagedVectorRef) {
        Value<UInt64> expectedVal(i);
        auto resultVal = it.load<UInt64>();
        ASSERT_EQ(resultVal.getValue().getValue(), expectedVal.getValue().getValue());
        i = (i + 1) % numItemsPerPagedVector;
    }
}
}// namespace NES::Nautilus::Interface
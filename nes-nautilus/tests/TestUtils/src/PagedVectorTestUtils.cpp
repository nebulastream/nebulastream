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

#include <cmath>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <vector>
#include <API/Schema.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>
#include <Engine.hpp>
#include <NautilusTestUtils.hpp>
#include <PagedVectorTestUtils.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus::TestUtils
{


void runStoreTest(
    Interface::PagedVector& pagedVector,
    const Schema& testSchema,
    const uint64_t pageSize,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const std::vector<Memory::TupleBuffer>& allRecords,
    const nautilus::engine::NautilusEngine& nautilusEngine,
    Memory::AbstractBufferProvider& bufferManager)
{
    /// Creating the memory provider for the paged vector
    const auto memoryProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);

    /// Compiling the function that inserts the records into the PagedVector, if it is not already compiled.
    const auto memoryProviderInputBuffer
        = Interface::MemoryProvider::TupleBufferMemoryProvider::create(allRecords[0].getBufferSize(), testSchema);
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    auto insertIntoPagedVector = nautilusEngine.registerFunction(std::function(
        [=](nautilus::val<Memory::TupleBuffer*> inputBufferRef,
            nautilus::val<Memory::AbstractBufferProvider*> bufferProviderVal,
            nautilus::val<Interface::PagedVector*> pagedVectorVal)
        {
            const RecordBuffer recordBuffer(inputBufferRef);
            const Interface::PagedVectorRef pagedVectorRef(pagedVectorVal, memoryProvider, bufferProviderVal);
            for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
            {
                const auto record = memoryProviderInputBuffer->readRecord(projections, recordBuffer, i);
                pagedVectorRef.writeRecord(record, bufferProviderVal);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)

    /// Inserting each tuple by iterating over all tuple buffers
    for (auto buf : allRecords)
    {
        insertIntoPagedVector(std::addressof(buf), std::addressof(bufferManager), std::addressof(pagedVector));
    }


    /// Calculating the expected number of entries and pages
    const uint64_t expectedNumberOfEntries = std::accumulate(
        allRecords.begin(), allRecords.end(), 0UL, [](const auto& sum, const auto& buffer) { return sum + buffer.getNumberOfTuples(); });
    const uint64_t capacityPerPage = memoryProvider->getMemoryLayout()->getCapacity();
    const uint64_t numberOfPages = std::ceil(static_cast<double>(expectedNumberOfEntries) / capacityPerPage);
    ASSERT_EQ(pagedVector.getTotalNumberOfEntries(), expectedNumberOfEntries);
    ASSERT_EQ(pagedVector.getNumberOfPages(), numberOfPages);

    /// As we do lazy allocation, we do not create a new page if the last tuple fit on the page
    const bool lastTupleFitsOntoLastPage = (expectedNumberOfEntries % capacityPerPage) == 0;
    const uint64_t numTuplesLastPage = lastTupleFitsOntoLastPage ? capacityPerPage : (expectedNumberOfEntries % capacityPerPage);
    ASSERT_EQ(pagedVector.getLastPage().getNumberOfTuples(), numTuplesLastPage);
}

void runRetrieveTest(
    Interface::PagedVector& pagedVector,
    const Schema& testSchema,
    const uint64_t pageSize,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const std::vector<Memory::TupleBuffer>& allRecords,
    const nautilus::engine::NautilusEngine& nautilusEngine,
    Memory::AbstractBufferProvider& bufferManager)
{
    /// Creating a buffer to store the retrieved records and then calling the compiled method to retrieve the records
    const uint64_t numberOfExpectedTuples = std::accumulate(
        allRecords.begin(), allRecords.end(), 0UL, [](const auto& sum, const auto& buffer) { return sum + buffer.getNumberOfTuples(); });
    ASSERT_EQ(pagedVector.getTotalNumberOfEntries(), numberOfExpectedTuples);
    auto outputBufferVal = bufferManager.getUnpooledBuffer(numberOfExpectedTuples * testSchema.getSchemaSizeInBytes());
    ASSERT_TRUE(outputBufferVal.has_value());
    auto outputBuffer = outputBufferVal.value();

    /// Creating the memory provider for the input buffers
    const auto memoryProviderActualBuffer
        = Interface::MemoryProvider::TupleBufferMemoryProvider::create(outputBuffer.getBufferSize(), testSchema);

    /// Compiling the function that reads the records from the PagedVector, if it is not already compiled
    const auto memoryProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    auto readFromPagedVectorIntoTupleBuffer = nautilusEngine.registerFunction(std::function(
        [=](nautilus::val<Memory::TupleBuffer*> outputBufferRef,
            nautilus::val<Memory::AbstractBufferProvider*> bufferProviderVal,
            nautilus::val<Interface::PagedVector*> pagedVectorVal)
        {
            RecordBuffer recordBuffer(outputBufferRef);
            const Interface::PagedVectorRef pagedVectorRef(pagedVectorVal, memoryProvider, bufferProviderVal);
            nautilus::val<uint64_t> numberOfTuples = 0;
            for (auto it = pagedVectorRef.begin(projections); it != pagedVectorRef.end(projections); ++it)
            {
                auto record = *it;
                memoryProviderActualBuffer->writeRecord(numberOfTuples, recordBuffer, record, bufferProviderVal);
                numberOfTuples = numberOfTuples + 1;
                recordBuffer.setNumRecords(numberOfTuples);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)

    /// Retrieving the records from the PagedVector, by calling the compiled function
    readFromPagedVectorIntoTupleBuffer(std::addressof(outputBuffer), std::addressof(bufferManager), std::addressof(pagedVector));


    /// Checking for correctness of the retrieved records
    const RecordBuffer recordBufferActual(nautilus::val<Memory::TupleBuffer*>(std::addressof(outputBuffer)));
    nautilus::val<uint64_t> recordBufferIndexActual = 0;
    const auto memoryProviderInputBuffer
        = Interface::MemoryProvider::TupleBufferMemoryProvider::create(allRecords[0].getBufferSize(), testSchema);
    for (auto inputBuf : allRecords)
    {
        const RecordBuffer recordBufferInput(nautilus::val<Memory::TupleBuffer*>(std::addressof(inputBuf)));
        for (nautilus::val<uint64_t> i = 0; i < inputBuf.getNumberOfTuples(); ++i)
        {
            auto recordActual = memoryProviderActualBuffer->readRecord(projections, recordBufferActual, recordBufferIndexActual);
            auto recordExpected = memoryProviderInputBuffer->readRecord(projections, recordBufferInput, i);

            /// Printing an error message, if the values are not equal.
            auto compareResult = NautilusTestUtils::compareRecords(recordActual, recordExpected, projections);
            if (not compareResult.empty())
            {
                EXPECT_TRUE(false) << compareResult;
            }

            /// Incrementing the index of the actual record buffer
            recordBufferIndexActual = recordBufferIndexActual + 1;
        }
    }

    ASSERT_EQ(recordBufferIndexActual, numberOfExpectedTuples);
}

void insertAndAppendAllPagesTest(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const Schema& schema,
    const uint64_t entrySize,
    const uint64_t pageSize,
    const std::vector<std::vector<Memory::TupleBuffer>>& allRecordsAndVectors,
    const std::vector<Memory::TupleBuffer>& expectedRecordsAfterAppendAll,
    uint64_t differentPageSizes,
    const nautilus::engine::NautilusEngine& nautilusEngine,
    Memory::AbstractBufferProvider& bufferManager)
{
    /// Inserting data into each PagedVector and checking for correct values
    std::vector<std::unique_ptr<Interface::PagedVector>> allPagedVectors;
    auto varPageSize = pageSize;
    for (const auto& allRecords : allRecordsAndVectors)
    {
        if (differentPageSizes != 0)
        {
            differentPageSizes++;
        }
        varPageSize += differentPageSizes * entrySize;
        const auto memoryProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(varPageSize, schema);
        allPagedVectors.emplace_back(std::make_unique<Interface::PagedVector>());
        runStoreTest(*allPagedVectors.back(), schema, pageSize, projections, allRecords, nautilusEngine, bufferManager);
        runRetrieveTest(*allPagedVectors.back(), schema, pageSize, projections, allRecords, nautilusEngine, bufferManager);
    }

    /// Appending and deleting all PagedVectors except for the first one
    const auto& firstPagedVec = allPagedVectors[0];
    if (allRecordsAndVectors.size() > 1)
    {
        for (uint64_t i = 1; i < allPagedVectors.size(); ++i)
        {
            auto& otherPagedVec = allPagedVectors[i];
            firstPagedVec->appendAllPages(*otherPagedVec);
            EXPECT_EQ(otherPagedVec->getNumberOfPages(), 0);
        }

        allPagedVectors.erase(allPagedVectors.begin() + 1, allPagedVectors.end());
    }

    /// Checking for number of pagedVectors and correct values
    EXPECT_EQ(allPagedVectors.size(), 1);
    const auto memoryProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(pageSize, schema);
    runRetrieveTest(*firstPagedVec, schema, pageSize, projections, expectedRecordsAfterAppendAll, nautilusEngine, bufferManager);
}

}

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

#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Core.hpp>

namespace NES::Runtime::Execution
{

NLJSlice::NLJSlice(const SliceStart sliceStart, const SliceEnd sliceEnd, const uint64_t numberOfWorkerThreads) : Slice(sliceStart, sliceEnd)
{
    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        leftPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>());
        leftPagedVectorsKeys.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>());
    }

    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        rightPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>());
        rightPagedVectorsKeys.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>());
    }
}

uint64_t NLJSlice::getNumberOfTuplesLeft() const
{
    return std::accumulate(
        leftPagedVectors.begin(),
        leftPagedVectors.end(),
        0,
        [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
}

uint64_t NLJSlice::getNumberOfTuplesRight() const
{
    return std::accumulate(
        rightPagedVectors.begin(),
        rightPagedVectors.end(),
        0,
        [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
}

Nautilus::Interface::PagedVector* NLJSlice::getPagedVectorRefLeft(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % leftPagedVectors.size();
    return leftPagedVectors[pos].get();
}

Nautilus::Interface::PagedVector* NLJSlice::getPagedVectorRefRight(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % rightPagedVectors.size();
    return rightPagedVectors[pos].get();
}

void NLJSlice::combinePagedVectors()
{
    /// Due to the out-of-order nature of our execution engine, it might happen that we call this code here from multiple worker threads.
    /// For example, if different worker threads are emitting the same slice for different windows.
    /// To ensure correctness, we use a lock here
    const std::scoped_lock lock(combinePagedVectorsMutex);

    /// Append all PagedVectors on the left join side and erase all items except for the first one
    /// We do this to ensure that we have only one PagedVector for each side during the probing phase
    if (leftPagedVectors.size() > 1)
    {
        for (uint64_t i = 1; i < leftPagedVectors.size(); ++i)
        {
            leftPagedVectors[0]->appendAllPages(*leftPagedVectors[i]);
        }
        leftPagedVectors.erase(leftPagedVectors.begin() + 1, leftPagedVectors.end());
    }

    /// Append all PagedVectors on the right join side and remove all items except for the first one
    if (rightPagedVectors.size() > 1)
    {
        for (uint64_t i = 1; i < rightPagedVectors.size(); ++i)
        {
            rightPagedVectors[0]->appendAllPages(*rightPagedVectors[i]);
        }
        rightPagedVectors.erase(rightPagedVectors.begin() + 1, rightPagedVectors.end());
    }

    /// Append all PagedVectorsKeys on the left join side and erase all items except for the first one
    if (leftPagedVectorsKeys.size() > 1)
    {
        for (uint64_t i = 1; i < leftPagedVectorsKeys.size(); ++i)
        {
            leftPagedVectorsKeys[0]->appendAllPages(*leftPagedVectorsKeys[i]);
        }
        leftPagedVectorsKeys.erase(leftPagedVectorsKeys.begin() + 1, leftPagedVectorsKeys.end());
    }

    /// Append all PagedVectorsKeys on the right join side and remove all items except for the first one
    if (rightPagedVectorsKeys.size() > 1)
    {
        for (uint64_t i = 1; i < rightPagedVectorsKeys.size(); ++i)
        {
            rightPagedVectorsKeys[0]->appendAllPages(*rightPagedVectorsKeys[i]);
        }
        rightPagedVectorsKeys.erase(rightPagedVectorsKeys.begin() + 1, rightPagedVectorsKeys.end());
    }
}

void NLJSlice::writeToFile(
    FileWriter& fileWriter,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const WorkerThreadId threadId,
    FileLayout fileLayout)
{
    // TODO remove once fileLayout is chosen adaptively
    fileLayout = !memoryLayout->getKeyFieldNames().empty() ? fileLayout : NO_SEPARATION;

    PRECONDITION(!memoryLayout->getSchema()->containsVarSizedDataField(), "NLJSlice does not currently support variable sized data");
    PRECONDITION(
        !memoryLayout->getKeyFieldNames().empty() || fileLayout == NO_SEPARATION,
        "Cannot separate key field data and payload as there are no key fields");
    // TODO fix this method and remove preconditon
    PRECONDITION(
        memoryLayout->getSchema()->getLayoutType() != Schema::MemoryLayoutType::ROW_LAYOUT,
        "NLJSlice does not currently support any memory layout other than row layout");

    const auto [pagedVector, pagedVectorKeys] = getPagedVectors(joinBuildSide, threadId);
    switch (fileLayout)
    {
        /// Write all tuples consecutivley to file
        case NO_SEPARATION_KEEP_KEYS:
        case NO_SEPARATION: {
            for (const auto& page : pagedVector->getPages())
            {
                fileWriter.write(page.getBuffer(), page.getNumberOfTuples() * memoryLayout->getTupleSize());
            }
            break;
        }
        /// Write only payload to file and append key field data to designated pagedVectorKeys
        case SEPARATE_PAYLOAD: {
            writePayloadOnlyToFile(pagedVector, memoryLayout, bufferProvider, pagedVectorKeys, fileWriter);
            break;
        }
        /// Write designated pagedVectorKeys to key file first and then remaining payload and key field data to separate files
        case SEPARATE_KEYS: {
            writePayloadAndKeysToSeparateFiles(pagedVector, memoryLayout, pagedVectorKeys, fileWriter);
            break;
        }
    }
}

void NLJSlice::readFromFile(
    FileReader& fileReader,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const WorkerThreadId threadId,
    FileLayout fileLayout)
{
    // TODO remove once fileLayout is chosen adaptively
    fileLayout = !memoryLayout->getKeyFieldNames().empty() ? fileLayout : NO_SEPARATION;

    PRECONDITION(!memoryLayout->getSchema()->containsVarSizedDataField(), "NLJSlice does not currently support variable sized data");
    PRECONDITION(
        !memoryLayout->getKeyFieldNames().empty() || fileLayout == NO_SEPARATION,
        "Cannot separate key field data and payload as there are no key fields");
    // TODO fix this method and remove preconditon
    PRECONDITION(
        memoryLayout->getSchema()->getLayoutType() != Schema::MemoryLayoutType::ROW_LAYOUT,
        "NLJSlice does not currently support any memory layout other than row layout");

    const auto [pagedVector, pagedVectorKeys] = getPagedVectors(joinBuildSide, threadId);
    switch (fileLayout)
    {
        /// Read all tuples consecutivley from file
        case NO_SEPARATION_KEEP_KEYS:
        case NO_SEPARATION: {
            pagedVector->appendPageIfFull(bufferProvider, memoryLayout);
            auto lastPage = pagedVector->getLastPage();
            auto tuplesToRead = memoryLayout->getCapacity() - lastPage.getNumberOfTuples();
            const auto tupleSize = memoryLayout->getTupleSize();

            while (const auto bytesRead = fileReader.read(lastPage.getBuffer(), tuplesToRead * tupleSize))
            {
                lastPage.setNumberOfTuples(bytesRead / tupleSize);
                pagedVector->appendPageIfFull(bufferProvider, memoryLayout);
                lastPage = pagedVector->getLastPage();
                tuplesToRead = memoryLayout->getCapacity();
            }
            break;
        }
        /// Read payload and key field data from separate files first and then remaining designated pagedVectorKeys and payload from file
        case SEPARATE_PAYLOAD:
        case SEPARATE_KEYS: {
            readSeparatelyFromFiles(pagedVector, memoryLayout, bufferProvider, pagedVectorKeys, fileReader);
            break;
        }
    }
}

void NLJSlice::truncate(const QueryCompilation::JoinBuildSideType joinBuildSide, const WorkerThreadId threadId, const FileLayout fileLayout)
{
    /// Clear the pages without appending a new one afterwards as this will be done in PagedVectorRef
    // TODO append key field data to designated pagedVectorKeys to be able to write all tuples to file but keep keys in memory, alternatively create mew FileLayout and do this in writeToFile()->writePayloadOnlyToFile()
    const auto [pagedVector, pagedVectorKeys] = getPagedVectors(joinBuildSide, threadId);
    switch (fileLayout)
    {
        /// Append key field data to designated pagedVectorKeys
        case NO_SEPARATION_KEEP_KEYS: {
            // TODO
            break;
        }
        /// Do nothing as key field data has just been written to designated pagedVectorKeys
        case SEPARATE_PAYLOAD: {
            break;
        }
        /// Remove all key field data as a different FileLayout might have been used previously
        case NO_SEPARATION:
        case SEPARATE_KEYS: {
            pagedVectorKeys->getPages().clear();
            break;
        }
    }
    pagedVector->getPages().clear();
}

size_t NLJSlice::getStateSizeInBytesForThreadId(
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const WorkerThreadId threadId) const
{
    const auto [pagedVector, pagedVectorKeys] = getPagedVectors(joinBuildSide, threadId);
    const auto pageSize = memoryLayout->getBufferSize();
    const auto numPages = pagedVector->getNumberOfPages();
    const auto numPagesKeys = pagedVectorKeys->getNumberOfPages();

    return pageSize * numPages + pageSize * numPagesKeys;
}

std::tuple<Interface::PagedVector*, Interface::PagedVector*>
NLJSlice::getPagedVectors(const QueryCompilation::JoinBuildSideType joinBuildSide, const WorkerThreadId threadId) const
{
    switch (joinBuildSide)
    {
        case QueryCompilation::JoinBuildSideType::Left: {
            const auto leftPos = threadId % leftPagedVectors.size();
            const auto leftPosKeys = threadId % leftPagedVectorsKeys.size();
            return std::make_tuple(leftPagedVectors[leftPos].get(), leftPagedVectorsKeys[leftPosKeys].get());
        }
        case QueryCompilation::JoinBuildSideType::Right: {
            const auto rightPos = threadId % rightPagedVectors.size();
            const auto rightPosKeys = threadId % rightPagedVectorsKeys.size();
            return std::make_tuple(rightPagedVectors[rightPos].get(), rightPagedVectorsKeys[rightPosKeys].get());
        }
        default:
            throw UnknownJoinBuildSide();
    }
}

void NLJSlice::writePayloadOnlyToFile(
    Interface::PagedVector* pagedVector,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    Memory::AbstractBufferProvider* bufferProvider,
    Interface::PagedVector* pagedVectorKeys,
    FileWriter& fileWriter)
{
    const auto keyFieldsOnlySchema = memoryLayout->createKeyFieldsOnlySchema();
    const auto keyFieldsOnlyMemoryLayout = NES::Util::createMemoryLayout(keyFieldsOnlySchema, memoryLayout->getBufferSize());
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    for (const auto& page : pagedVector->getPages())
    {
        auto pagePtr = page.getBuffer();
        for (auto tupleIdx = 0UL; tupleIdx < page.getNumberOfTuples(); ++tupleIdx)
        {
            // TODO appendPageIfFull only when page is full not for each tuple
            pagedVectorKeys->appendPageIfFull(bufferProvider, keyFieldsOnlyMemoryLayout.get());
            auto lastKeyPage = pagedVectorKeys->getLastPage();
            const auto numTuplesLastKeyPage = lastKeyPage.getNumberOfTuples();
            auto lastKeyPagePtr = lastKeyPage.getBuffer() + numTuplesLastKeyPage * keyFieldsOnlyMemoryLayout->getTupleSize();

            for (const auto& [fieldType, fieldSize] : groupedFieldTypeSizes)
            {
                if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::KEY)
                {
                    std::memcpy(lastKeyPagePtr, pagePtr, fieldSize);
                    lastKeyPagePtr += fieldSize;
                }
                else if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::PAYLOAD)
                {
                    fileWriter.write(pagePtr, fieldSize);
                }
                pagePtr += fieldSize;
            }
            lastKeyPage.setNumberOfTuples(numTuplesLastKeyPage + 1);
        }
    }
}

void NLJSlice::writePayloadAndKeysToSeparateFiles(
    Interface::PagedVector* pagedVector,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    Interface::PagedVector* pagedVectorKeys,
    FileWriter& fileWriter)
{
    const auto keyFieldsOnlySchema = memoryLayout->createKeyFieldsOnlySchema();
    const auto keyFieldsOnlyMemoryLayout = NES::Util::createMemoryLayout(keyFieldsOnlySchema, memoryLayout->getBufferSize());
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    for (const auto& keyPage : pagedVectorKeys->getPages())
    {
        fileWriter.writeKey(keyPage.getBuffer(), keyPage.getNumberOfTuples() * keyFieldsOnlyMemoryLayout->getTupleSize());
    }

    for (const auto& page : pagedVector->getPages())
    {
        auto pagePtr = page.getBuffer();
        for (auto tupleIdx = 0UL; tupleIdx < page.getNumberOfTuples(); ++tupleIdx)
        {
            for (const auto& [fieldType, fieldSize] : groupedFieldTypeSizes)
            {
                if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::KEY)
                {
                    fileWriter.writeKey(pagePtr, fieldSize);
                }
                else if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::PAYLOAD)
                {
                    fileWriter.write(pagePtr, fieldSize);
                }
                pagePtr += fieldSize;
            }
        }
    }
}

void NLJSlice::readSeparatelyFromFiles(
    Interface::PagedVector* pagedVector,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    Memory::AbstractBufferProvider* bufferProvider,
    Interface::PagedVector* pagedVectorKeys,
    FileReader& fileReader)
{
    const auto keyFieldsOnlySchema = memoryLayout->createKeyFieldsOnlySchema();
    const auto keyFieldsOnlyMemoryLayout = NES::Util::createMemoryLayout(keyFieldsOnlySchema, memoryLayout->getBufferSize());
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    auto& keyPages = pagedVectorKeys->getPages();
    auto keyPagePtr = !keyPages.empty() ? keyPages.front().getBuffer() : nullptr;

    while (true)
    {
        // TODO appendPageIfFull only when page is full not for each tuple
        pagedVector->appendPageIfFull(bufferProvider, memoryLayout);
        auto lastPage = pagedVector->getLastPage();
        const auto numTuplesLastPage = lastPage.getNumberOfTuples();
        auto lastPagePtr = lastPage.getBuffer() + numTuplesLastPage * memoryLayout->getTupleSize();

        // TODO get new key page only when all tuples were read and do not check for each tuple
        if (keyPagePtr)
        {
            if (const auto& currKeyPage = keyPages.front();
                keyPagePtr >= currKeyPage.getBuffer() + currKeyPage.getNumberOfTuples() * keyFieldsOnlyMemoryLayout->getTupleSize())
            {
                keyPages.erase(keyPages.begin());
                keyPagePtr = !keyPages.empty() ? keyPages.front().getBuffer() : nullptr;
                // TODO if fileReader.peek() != EOF then return
            }
        }

        for (const auto& [fieldType, fieldSize] : groupedFieldTypeSizes)
        {
            if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::KEY)
            {
                if (!fileReader.readKey(lastPagePtr, fieldSize) && keyPagePtr != nullptr)
                {
                    std::memcpy(lastPagePtr, keyPagePtr, fieldSize);
                    keyPagePtr += fieldSize;
                }
            }
            else if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::PAYLOAD)
            {
                if (!fileReader.read(lastPagePtr, fieldSize))
                {
                    return;
                }
            }
            lastPagePtr += fieldSize;
        }
        lastPage.setNumberOfTuples(numTuplesLastPage + 1);
    }
}

}

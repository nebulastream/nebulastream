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
    const FileLayout fileLayout)
{
    PRECONDITION(!memoryLayout->getSchema()->containsVarSizedDataField(), "NLJSlice does not currently support variable sized data");

    const auto [pagedVector, pagedVectorKeys] = getPagedVectors(joinBuildSide, threadId);
    const auto pages = pagedVector->getPages();

    switch (fileLayout)
    {
        /// Write all tuples consecutivley to file
        case NO_SEPARATION: {
            for (const auto& page : pages)
            {
                fileWriter.write(page.getBuffer(), page.getNumberOfTuples() * memoryLayout->getTupleSize());
            }
            break;
        }
        /// Write only payload to file and move key field data to designated pagedVectorKeys
        case SEPARATE_PAYLOAD: {
            writePayloadOnlyToFile(pages, memoryLayout, bufferProvider, pagedVectorKeys, fileWriter);
            break;
        }
        /// Write payload and key field data to separate files
        case SEPARATE_KEYS: {
            writePayloadAndKeysToSeparateFiles(pages, memoryLayout, pagedVectorKeys, fileWriter);
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
    const FileLayout fileLayout)
{
    PRECONDITION(!memoryLayout->getSchema()->containsVarSizedDataField(), "NLJSlice does not currently support variable sized data");

    const auto [pagedVector, pagedVectorKeys] = getPagedVectors(joinBuildSide, threadId);
    pagedVector->appendPage(bufferProvider, memoryLayout);

    switch (fileLayout)
    {
        /// Read all tuples consecutivley from file
        case NO_SEPARATION: {
            auto lastPage = pagedVector->getLastPage();
            const auto tupleSize = memoryLayout->getTupleSize();
            while (const auto bytesRead = fileReader.read(lastPage.getBuffer(), memoryLayout->getCapacity() * tupleSize))
            {
                lastPage.setNumberOfTuples(bytesRead / tupleSize);
                pagedVector->appendPageIfFull(bufferProvider, memoryLayout);
                lastPage = pagedVector->getLastPage();
            }
            break;
        }
        /// Read payload from file and move key field data from designated pagedVectorKeys
        case SEPARATE_PAYLOAD:
        /// Read payload and key field data from separate files
        case SEPARATE_KEYS: {
            readSeparatelyFromFiles(pagedVector, memoryLayout, bufferProvider, pagedVectorKeys, fileReader);
            break;
        }
    }
    pagedVectorKeys->getPages().clear();
}

void NLJSlice::truncate(const QueryCompilation::JoinBuildSideType joinBuildSide, const WorkerThreadId threadId, const FileLayout fileLayout)
{
    /// Clear the pages without appending a new one afterwards as this will be done in PagedVectorRef
    const auto [pagedVector, pagedVectorKeys] = getPagedVectors(joinBuildSide, threadId);
    // TODO move key field data to designated pagedVectorKeys to be able to write all tuples to file but keep keys in memory, alternatively create mew FileLayout and do this in writeToFile()->writePayloadOnlyToFile()
    if (fileLayout == SEPARATE_KEYS)
    {
        pagedVectorKeys->getPages().clear();
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

uint64_t NLJSlice::getNumberOfWorkerThreads() const
{
    return leftPagedVectors.size();
}

std::tuple<Interface::PagedVector*, Interface::PagedVector*>
NLJSlice::getPagedVectors(const QueryCompilation::JoinBuildSideType joinBuildSide, const WorkerThreadId threadId) const
{
    switch (joinBuildSide)
    {
        case QueryCompilation::JoinBuildSideType::Left: {
            const auto leftPos = threadId % leftPagedVectors.size();
            return std::make_tuple(leftPagedVectors[leftPos].get(), leftPagedVectorsKeys[leftPos].get());
        }
        case QueryCompilation::JoinBuildSideType::Right: {
            const auto rightPos = threadId % rightPagedVectors.size();
            return std::make_tuple(rightPagedVectors[rightPos].get(), rightPagedVectorsKeys[rightPos].get());
        }
        default:
            throw UnknownJoinBuildSide();
    }
}

void NLJSlice::writePayloadOnlyToFile(
    const std::vector<Memory::TupleBuffer>& pages,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    Memory::AbstractBufferProvider* bufferProvider,
    Interface::PagedVector* pagedVectorKeys,
    FileWriter& fileWriter)
{
    const auto keyFieldsOnlyMemoryLayout
        = NES::Util::createMemoryLayout(memoryLayout->createKeyFieldsOnlySchema(), memoryLayout->getBufferSize()).get();
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    for (const auto& page : pages)
    {
        auto pagePtr = page.getBuffer();
        for (auto tupleIdx = 0UL; tupleIdx < page.getNumberOfTuples(); ++tupleIdx)
        {
            pagedVectorKeys->appendPageIfFull(bufferProvider, keyFieldsOnlyMemoryLayout);
            auto lastKeyPage = pagedVectorKeys->getLastPage();
            const auto numTuplesLastKeyPage = lastKeyPage.getNumberOfTuples();
            auto lastKeyPagePtr = lastKeyPage.getBuffer() + numTuplesLastKeyPage * keyFieldsOnlyMemoryLayout->getTupleSize();

            for (const auto [fieldType, fieldSize] : groupedFieldTypeSizes)
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
    const std::vector<Memory::TupleBuffer>& pages,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    Interface::PagedVector* pagedVectorKeys,
    FileWriter& fileWriter)
{
    const auto keyFieldsOnlyMemoryLayout
        = NES::Util::createMemoryLayout(memoryLayout->createKeyFieldsOnlySchema(), memoryLayout->getBufferSize()).get();
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    for (const auto& keyPage : pagedVectorKeys->getPages())
    {
        fileWriter.writeKey(keyPage.getBuffer(), keyPage.getNumberOfTuples() * keyFieldsOnlyMemoryLayout->getTupleSize());
    }

    for (const auto& page : pages)
    {
        auto pagePtr = page.getBuffer();
        for (auto tupleIdx = 0UL; tupleIdx < page.getNumberOfTuples(); ++tupleIdx)
        {
            for (const auto [fieldType, fieldSize] : groupedFieldTypeSizes)
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
    const auto keyFieldsOnlyMemoryLayout
        = NES::Util::createMemoryLayout(memoryLayout->createKeyFieldsOnlySchema(), memoryLayout->getBufferSize()).get();
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    auto keyPageIdx = 0UL;
    const auto keyPages = pagedVectorKeys->getPages();
    auto keyPagePtr = !keyPages.empty() ? keyPages.at(keyPageIdx).getBuffer() : nullptr;

    auto keepReading = true;
    while (keepReading)
    {
        pagedVector->appendPageIfFull(bufferProvider, memoryLayout);
        auto lastPage = pagedVector->getLastPage();
        const auto numTuplesLastPage = lastPage.getNumberOfTuples();
        auto lastPagePtr = lastPage.getBuffer() + numTuplesLastPage * memoryLayout->getTupleSize();

        if (keyPagePtr
            && keyPagePtr - keyPages.at(keyPageIdx).getBuffer()
                >= keyPages.at(keyPageIdx).getNumberOfTuples() * keyFieldsOnlyMemoryLayout->getTupleSize())
        {
            keyPagePtr = ++keyPageIdx < keyPages.size() ? keyPages.at(keyPageIdx).getBuffer() : nullptr;
        }

        for (const auto [fieldType, fieldSize] : groupedFieldTypeSizes)
        {
            if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::KEY)
            {
                if (!fileReader.readKey(lastPagePtr, fieldSize))
                {
                    INVARIANT(keyPagePtr != nullptr, "pagedVectorKeys does not contain any pages");

                    std::memcpy(lastPagePtr, keyPagePtr, fieldSize);
                    keyPagePtr += fieldSize;
                }
            }
            else if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::PAYLOAD)
            {
                keepReading = fileReader.read(lastPagePtr, fieldSize);
            }
            lastPagePtr += fieldSize;
        }
        lastPage.setNumberOfTuples(numTuplesLastPage + 1);
    }
}

}

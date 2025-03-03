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
        keysOnlyLeftPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>());
    }

    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        rightPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>());
        keysOnlyRightPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>());
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
}

void NLJSlice::writeToFile(
    FileWriter& leftFileWriter,
    FileWriter& rightFileWriter,
    const std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> leftMemoryLayout,
    const std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> rightMemoryLayout,
    const WorkerThreadId threadId)
{
    const auto leftTupleSize = leftMemoryLayout->getTupleSize();
    const auto leftPages = leftPagedVectors[threadId.getRawValue()]->getPages();
    for (auto pageIdx = 0UL; pageIdx < leftPages.size(); ++pageIdx)
    {
        // TODO write variable sized data in place to file
        auto page = leftPages[pageIdx];
        leftFileWriter.write(page.getBuffer(), page.getNumberOfTuples() * leftTupleSize);
    }

    const auto rightTupleSize = rightMemoryLayout->getTupleSize();
    const auto rightPages = rightPagedVectors[threadId.getRawValue()]->getPages();
    for (auto pageIdx = 0UL; pageIdx < rightPages.size(); ++pageIdx)
    {
        // TODO write variable sized data in place to file
        auto page = rightPages[pageIdx];
        rightFileWriter.write(page.getBuffer(), page.getNumberOfTuples() * rightTupleSize);
    }
}

void NLJSlice::readFromFile(
    FileReader& leftFileReader,
    FileReader& rightFileReader,
    const std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> leftMemoryLayout,
    const std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> rightMemoryLayout,
    const WorkerThreadId threadId)
{
    (void)leftMemoryLayout;
    (void)rightMemoryLayout;

    // TODO leftPagedVectors[threadId.getRawValue()]->appendPage();
    auto lastPageLeft = leftPagedVectors[threadId.getRawValue()]->getLastPage();
    const auto leftNumTuplesPerPage = leftMemoryLayout->getCapacity();
    const auto leftTupleSize = leftMemoryLayout->getTupleSize();

    while (const auto bytesRead = leftFileReader.read(lastPageLeft.getBuffer(), leftNumTuplesPerPage * leftTupleSize))
    {
        // TODO read variable sized data in place from file
        lastPageLeft.setNumberOfTuples(bytesRead / leftTupleSize);
        // TODO leftPagedVectors[threadId.getRawValue()]->appendPageIfFull();
        lastPageLeft = leftPagedVectors[threadId.getRawValue()]->getLastPage();
    }

    // TODO rightPagedVectors[threadId.getRawValue()]->appendPage();
    auto lastPageRight = rightPagedVectors[threadId.getRawValue()]->getLastPage();
    const auto rightNumTuplesPerPage = rightMemoryLayout->getCapacity();
    const auto rightTupleSize = rightMemoryLayout->getTupleSize();

    while (const auto bytesRead = rightFileReader.read(lastPageRight.getBuffer(), rightNumTuplesPerPage * rightTupleSize))
    {
        // TODO read variable sized data in place from file
        lastPageRight.setNumberOfTuples(bytesRead / rightTupleSize);
        // TODO rightPagedVectors[threadId.getRawValue()]->appendPageIfFull();
        lastPageRight = rightPagedVectors[threadId.getRawValue()]->getLastPage();
    }
}

void NLJSlice::truncate(
    const std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> leftMemoryLayout,
    const std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> rightMemoryLayout,
    const WorkerThreadId threadId)
{
    (void)leftMemoryLayout;
    (void)rightMemoryLayout;

    leftPagedVectors[threadId.getRawValue()]->getPages().clear();
    // TODO leftPagedVectors[threadId.getRawValue()]->appendPageIfFull();

    rightPagedVectors[threadId.getRawValue()]->getPages().clear();
    //TODO rightPagedVectors[threadId.getRawValue()]->appendPageIfFull();
}

size_t NLJSlice::getStateSizeInBytesForThreadId(
    const std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> leftMemoryLayout,
    const std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> rightMemoryLayout,
    const WorkerThreadId threadId)
{
    const auto leftPos = threadId % leftPagedVectors.size();
    const auto* const leftPagedVector = leftPagedVectors[leftPos].get();
    const auto rightPos = threadId % rightPagedVectors.size();
    const auto* const rightPagedVector = rightPagedVectors[rightPos].get();

    const auto leftPageSize = leftMemoryLayout->getBufferSize();
    const auto leftNumPages = leftPagedVector->getNumberOfPages();
    const auto rightPageSize = rightMemoryLayout->getBufferSize();
    const auto rightNumPages = rightPagedVector->getNumberOfPages();

    return leftPageSize * leftNumPages + rightPageSize * rightNumPages;
}

uint64_t NLJSlice::getNumberOfWorkerThreads()
{
    return leftPagedVectors.size();
}

}

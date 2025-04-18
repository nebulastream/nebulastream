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
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/StateSerializationUtil.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>

namespace NES::Runtime::Execution {

NLJSlice::NLJSlice(uint64_t windowStart,
                   uint64_t windowEnd,
                   uint64_t nljSliceId,
                   uint64_t numberOfWorker,
                   BufferManagerPtr& bufferManager,
                   SchemaPtr& leftSchema,
                   uint64_t leftPageSize,
                   SchemaPtr& rightSchema,
                   uint64_t rightPageSize)
    : StreamSlice(windowStart, windowEnd, nljSliceId) {
    for (uint64_t i = 0; i < numberOfWorker; ++i) {
        leftPagedVectors.emplace_back(
            std::make_unique<Nautilus::Interface::PagedVectorVarSized>(bufferManager, leftSchema, leftPageSize));
    }

    for (uint64_t i = 0; i < numberOfWorker; ++i) {
        rightPagedVectors.emplace_back(
            std::make_unique<Nautilus::Interface::PagedVectorVarSized>(bufferManager, rightSchema, rightPageSize));
    }
    NES_TRACE("Created NLJWindow {} for {} workerThreads, resulting in {} leftTuples.size() and {} rightTuples.size()",
              NLJSlice::toString(),
              numberOfWorker,
              leftPagedVectors.size(),
              rightPagedVectors.size());
}

uint64_t NLJSlice::getNumberOfTuplesLeft() {
    uint64_t sum = 0;
    for (auto& pagedVec : leftPagedVectors) {
        sum += pagedVec->getNumberOfEntries();
    }
    return sum;
}

uint64_t NLJSlice::getNumberOfTuplesRight() {
    uint64_t sum = 0;
    for (auto& pagedVec : rightPagedVectors) {
        sum += pagedVec->getNumberOfEntries();
    }
    return sum;
}

std::string NLJSlice::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "(sliceStart: " << sliceStart << " sliceEnd: " << sliceEnd
                       << " leftNumberOfTuples: " << getNumberOfTuplesLeft()
                       << " rightNumberOfTuples: " << getNumberOfTuplesRight() << ")";
    for (size_t pagedIndex = 0; pagedIndex < leftPagedVectors.size(); ++pagedIndex) {
        auto pages = leftPagedVectors[pagedIndex].get()->getPages();
        auto maxCapacity = leftPagedVectors[pagedIndex]->getCapacityPerPage();
        auto pageIdx = 0;
        for (auto& page : pages) {
            NES_ERROR("page {}, tuples {} capacity {}", pageIdx, page.getNumberOfTuples(), maxCapacity);
            auto* records = page.getBuffer<Record>();
            for (uint64_t i = 0; i < maxCapacity; ++i) {
                NES_ERROR("page {}.{} contains record id {}, join id {} value {}", pagedIndex, pageIdx, records[i].id, records[i].joinId, records[i].value);
            }
        }
    }

    for (size_t pagedIndex = 0; pagedIndex < rightPagedVectors.size(); ++pagedIndex) {
        auto pages = rightPagedVectors[pagedIndex].get()->getPages();
        auto pageIdx = 0;
        auto maxCapacity = rightPagedVectors[pagedIndex]->getCapacityPerPage();
        for (auto& page : pages) {
                NES_ERROR("page {}, tuples {} capacity {}", pageIdx, page.getNumberOfTuples(), maxCapacity);
                auto* records = page.getBuffer<Record>();
                for (uint64_t i = 0; i < maxCapacity; ++i) {
                    NES_ERROR("page {}.{} contains record id {}, join id {} value {}", pagedIndex, pageIdx, records[i].id, records[i].joinId, records[i].value);
                }
        }
    }
    return basicOstringstream.str();
}

void* NLJSlice::getPagedVectorRefLeft(WorkerThreadId workerThreadId) {
    const auto pos = workerThreadId % leftPagedVectors.size();
    return leftPagedVectors[pos].get();
}

void* NLJSlice::getPagedVectorRefRight(WorkerThreadId workerThreadId) {
    const auto pos = workerThreadId % rightPagedVectors.size();
    return rightPagedVectors[pos].get();
}

void NLJSlice::combinePagedVectors() {
    NES_TRACE("Combining pagedVectors for window: {}", this->toString());

    // Appending all PagedVectors for the left join side and removing all items except the first one
    if (leftPagedVectors.size() > 1) {
        for (uint64_t i = 1; i < leftPagedVectors.size(); ++i) {
            leftPagedVectors[0]->appendAllPages(*leftPagedVectors[i]);
        }
        leftPagedVectors.erase(leftPagedVectors.begin() + 1, leftPagedVectors.end());
    }

    // Appending all PagedVectors for the right join side and removing all items except the first one
    if (rightPagedVectors.size() > 1) {
        for (uint64_t i = 1; i < rightPagedVectors.size(); ++i) {
            rightPagedVectors[0]->appendAllPages(*rightPagedVectors[i]);
        }
        rightPagedVectors.erase(rightPagedVectors.begin() + 1, rightPagedVectors.end());
    }
}

std::vector<Runtime::TupleBuffer> NLJSlice::serialize(BufferManagerPtr& bufferManager) {

    auto buffersToTransfer = std::vector<Runtime::TupleBuffer>();

    auto mainMetadata = bufferManager->getBufferBlocking();
    buffersToTransfer.emplace(buffersToTransfer.begin(), mainMetadata);
    uint64_t metadataBuffersCount = 1;

    // check that tuple buffer size is more than uint64_t to write number of metadata buffers
    if (!mainMetadata.hasSpaceLeft(0, sizeof(uint64_t))) {
        NES_THROW_RUNTIME_ERROR(
            "Buffer size has to be at least greater or equal to uint64_t in size for successful state migration.");
    }
    auto metadataPtr = mainMetadata.getBuffer<uint64_t>();
    uint64_t metadataIdx = 1;

    /** @brief Lambda to write to metadata buffers */
    auto writeToMetadata = [&mainMetadata, &metadataPtr, &metadataIdx, &bufferManager, &metadataBuffersCount, &buffersToTransfer](
                               uint64_t dataToWrite) {
        StateSerializationUtil::writeToBuffer(bufferManager,
                                              mainMetadata.getBufferSize(),
                                              metadataPtr,
                                              metadataIdx,
                                              metadataBuffersCount,
                                              buffersToTransfer,
                                              dataToWrite);
    };
    // NOTE: Do not change the order of writes to metadata (order is documented in function declaration)
    // 0. Store slice id
    writeToMetadata(sliceId);
    // 1. Store slice start
    writeToMetadata(sliceStart);
    // 2. Store slice end
    writeToMetadata(sliceEnd);
    // 3. Store number of worker threads
    writeToMetadata(leftPagedVectors.size());

    for (auto& pagedVec : leftPagedVectors) {
        // 4. Store number of pages in i-th left vector
        writeToMetadata(pagedVec->getNumberOfPages());
        // get all pages from vector
        buffersToTransfer.insert(buffersToTransfer.end(), pagedVec->getPages().begin(), pagedVec->getPages().end());
        // set number of tuples on page for the last page
        buffersToTransfer.back().setNumberOfTuples(pagedVec->getNumberOfEntriesOnCurrentPage());
        for (auto& page : pagedVec->getPages()) {
            NES_ERROR("number of entries on the page: {}", page.getNumberOfTuples());
        }
    }

    for (auto& pagedVec : rightPagedVectors) {
        // 5. Store number of pages in i-th right vector
        writeToMetadata(pagedVec->getNumberOfPages());
        // get all pages from vector
        buffersToTransfer.insert(buffersToTransfer.end(), pagedVec->getPages().begin(), pagedVec->getPages().end());
        // set number of tuples on page for the last page
        buffersToTransfer.back().setNumberOfTuples(pagedVec->getNumberOfEntriesOnCurrentPage());
    }

    // set number of metadata buffers to the first metadata buffer
    mainMetadata.getBuffer<uint64_t>()[0] = metadataBuffersCount;

    return buffersToTransfer;
}

StreamSlicePtr NLJSlice::deserialize(BufferManagerPtr& bufferManager,
                                     SchemaPtr& leftSchema,
                                     uint64_t leftPageSize,
                                     SchemaPtr& rightSchema,
                                     uint64_t rightPageSize,
                                     std::span<const Runtime::TupleBuffer> buffers) {

    // create new slice with 0 number of worker thread (this will create empty vector of var sized pages) and 0 in slice start and end
    auto newSlice = std::make_shared<NLJSlice>(0, 0, 0, 0, bufferManager, leftSchema, leftPageSize, rightSchema, rightPageSize);

    // get main metadata buffer
    uint64_t metadataBuffersIdx = 0;
    auto metadataPtr = buffers[metadataBuffersIdx++].getBuffer<uint64_t>();
    uint64_t metadataIdx = 0;

    // read number of metadata buffers
    auto numberOfMetadataBuffers = metadataPtr[metadataIdx++];

    /** @brief Lambda to read from metadata buffers */
    auto readFromMetadata = [&metadataPtr, &metadataIdx, &metadataBuffersIdx, &buffers]() -> uint64_t {
        return StateSerializationUtil::readFromBuffer(metadataPtr, metadataIdx, metadataBuffersIdx, buffers);
    };
    // NOTE: Do not change the order of reads from metadata (order is documented in function declaration)
    // 0. Retrieve slice id
    newSlice->sliceId = readFromMetadata();
    // 1. Retrieve slice start
    newSlice->sliceStart = readFromMetadata();
    // 2. Retrieve slice end
    newSlice->sliceEnd = readFromMetadata();
    // 3. Retrieve number of worker threads
    auto numberOfWorker = readFromMetadata();

    auto buffIdx = 0UL;
    // recreate left paged vectors
    for (uint64_t i = 0; i < numberOfWorker; ++i) {
        // 4. Retrieve number of pages in i-th left vector
        auto numberOfPages = readFromMetadata();

        newSlice->leftPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::PagedVectorVarSized>(
            bufferManager,
            leftSchema,
            std::span<const Runtime::TupleBuffer>(buffers.data() + numberOfMetadataBuffers + buffIdx, numberOfPages),
            leftPageSize));
        buffIdx += numberOfPages;
    }

    // recreate right paged vectors
    for (uint64_t i = 0; i < numberOfWorker; ++i) {
        // 5. Retrieve number of pages in i-th right vector
        auto numberOfPages = readFromMetadata();

        newSlice->rightPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::PagedVectorVarSized>(
            bufferManager,
            rightSchema,
            std::span<const Runtime::TupleBuffer>(buffers.data() + numberOfMetadataBuffers + buffIdx, numberOfPages),
            rightPageSize));
        buffIdx += numberOfPages;
    }

    return newSlice;
}
};// namespace NES::Runtime::Execution

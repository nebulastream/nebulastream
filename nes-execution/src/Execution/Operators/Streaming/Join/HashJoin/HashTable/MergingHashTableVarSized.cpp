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

#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/MergingHashTableVarSized.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {
void MergingHashTableVarSized::insertBucket(size_t bucketPos, Nautilus::Interface::PagedVectorVarSizedPtr pagedVector) {
    auto& numItems = bucketNumItems[bucketPos];
    auto& numPages = bucketNumPages[bucketPos];
    auto lockedBucketHeads = bucketHeads[bucketPos].wlock();

    if (*lockedBucketHeads == nullptr) {
        (*lockedBucketHeads) = pagedVector;
    } else {
        (*lockedBucketHeads)->appendAllPages(*pagedVector);
    }
    numItems += pagedVector->getNumberOfEntries();
    numPages += pagedVector->getPages().size();
}

void* MergingHashTableVarSized::getBucketAtPos(size_t bucketPos) const {
    auto lockedBucketHeads = bucketHeads[bucketPos].rlock();
    return lockedBucketHeads->get();
}

uint64_t MergingHashTableVarSized::getNumberOfTuplesForPage(size_t bucketPos, size_t pageNo) const {
    auto lockedBucketHeads = bucketHeads[bucketPos].rlock();
    NES_ASSERT2_FMT(lockedBucketHeads->get()->getNumberOfPages() > pageNo, "Accessing a page that does not exist!");
    return lockedBucketHeads->get()->getPages()[pageNo].getNumberOfTuples();
}

size_t MergingHashTableVarSized::getNumItems(size_t bucketPos) const { return bucketNumItems[bucketPos].load(); }

size_t MergingHashTableVarSized::getNumPages(size_t bucketPos) const { return bucketNumPages[bucketPos].load(); }

size_t MergingHashTableVarSized::getNumBuckets() const { return bucketNumPages.size(); }

MergingHashTableVarSized::MergingHashTableVarSized(size_t numBuckets)
    : bucketHeads(numBuckets), bucketNumItems(numBuckets), bucketNumPages(numBuckets) {}

}// namespace NES::Runtime::Execution::Operators

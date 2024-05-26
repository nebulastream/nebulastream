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

#include <Execution/Operators/Streaming/Join/HashJoin/HashTable/StreamJoinHashTableVarSized.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution::Operators {
StreamJoinHashTableVarSized::StreamJoinHashTableVarSized(size_t numPartitions,
                                                         BufferManagerPtr& bufferManager,
                                                         size_t pageSize,
                                                         SchemaPtr& schema)
    : mask(numPartitions - 1) {

    for (auto i = 0UL; i < numPartitions; ++i) {
        buckets.emplace_back(std::make_unique<Nautilus::Interface::PagedVectorVarSized>(bufferManager, schema, pageSize));
    }
}

Nautilus::Interface::PagedVectorVarSizedPtr StreamJoinHashTableVarSized::insert(uint64_t key) const {
    auto hashedKey = Util::murmurHash(key);
    return getBucketPagedVector(getBucketPos(hashedKey));
}

size_t StreamJoinHashTableVarSized::getBucketPos(uint64_t hash) const {
    if (mask == 0) {
        return 0;
    }
    return hash % mask;
}

Nautilus::Interface::PagedVectorVarSizedPtr StreamJoinHashTableVarSized::getBucketPagedVector(size_t bucketPos) const {
    NES_ASSERT2_FMT(bucketPos < buckets.size(), "Tried to access a bucket that does not exist in StreamJoinHashTableVarSized!");
    return buckets[bucketPos];
}

uint64_t StreamJoinHashTableVarSized::getNumberOfTuples() const {
    size_t cnt = 0;
    for (const auto& bucket : buckets) {
        cnt += bucket->getNumberOfEntries();
    }

    NES_DEBUG("HashTable has {} tuples", cnt);
    return cnt;
}

size_t StreamJoinHashTableVarSized::getNumItems(size_t bucketPos) const { return buckets[bucketPos]->getNumberOfEntries(); }

size_t StreamJoinHashTableVarSized::getNumPages(size_t bucketPos) const { return buckets[bucketPos]->getPages().size(); }

size_t StreamJoinHashTableVarSized::getNumBuckets() const { return buckets.size(); }

}// namespace NES::Runtime::Execution::Operators

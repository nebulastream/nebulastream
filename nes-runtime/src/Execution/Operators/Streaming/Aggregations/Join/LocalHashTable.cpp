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

#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinBuild.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinUtil.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LocalHashTable.hpp>

namespace NES::Runtime::Execution::Operators {

void LocalHashTable::insert(uint64_t key, const std::string& joinFieldName) const {

    auto hashedKey = Util::murmurHash(key);
    buckets[hashedKey & LazyJoinOperatorHandler::MASK]->append(hashedKey);

}
FixedPagesLinkedList* LocalHashTable::getBucketLinkedList(size_t bucketPos) const {
    NES_ASSERT2_FMT(bucketPos < buckets.size(), "Tried to access a bucket that does not exist in LocalHashTable!");

    return buckets[bucketPos];
}
LocalHashTable::LocalHashTable(SchemaPtr schema,
                               size_t numPartitions,
                               std::atomic<uint64_t>& tail,
                               size_t overrunAddress) : schema(schema) {
    buckets.reserve(numPartitions);
    for (auto i = 0UL; i < numPartitions; ++i) {
        buckets.emplace_back(new FixedPagesLinkedList(tail, overrunAddress, schema->getSchemaSizeInBytes()));
    }
}

} // namespace NES::Runtime::Execution::Operators
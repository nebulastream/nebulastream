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

#ifndef NES_INCLUDE_RUNTIME_INMEMORYLINEAGEMANAGER_HPP_
#define NES_INCLUDE_RUNTIME_INMEMORYLINEAGEMANAGER_HPP_

#include <Runtime/AbstractLineageManager.hpp>
#include <Util/BufferSequenceNumber.hpp>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace NES::Runtime {

/**
 * @brief The Lineage Manager class stores map of all tuples that got their sequence number changed
 * by stateful operators
 */
class InMemoryLineageManager : public AbstractLineageManager {
  public:
    InMemoryLineageManager() = default;

    /**
     * @brief Inserts a pair newId, oldId into bufferAncestorMapping, where newId is a key
     * @param newId new sequence number that was created by a stateful operator
     * @param oldId old sequence number that the tuple had
     */
    void insert(BufferSequenceNumber newBufferSequenceNumber, BufferSequenceNumber oldBufferSequenceNumber) override;

    /**
     * @brief Deletes a pair<newId,oldId> from bufferAncestorMapping manager
     * @param id newId of the tuple
     * @return true in case of a success trimming
     */
    bool trim(BufferSequenceNumber bufferSequenceNumber) override;

    /**
     * @brief Finds an old id for the tuple with a given id
     * @param id new id of the tuple
     * @return old id of the tuple
     */
    std::vector<BufferSequenceNumber> findTupleBufferAncestor(BufferSequenceNumber bufferSequenceNumber);

    /**
     * @brief Return current bufferAncestorMapping size
     * @return Current bufferAncestorMapping size
     */
    size_t getLineageSize() const override;

  private:
    ///this unordered map maps new buffer sequence numbers to old ones, which tuple buffer had before a statefull operator

    std::unordered_map<BufferSequenceNumber, std::vector<BufferSequenceNumber>> bufferAncestorMapping;
    mutable std::mutex mutex;
};

using LineageManagerPtr = std::shared_ptr<Runtime::InMemoryLineageManager>;

}// namespace NES::Runtime

#endif  // NES_INCLUDE_RUNTIME_INMEMORYLINEAGEMANAGER_HPP_

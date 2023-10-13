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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWHANDLER_BATCHJOINHANDLER_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWHANDLER_BATCHJOINHANDLER_HPP_
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateManager.hpp>
#include <State/StateVariable.hpp>
#include <Util/Core.hpp>
#include <Util/libcuckoo/cuckoohash_map.hh>
#include<Operators/LogicalOperators/Windows/Joins/JoinForwardRefs.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDefinition.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowHandler/AbstractBatchJoinHandler.hpp>
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>

namespace NES::Join::Experimental {

template<class KeyType, class InputTypeBuild>
using HashTable = cuckoohash_map<KeyType, InputTypeBuild>;
template<class KeyType, class InputTypeBuild>
using HashTablePtr = std::shared_ptr<HashTable<KeyType, InputTypeBuild>>;

template<class KeyType, class InputTypeBuild>
class BatchJoinHandler : public AbstractBatchJoinHandler {
  public:
    explicit BatchJoinHandler(const LogicalBatchJoinDefinitionPtr& batchJoinDefinition, uint64_t id)
        : AbstractBatchJoinHandler(std::move(batchJoinDefinition)), id(id), refCnt(2), isRunning(false),
          hashTable(std::make_shared<HashTable<KeyType, InputTypeBuild>>()) {
        NES_ASSERT(this->batchJoinDefinition, "invalid join definition");
        NES_TRACE("Created join handler with id={}", id);
    }

    static AbstractBatchJoinHandlerPtr create(LogicalBatchJoinDefinitionPtr batchJoinDefinition, uint64_t id) {
        return std::make_shared<BatchJoinHandler>(batchJoinDefinition, id);
    }

    std::string toString() override {
        std::stringstream ss;
        ss << "BatchJoinHandler id=" << id;
        return ss.str();
    }

    /**
     * @brief Returns the join state in form of the hash table.
     * @return Templated Hashtable.
     */
    HashTablePtr<KeyType, InputTypeBuild> getHashTable() { return hashTable; }

    /**
     * @brief reconfigure machinery for the join handler: do not nothing (for now)
     * @param message
     * @param context
     */
    void reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) override {
        // todo jm
        AbstractBatchJoinHandler::reconfigure(message, context);
    }

    /**
     * @brief Reconfiguration machinery on the last thread for the join handler: react to soft or hard termination
     * @param message
     */
    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override {
        // todo jm
        AbstractBatchJoinHandler::postReconfigurationCallback(message);
    }

  private:
    std::recursive_mutex mutex;
    uint64_t id;
    std::atomic<uint32_t> refCnt;
    std::atomic<bool> isRunning;
    HashTablePtr<KeyType, InputTypeBuild> hashTable;
};
}// namespace NES::Join::Experimental
#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWHANDLER_BATCHJOINHANDLER_HPP_

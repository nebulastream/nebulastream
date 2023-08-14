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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINHANDLER_HPP_
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <vector>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief The batch join handler maintains the join state that is used by the join build and the join probe operator.
 * To this end, we use a two-phase algorithm proposed by Leis et al.
 * Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age
 * https://db.in.tum.de/~leis/papers/morsels.pdf
 *
 * The build phase, consumes input records from the build side and materializes hash-entries in a thread local paged vector.
 * If all records, are consumed, we build a global hash-map on top of all materialized values (see mergeState).
 * The final probe phase, consumes the probe side and performs key lookups in the global hash-map.
 * TODO: We could further, improve this operator by adding support for concurrent merges of the local paged vectors to the hash table.
 * This code improve performance, but it should first investigated if the merging becomes a performance bottleneck.
 */
class BatchJoinHandler : public Runtime::Execution::OperatorHandler,
                         public NES::detail::virtual_enable_shared_from_this<BatchJoinHandler, false> {

  public:
    /**
     * @brief Creates the operator handler for the join operator.
     */
    BatchJoinHandler();

    /**
     * @brief Initializes the thread local state for the join operator
     * @param ctx PipelineExecutionContext
     * @param entrySize Size of the aggregated values in memory
     * @param keySize Size of the key values in memory
     * @param valueSize Size of the value values in memory
     * @param markBool If true, we init extra space at the key for a bool to mark seen entries in the hash map (used for semi/anti-joins)
     */
    void setup(Runtime::Execution::PipelineExecutionContext& ctx,
               uint64_t entrySize,
               uint64_t keySize,
               uint64_t valueSize,
               bool markBool);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Returns the thread local state for a  specific worker thread id
     * @param workerId
     * @return Nautilus::Interface::PagedVector*
     */
    Nautilus::Interface::PagedVector* getThreadLocalState(uint64_t workerId);

    /**
     * @brief This function creates the global hash map. To this end, it builds a new hash map based on the thread local paged vectors.
     * @return ChainedHashMap*
     */
    Nautilus::Interface::ChainedHashMap* mergeState();

    /**
     * @brief Returns a reference to the global hash map
     * @return ChainedHashMap*
     */
    Nautilus::Interface::ChainedHashMap* getGlobalHashMap();

    ~BatchJoinHandler();

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

  private:
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVector>> threadLocalStateStores;
    std::unique_ptr<Nautilus::Interface::ChainedHashMap> globalMap;
    uint64_t keySize;
    uint64_t valueSize;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINHANDLER_HPP_

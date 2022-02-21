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
#ifndef NES_REPLICATIONSERVICE_H
#define NES_REPLICATIONSERVICE_H
#include <Components/NesCoordinator.hpp>
#include <unordered_map>
#include <utility>
#include <mutex>

namespace NES {

using EpochId = int;

/**
 * @brief: This class is located at the coordinator side and responsible for notifying all sources that participate in the query about current
 * epoch barrier. It saves current  For the given query id it finds logical sources and maps them to the physical ones. For every physical source it creates
 * a RPCWorkerClient to send current epoch barrier.
 */
class ReplicationService {
  public:
    ReplicationService(NesCoordinatorPtr coordinatorPtr);
    ~ReplicationService() = default;

    /**
     * @brief method to propagate new epoch timestamp to source nodes
     * @param epochBarrier: max timestamp of current epoch
     * @param queryId: identifies what query sends punctuation
     * @return bool indicating success
     */
    bool notifyEpochTermination(uint64_t epochBarrier, uint64_t queryId) const;

    /**
     * @brief getter of current epoch barrier for a given query id
     * @param queryId current query id
     * @return current epoch barrier
     */
    EpochId getCurrentEpochBarrier(uint64_t queryId, uint64_t epoch) const;

  private:

    /**
     * @brief saves current epoch barrier for a given query id and epoch
     * @param queryId current query id
     * @param epoch current epoch of the given query
     */
    void saveEpochBarrier(uint64_t queryId, uint64_t epoch) const;

    /**
     * @brief finds logical sources for a given query id
     * @param queryId current query id
     * @return vector of logical sources
     */
    std::vector<SourceLogicalOperatorNodePtr> getLogicalSources(uint64_t queryId) const;

    /**
     * @brief maps a logical source to a vector of physical sources
     * @param logicalSource one of the logical sources of a query
     * @return vector of physical sources
     */
    std::vector<TopologyNodePtr> getPhysicalSources(SourceLogicalOperatorNodePtr logicalSource) const;

    mutable std::recursive_mutex replicationServiceMutex;
    NesCoordinatorPtr coordinatorPtr;
    mutable std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> queryIdToCurrentEpochBarrierMap;
};
using ReplicationServicePtr = std::shared_ptr<ReplicationService>;
}
#endif//NES_REPLICATIONSERVICE_H

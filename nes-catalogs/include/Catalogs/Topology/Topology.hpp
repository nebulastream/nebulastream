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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGY_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGY_HPP_

#include <Identifiers.hpp>
#include <any>
#include <folly/Synchronized.h>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace NES {

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

/**
 * @brief This class represents the overall physical infrastructure with different nodes
 */
class Topology {

  public:
    /**
     * @brief Factory to create instance of topology
     * @return shared pointer to the topology
     */
    static TopologyPtr create();

    /**
     * @brief Get the root node of the topology
     * @return root of the topology
     */
    TopologyNodePtr getRoot();

    /**
     * @brief Register a new node in the topology map without creating parent child relationship
     * @param newTopologyNode : the shared pointer of the new node
     * @return true if registered else false
     */
    bool registerTopologyNode(TopologyNodePtr&& newTopologyNode);

    /**
     * @brief This method will add the a topology node as child to the parent with provided Id
     * @param parentWorkerId : the id of the parent topology node
     * @param childWorkerId : the new topology node.
     * @return true if successful if one or both node does not exists then false
     */
    bool addTopologyNodeAsChild(WorkerId parentWorkerId, WorkerId childWorkerId);

    /**
     * @brief This method will remove a given topology node
     * @param nodeToRemove : the node to be removed
     * @return true if successful
     */
    bool removeTopologyNode(const TopologyNodePtr& nodeToRemove);

    /**
     * @brief This method will find a given worker node by its id
     * @param workerId : the id of the worker
     * @return Topology node if found else nullptr
     */
    TopologyNodePtr findWorkerWithId(WorkerId workerId);

    /**
     * @brief checks if a topology node with workerId exists
     * @param workerId: workerId of the node
     * @return true if exists, false otherwise
     */
    bool nodeWithWorkerIdExists(WorkerId workerId);

    /**
     * @brief Add a topology node as the root node of the topology
     * @param physicalNode: topology node to be set as root node
     */
    void setAsRoot(const TopologyNodePtr& physicalNode);

    /**
     * @brief Remove links between
     * @param parentNode
     * @param childNode
     * @return
     */
    bool removeNodeAsChild(const TopologyNodePtr& parentNode, const TopologyNodePtr& childNode);

    /**
     * @brief acquire the lock on the topology node
     * @param workerId : the id of the topology node
     * @return true if successfully acquired the lock else false
     */
    bool acquireLockOnTopologyNode(WorkerId workerId);

    /**
     * @brief release the lock on the topology node
     * @param workerId : the id of the topology node
     * @return true if successfully released the lock else false
     */
    bool releaseLockOnTopologyNode(WorkerId workerId);

    /**
     * @brief Increase the amount of resources on the node with the id
     * @param workerId : the node id
     * @param amountToRelease : resources to free
     * @return true if successful
     */
    bool releaseSlots(WorkerId workerId, uint16_t amountToRelease);

    /**
     * @brief Reduce the amount of resources on the node with given id
     * @param workerId : the worker id
     * @param amountToOccupy : amount of resources to reduce
     * @return true if successful
     */
    bool occupySlots(WorkerId workerId, uint16_t amountToOccupy);

    /**
     * @brief Print the current topology information
     */
    void print();

    /**
     * @brief Return graph as string
     * @return string object representing topology information
     */
    std::string toString();

  private:
    explicit Topology();

    //TODO: At present we assume that we have only one root node
    WorkerId rootWorkerId;
    folly::Synchronized<std::map<WorkerId, folly::Synchronized<TopologyNodePtr>>> workerIdToTopologyNode;
};
}// namespace NES
#endif// NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGY_HPP_

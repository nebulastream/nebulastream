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
#ifndef NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_CONSERVATIVETWOPHASELOCKMANAGER_HPP
#define NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_CONSERVATIVETWOPHASELOCKMANAGER_HPP
#include <WorkQueues/StorageHandles/StorageHandler.hpp>

//todo #3588: This class is not not needed if we move the mutexes into the data structures itself
/**
 * @brief This class hands out locks to different instances of the two phase locking
 * storage handle. This way each request can have its own storage handle but still have thread safe access.
 */
namespace NES {
class LockManager {
  public:
    /**
     * @brief Obtain a lock for a resource
     * @param type: The kind of resource that should be locked
     * @return a lock for the mutex corresponding to the resource specified in the 'type' parameter
     */
    std::unique_lock<std::mutex> getLock(StorageHandlerResourceType type);

  private:
    std::mutex topologyMutex;
    std::mutex queryCatalogMutex;
    std::mutex sourceCatalogMutex;
    std::mutex globalExecutionPlanMutex;
    std::mutex globalQueryPlanMutex;
    std::mutex udfCatalogMutex;
};
}// namespace NES
#endif//NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_CONSERVATIVETWOPHASELOCKMANAGER_HPP

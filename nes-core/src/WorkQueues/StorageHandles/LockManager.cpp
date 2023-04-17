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
#include <WorkQueues/StorageHandles/LockManager.hpp>
#include <WorkQueues/StorageHandles/StorageHandlerResourceType.hpp>

namespace NES {
std::unique_lock<std::mutex> LockManager::getLock(StorageHandlerResourceType type) {
    switch (type) {
        case StorageHandlerResourceType::Topology:
            return std::unique_lock(topologyMutex);
        case StorageHandlerResourceType::QueryCatalogService:
            return std::unique_lock(queryCatalogMutex);
        case StorageHandlerResourceType::SourceCatalog:
            return std::unique_lock(sourceCatalogMutex);
        case StorageHandlerResourceType::GlobalExecutionPlan:
            return std::unique_lock(globalExecutionPlanMutex);
        case StorageHandlerResourceType::GlobalQueryPlan:
            return std::unique_lock(globalQueryPlanMutex);
        case StorageHandlerResourceType::UdfCatalog:
            return std::unique_lock(udfCatalogMutex);
    }
}
}// namespace NES

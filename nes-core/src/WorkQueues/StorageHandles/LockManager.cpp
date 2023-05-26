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
#include <WorkQueues/StorageHandles/ResourceType.hpp>

namespace NES {
LockManager::LockManager(GlobalExecutionPlanPtr globalExecutionPlan,
                         TopologyPtr topology,
                         QueryCatalogServicePtr queryCatalogService,
                         GlobalQueryPlanPtr globalQueryPlan,
                         Catalogs::Source::SourceCatalogPtr sourceCatalog,
                         Catalogs::UDF::UDFCatalogPtr udfCatalog)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topology)),
      queryCatalogService(std::move(queryCatalogService)), globalQueryPlan(std::move(globalQueryPlan)),
      sourceCatalog(std::move(sourceCatalog)), udfCatalog(std::move(udfCatalog)) {}

GlobalExecutionPlanHandle LockManager::getGlobalExecutionPlanHandle() {
    return {&*globalExecutionPlan, UnlockDeleter(globalExecutionPlanMutex)};
}
TopologyHandle LockManager::getTopologyHandle() { return {&*topology, UnlockDeleter(topologyMutex)}; }

QueryCatalogServiceHandle LockManager::getQueryCatalogHandle() {
    return {&*queryCatalogService, UnlockDeleter(queryCatalogMutex)};
}

GlobalQueryPlanHandle LockManager::getGlobalQueryPlanHandle() { return {&*globalQueryPlan, UnlockDeleter(globalQueryPlanMutex)}; }

SourceCatalogHandle LockManager::getSourceCatalogHandle() { return {&*sourceCatalog, UnlockDeleter(sourceCatalogMutex)}; }

UDFCatalogHandle LockManager::getUDFCatalogHandle() { return {&*udfCatalog, UnlockDeleter(udfCatalogMutex)}; }

}// namespace NES

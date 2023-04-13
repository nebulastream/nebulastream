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

#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandle.hpp>
#include <utility>

namespace NES {

TwoPhaseLockingStorageHandle::TwoPhaseLockingStorageHandle(GlobalExecutionPlanPtr globalExecutionPlan,
                                                           TopologyPtr topology,
                                                           QueryCatalogServicePtr queryCatalogService,
                                                           GlobalQueryPlanPtr globalQueryPlan,
                                                           Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                           Catalogs::UDF::UdfCatalogPtr udfCatalog)
    : StorageHandle(std::move(globalExecutionPlan),
                    std::move(topology),
                    std::move(queryCatalogService),
                    std::move(globalQueryPlan),
                    std::move(sourceCatalog),
                    std::move(udfCatalog)) {}

std::shared_ptr<TwoPhaseLockingStorageHandle>
TwoPhaseLockingStorageHandle::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                     const TopologyPtr& topology,
                                     const QueryCatalogServicePtr& queryCatalogService,
                                     const GlobalQueryPlanPtr& globalQueryPlan,
                                     const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                     const Catalogs::UDF::UdfCatalogPtr& udfCatalog) {
    return std::make_shared<TwoPhaseLockingStorageHandle>(globalExecutionPlan,
                                                          topology,
                                                          queryCatalogService,
                                                          globalQueryPlan,
                                                          sourceCatalog,
                                                          udfCatalog);
}

TopologyHandle TwoPhaseLockingStorageHandle::getTopologyHandle() {
    return {&*topology, UnlockDeleter(topologyMutex, std::try_to_lock)};
}

QueryCatalogServiceHandle TwoPhaseLockingStorageHandle::getQueryCatalogHandle() {
    return {&*queryCatalogService, UnlockDeleter(queryCatalogMutex, std::try_to_lock)};
}

SourceCatalogHandle TwoPhaseLockingStorageHandle::getSourceCatalogHandle() {
    return {&*sourceCatalog, UnlockDeleter(sourceCatalogMutex, std::try_to_lock)};
}

GlobalExecutionPlanHandle TwoPhaseLockingStorageHandle::getGlobalExecutionPlanHandle() {
    return {&*globalExecutionPlan, UnlockDeleter(globalExecutionPlanMutex, std::try_to_lock)};
}

GlobalQueryPlanHandle TwoPhaseLockingStorageHandle::getGlobalQueryPlanHandle() {
    return {&*globalQueryPlan, UnlockDeleter(globalQueryPlanMutex, std::try_to_lock)};
}

UdfCatalogHandle TwoPhaseLockingStorageHandle::getUdfCatalogHandle() {
    return {&*udfCatalog, UnlockDeleter(udfCatalogMutex, std::try_to_lock)};
}
}// namespace NES

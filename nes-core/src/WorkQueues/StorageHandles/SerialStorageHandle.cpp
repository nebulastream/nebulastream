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
#include <Topology/Topology.hpp>
#include <WorkQueues/StorageHandles/SerialStorageHandle.hpp>
#include <memory>
#include <utility>

namespace NES {

SerialStorageHandle::SerialStorageHandle(GlobalExecutionPlanPtr globalExecutionPlan,
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

std::shared_ptr<SerialStorageHandle> SerialStorageHandle::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                                 const TopologyPtr& topology,
                                                                 const QueryCatalogServicePtr& queryCatalogService,
                                                                 const GlobalQueryPlanPtr& globalQueryPlan,
                                                                 const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                                 const Catalogs::UDF::UdfCatalogPtr& udfCatalog) {
    return std::make_shared<SerialStorageHandle>(globalExecutionPlan,
                                                 topology,
                                                 queryCatalogService,
                                                 globalQueryPlan,
                                                 sourceCatalog,
                                                 udfCatalog);
}

GlobalExecutionPlanHandle SerialStorageHandle::getGlobalExecutionPlanHandle() { return {&*globalExecutionPlan, UnlockDeleter()}; }

TopologyHandle SerialStorageHandle::getTopologyHandle() { return {&*topology, UnlockDeleter()}; }

QueryCatalogServiceHandle SerialStorageHandle::getQueryCatalogHandle() { return {&*queryCatalogService, UnlockDeleter()}; }

GlobalQueryPlanHandle SerialStorageHandle::getGlobalQueryPlanHandle() { return {&*globalQueryPlan, UnlockDeleter()}; }

SourceCatalogHandle SerialStorageHandle::getSourceCatalogHandle() { return {&*sourceCatalog, UnlockDeleter()}; }

UdfCatalogHandle SerialStorageHandle::getUdfCatalogHandle() {
    return {&*udfCatalog, UnlockDeleter()};
}

void SerialStorageHandle::preExecution(std::vector<StorageHandleResourceType> requiredResources) {
    (void) requiredResources;
}
}

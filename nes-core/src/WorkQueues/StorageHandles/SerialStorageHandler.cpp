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
#include <WorkQueues/StorageHandles/SerialStorageHandler.hpp>
#include <memory>
#include <utility>

namespace NES {

SerialStorageHandler::SerialStorageHandler(GlobalExecutionPlanPtr globalExecutionPlan,
                                           TopologyPtr topology,
                                           QueryCatalogServicePtr queryCatalogService,
                                           GlobalQueryPlanPtr globalQueryPlan,
                                           Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                           Catalogs::UDF::UdfCatalogPtr udfCatalog)
    : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topology)),
      queryCatalogService(std::move(queryCatalogService)), globalQueryPlan(std::move(globalQueryPlan)),
      sourceCatalog(std::move(sourceCatalog)), udfCatalog(std::move(udfCatalog)) {}

std::shared_ptr<SerialStorageHandler> SerialStorageHandler::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                                   const TopologyPtr& topology,
                                                                   const QueryCatalogServicePtr& queryCatalogService,
                                                                   const GlobalQueryPlanPtr& globalQueryPlan,
                                                                   const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                                   const Catalogs::UDF::UdfCatalogPtr& udfCatalog) {
    return std::make_shared<SerialStorageHandler>(globalExecutionPlan,
                                                  topology,
                                                  queryCatalogService,
                                                  globalQueryPlan,
                                                  sourceCatalog,
                                                  udfCatalog);
}

GlobalExecutionPlanHandle SerialStorageHandler::getGlobalExecutionPlanHandle() {
    return {&*globalExecutionPlan, UnlockDeleter()};
}

TopologyHandle SerialStorageHandler::getTopologyHandle() { return {&*topology, UnlockDeleter()}; }

QueryCatalogServiceHandle SerialStorageHandler::getQueryCatalogHandle() { return {&*queryCatalogService, UnlockDeleter()}; }

GlobalQueryPlanHandle SerialStorageHandler::getGlobalQueryPlanHandle() { return {&*globalQueryPlan, UnlockDeleter()}; }

SourceCatalogHandle SerialStorageHandler::getSourceCatalogHandle() { return {&*sourceCatalog, UnlockDeleter()}; }

UdfCatalogHandle SerialStorageHandler::getUdfCatalogHandle() { return {&*udfCatalog, UnlockDeleter()}; }

void SerialStorageHandler::acquireResources(std::vector<StorageHandlerResourceType>) {}
}// namespace NES

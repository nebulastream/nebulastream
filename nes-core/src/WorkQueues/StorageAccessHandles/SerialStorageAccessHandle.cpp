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
#include "WorkQueues/StorageAccessHandles/SerialStorageAccessHandle.hpp"
#include "Topology/Topology.hpp"
#include <memory>
namespace NES {

SerialStorageAccessHandle::SerialStorageAccessHandle(GlobalExecutionPlanPtr globalExecutionPlan,
                                                     TopologyPtr topology,
                                                     QueryCatalogServicePtr queryCatalogService,
                                                     GlobalQueryPlanPtr globalQueryPlan,
                                                     Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                     Catalogs::UDF::UdfCatalogPtr udfCatalog)
    : StorageAccessHandle(globalExecutionPlan, topology, queryCatalogService, globalQueryPlan, sourceCatalog, udfCatalog) {}

std::shared_ptr<SerialStorageAccessHandle> SerialStorageAccessHandle::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                     TopologyPtr topology,
                                                     QueryCatalogServicePtr queryCatalogService,
                                                     GlobalQueryPlanPtr globalQueryPlan,
                                                     Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                     Catalogs::UDF::UdfCatalogPtr udfCatalog) {
    return std::make_shared<SerialStorageAccessHandle>(globalExecutionPlan, topology, queryCatalogService, globalQueryPlan, sourceCatalog, udfCatalog);
}

GlobalExecutionPlanHandle SerialStorageAccessHandle::getGlobalExecutionPlanHandle() {
    return NES::GlobalExecutionPlanHandle();
}

bool SerialStorageAccessHandle::requiresRollback() {
    return true;
}

TopologyHandle SerialStorageAccessHandle::getTopologyHandle() {
    return {&*topology, UnlockDeleter()};
}

QueryCatalogServiceHandle SerialStorageAccessHandle::getQueryCatalogHandle() {
    return {&*queryCatalogService, UnlockDeleter()};
}

GlobalQueryPlanHandle SerialStorageAccessHandle::getGlobalQueryPlanHandle() {
    return {&*globalQueryPlan, UnlockDeleter()};
}

SourceCatalogHandle SerialStorageAccessHandle::getSourceCatalogHandle() {
    return {&*sourceCatalog, UnlockDeleter()};
}
UdfCatalogHandle SerialStorageAccessHandle::getUdfCatalogHandle() {
    return {&*udfCatalog, UnlockDeleter()};
}
}

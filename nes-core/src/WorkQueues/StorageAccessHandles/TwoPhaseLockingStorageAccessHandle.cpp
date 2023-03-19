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

#include <WorkQueues/StorageAccessHandles/TwoPhaseLockingStorageAccessHandle.hpp>

namespace NES {

TwoPhaseLockingStorageAccessHandle::TwoPhaseLockingStorageAccessHandle(GlobalExecutionPlanPtr globalExecutionPlan,
                                                                       TopologyPtr topology,
                                                                       QueryCatalogServicePtr queryCatalogService,
                                                                       GlobalQueryPlanPtr globalQueryPlan,
                                                                       Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                                       Catalogs::UDF::UdfCatalogPtr udfCatalog)
    : StorageAccessHandle(globalExecutionPlan, topology, queryCatalogService, globalQueryPlan, sourceCatalog, udfCatalog) {}

std::shared_ptr<TwoPhaseLockingStorageAccessHandle> TwoPhaseLockingStorageAccessHandle::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                                       TopologyPtr topology,
                                                                       QueryCatalogServicePtr queryCatalogService,
                                                                       GlobalQueryPlanPtr globalQueryPlan,
                                                                       Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                                       Catalogs::UDF::UdfCatalogPtr udfCatalog) {
    return std::make_shared<TwoPhaseLockingStorageAccessHandle>(globalExecutionPlan, topology, queryCatalogService, globalQueryPlan, sourceCatalog, udfCatalog);
}

bool TwoPhaseLockingStorageAccessHandle::requiresRollback() {
    return false;
}

TopologyHandle TwoPhaseLockingStorageAccessHandle::getTopologyHandle() {
    return TopologyHandle(&*topology, UnlockDeleter(topologyMutex, std::try_to_lock));
}

QueryCatalogServiceHandle TwoPhaseLockingStorageAccessHandle::getQueryCatalogHandle() {
    return QueryCatalogServiceHandle(&*queryCatalogService, UnlockDeleter(queryCatalogMutex, std::try_to_lock));
}

SourceCatalogHandle TwoPhaseLockingStorageAccessHandle::getSourceCatalogHandle() {
    return NES::SourceCatalogHandle(&*sourceCatalog, UnlockDeleter(sourceCatalogMutex, std::try_to_lock));
}

GlobalExecutionPlanHandle TwoPhaseLockingStorageAccessHandle::getGlobalExecutionPlanHandle() {
    return NES::GlobalExecutionPlanHandle(&*globalExecutionPlan, UnlockDeleter(globalExecutionPlanMutex, std::try_to_lock));
}

GlobalQueryPlanHandle TwoPhaseLockingStorageAccessHandle::getGlobalQueryPlanHandle() {
    return NES::GlobalQueryPlanHandle(&*globalQueryPlan, UnlockDeleter(globalQueryPlanMutex, std::try_to_lock));
}

UdfCatalogHandle TwoPhaseLockingStorageAccessHandle::getUdfCatalogHandle() {
    return NES::UdfCatalogHandle(&*udfCatalog, UnlockDeleter(udfCatalogMutex, std::try_to_lock));
}
}// namespace NES

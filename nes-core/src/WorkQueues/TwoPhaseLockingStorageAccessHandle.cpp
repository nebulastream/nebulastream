#include <WorkQueues/TwoPhaseLockingStorageAccessHandle.hpp>
#include <utility>

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

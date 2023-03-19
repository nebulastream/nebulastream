#include <WorkQueues/TwoPhaseLockingStorageAccessHandle.hpp>

namespace NES {

TwoPhaseLockingStorageAccessHandle::TwoPhaseLockingStorageAccessHandle(NES::TopologyPtr topology)
    : StorageAccessHandle(topology) {}

std::shared_ptr<TwoPhaseLockingStorageAccessHandle> TwoPhaseLockingStorageAccessHandle::create(TopologyPtr topology) {
    return std::make_shared<TwoPhaseLockingStorageAccessHandle>(topology);
}

bool TwoPhaseLockingStorageAccessHandle::requiresRollback() {
    return false;
}

TopologyHandle TwoPhaseLockingStorageAccessHandle::getTopologyHandle() {
    return TopologyHandle(&*topology, UnlockDeleter(topologyMutex, std::try_to_lock));
}

QueryCatalogHandle TwoPhaseLockingStorageAccessHandle::getQueryCatalogHandle() {
    return QueryCatalogHandle(&*queryCatalog, UnlockDeleter(queryCatalogMutex, std::try_to_lock));
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
}// namespace NES

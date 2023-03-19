#ifndef NES_TWOPHASELOCKINGSTORAGEACCESSHANDLE_HPP
#define NES_TWOPHASELOCKINGSTORAGEACCESSHANDLE_HPP
#include <WorkQueues/StorageAccessHandle.hpp>

namespace NES {

/**
 * Resource handles created by this class ensure that the resource is locked until the handle goes out of scope
 */
class TwoPhaseLockingStorageAccessHandle : public StorageAccessHandle {
  public:
    TwoPhaseLockingStorageAccessHandle(TopologyPtr topology);

    static std::shared_ptr<TwoPhaseLockingStorageAccessHandle> create(TopologyPtr topology);

    /**
     * Indicates that the storage handle requires a rollback because we perform serial operations on the actual data.
     * @return always true
     */
    bool requiresRollback() override;

    /**
     * Obtain a mutable topology handle. Throws an exception if the lock could not be acquired
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle() override;

    /**
     * Obtain a mutable query catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the query catalog.
     */
    QueryCatalogHandle getQueryCatalogHandle() override;

    /**
     * Obtain a mutable source catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the source catalog.
     */
    SourceCatalogHandle getSourceCatalogHandle() override;

    /**
     * Obtain a mutable global execution plan handle. Throws an exception if the lock could not be acquired
     * @return a handle to the global execution plan.
     */
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle() override;

    /**
     * Obtain a mutable global query plan handle. Throws an exception if the lock could not be acquired
     * @return a handle to the global query plan.
     */
    GlobalQueryPlanHandle getGlobalQueryPlanHandle() override;

  private:
    //todo: keep the mutexes here or in the class of the resource itself??
    std::mutex topologyMutex;
    std::mutex queryCatalogMutex;
    std::mutex sourceCatalogMutex;
    std::mutex globalExecutionPlanMutex;
    std::mutex globalQueryPlanMutex;
};
}
#endif//NES_TWOPHASELOCKINGSTORAGEACCESSHANDLE_HPP

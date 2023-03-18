#ifndef NES_STORAGEACCESSHANDLE_HPP
#define NES_STORAGEACCESSHANDLE_HPP

#include <memory>
class Topology;
class ResourceHandle;

//For serial access we just hand out the pointers
using TopologyHandle = ResourceHandle<Topology>;
/*
 * for 2pl we can let this class use this solution for pairing locks to pointers:
 * https://stackoverflow.com/questions/23610561/return-locked-resource-from-class-with-automatic-unlocking#comment36245473_23610561
 */
//todo: either make the handle a class and subclass it or use ifdefs bc we know at compile time which manager to use

//for other strategies like mvcc we can use a factory to generate a handle for each transaction
class StorageAccessHandle {
    /**
     * indicates if the storage handle requieres a rollback in case of an aborted operation
     * @return
     */
    virtual bool requiresRollback();

    //for out serial implementation the TopologyHandle is just a share ptr to the topology. for other ones it needs to also lock the mutex for its full lifetime
    /**
     * obtain a handle to a mutable topology. Throw exception if lock could not be acquired
     * @return
     */
    virtual TopologyHandle getTopology();

    virtual const TopologyHandle getConstTopology();

    /*
     * more getters for all the other resources
     */
};
using StorageAccessHandlePtr = std::shared_ptr<StorageAccessHandle>;

#endif//NES_STORAGEACCESSHANDLE_HPP

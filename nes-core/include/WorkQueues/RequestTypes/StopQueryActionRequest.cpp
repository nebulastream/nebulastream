#include "StopQueryActionRequest.hpp"
#include <WorkQueues/RequestTypes/StorageAccessHandle.hpp>

namespace NES {
bool StopQueryActionRequest::execute(StorageAccessHandlePtr storageManagerHandle) {
    //this will throw an exception if the resource could not be acquired which we propagate upwards
    auto queryCatalogServiceHandle = storageManagerHandle->getQueryCatalogService();

    //get query status

    //todo: use a switch statement for this
    if (Registered || Optimizing || Deployed || Running || Restarting ||
        Migrating || MarkedForSoftStop || SoftStopTriggered || SoftStopCompleted) {

        //set query status marked for hard stop

        auto globalQueryPlanHandle = storageManagerHandle->getGlobalQueryPlan();

        //remove query from global queryplan


        if (succesfullyRemoved) {
            //we do not change the topology so we can get a const handle
            auto topologyHandle = storageManagerHandle->getConstTopology();

            //
            //release handle
            globalQueryPlanHandle.reset();

            //execute undeployment phase (todo: change topology and gep to be parameters of execute instead of the constructor)

            //set query status stopped

        } else {
            //throw exception on error
        }

        return;
    }

    if (MarkedForHardStop || Stopped) {
        return;
    }

    //todo: move this to error method
    if (Error) {
        // set marked for failure

        //todo: do we want this action to happen as an extra request?
        followUpRequests.push_back(/* new fail request*/ );
        queueFollowingRequests(queue);
    }

    }


}
}
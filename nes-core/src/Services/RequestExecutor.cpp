#include "Services/RequestExecutor.hpp"
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
#include <exception>
#include <memory>
#include <vector>

namespace NES {

void RequestExecutor::run() {

    //todo: get this from request queue
    std::vector<std::shared_ptr<AbstractRequest>> requests;

    for (auto request : requests) {
        try {

            request->execute( storageAccessHandle);
        } catch (std::exception& ex) {
            request->handleError(ex, storageAccessHandle);
            if (request->retry()) {
                //queue again
            }
        }
    }
}
}

#ifndef NES_REQUESTEXECUTOR_HPP
#define NES_REQUESTEXECUTOR_HPP
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
#include <memory>

namespace NES {
class StorageAccessHandle;
using StorageAccessHandlePtr = std::shared_ptr<StorageAccessHandle>;
class RequestQueue;
class RequestQueuePtr = std::shared_ptr<RequestQueue>;

/**
 * serially executes requests
 */
class RequestExecutor {
  public:
    /**
     * @brief run serial request execution
     */
    void run();
  private:

    StorageAccessHandlePtr storageAccessHandle;
    RequestQueuePtr requestQueue;
};

}



#endif//NES_REQUESTEXECUTOR_HPP

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_CLEARQUERYBENCHMARKREQUEST_HPP
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_CLEARQUERYBENCHMARKREQUEST_HPP

#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>

namespace NES {

namespace RequestProcessor {

struct ClearQueryBenchmarkResponse : public AbstractRequestResponse {
    explicit ClearQueryBenchmarkResponse(const bool success) : success(success){};
    bool success;
};

class ClearQueryBenchmarkRequest;
using ClearQueryBenchmarkRequestPtr = std::shared_ptr<ClearQueryBenchmarkRequest>;

class ClearQueryBenchmarkRequest : public AbstractUniRequest {
  public:
    ClearQueryBenchmarkRequest(const uint8_t maxRetries);

    static ClearQueryBenchmarkRequestPtr create(const uint8_t maxRetries);

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandler) override;
};//class ClearQueryBenchmarkRequest
}// namespace RequestProcessor
}// namespace NES
#endif//NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_CLEARQUERYBENCHMARKREQUEST_HPP

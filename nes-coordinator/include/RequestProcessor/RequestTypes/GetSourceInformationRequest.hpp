#ifndef GETSOURCEINFORMATIONREQUEST_HPP
#define GETSOURCEINFORMATIONREQUEST_HPP

#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
#include <nlohmann/json.hpp>
#include <optional>
namespace NES::RequestProcessor {

//an enum to specify the type of source information to get
enum class SourceType {
    LOGICAL_SOURCE,
    PHYSICAL_SOURCE
};

//the response type for the request
struct GetSourceInformationResponse : public AbstractRequestResponse {
    explicit GetSourceInformationResponse(nlohmann::json json) : json(json){};
    nlohmann::json json;
};

class GetSourceInformationRequest;
using GetSourceInformationRequestPtr = std::shared_ptr<GetSourceInformationRequest>;

class GetSourceInformationRequest : public NES::RequestProcessor::AbstractUniRequest {
public:
  static GetSourceInformationRequestPtr create(uint8_t maxRetries);
  static GetSourceInformationRequestPtr create(SourceType sourceType, std::string sourceName, uint8_t maxRetries);
    /**
     * @brief constructor to get all sources
     * @param sourceInformationType type of source information to get
     **/
    GetSourceInformationRequest(uint8_t maxRetries);

    GetSourceInformationRequest(SourceType sourceInformationType, std::string sourceName, uint8_t maxRetries);

    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;
private:
  SourceType sourceInformationType;
  std::optional<std::string> sourceName;
};

}
#endif //GETSOURCEINFORMATIONREQUEST_HPP

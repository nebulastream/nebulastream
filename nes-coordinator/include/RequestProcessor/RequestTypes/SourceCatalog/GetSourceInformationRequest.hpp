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
    nlohmann::json getJson();
private:
    nlohmann::json json;
};

class GetSourceInformationRequest;
using GetSourceInformationRequestPtr = std::shared_ptr<GetSourceInformationRequest>;

/**
 * @brief A request to get information about logical or physical sources in json format
 */
class GetSourceInformationRequest : public NES::RequestProcessor::AbstractUniRequest {
public:
    /**
     * @brief creates a request to get all logical sources
     */
  static GetSourceInformationRequestPtr create(uint8_t maxRetries);

    /**
     * @brief creates a request to get a specific logical source or all physical sources for a logical source
     * @param sourceType: the type of source information to get (logical of physical)
     * @param sourceName: the name of the source to get information for
     * @param maxRetries: the maximum number of retries to attempt
     */
  static GetSourceInformationRequestPtr create(SourceType sourceType, std::string sourceName, uint8_t maxRetries);
    /**
     * @brief constructor that creates a request to get all logical sources
     */
    GetSourceInformationRequest(uint8_t maxRetries);

    /**
     * @brief constructor for  a request to get a specific logical source or all physical sources for a logical source
     * @param sourceType: the type of source information to get (logical of physical)
     * @param sourceName: the name of the source to get information for
     * @param maxRetries: the maximum number of retries to attempt
     */
    GetSourceInformationRequest(SourceType sourceType, std::string sourceName, uint8_t maxRetries);

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
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

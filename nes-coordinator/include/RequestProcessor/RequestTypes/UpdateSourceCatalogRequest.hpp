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

#ifndef UPDATESOURCECATALOGREQUEST_HPP
#define UPDATESOURCECATALOGREQUEST_HPP

#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
#include <memory>
#include <optional>
#include <variant>

namespace NES {
namespace RequestProcessor {
class UpdateSourceCatalogRequest;

struct UpdateSourceCatalogResponse : AbstractRequestResponse {
    explicit UpdateSourceCatalogResponse(bool success) : success(success){};
    bool success;
};

using UpdateSourceCatalogRequestPtr = std::shared_ptr<UpdateSourceCatalogRequest>;

//struct containing the definition of a single physical source to add
struct PhysicalSourceAddition {
    std::string logicalSourceName;
    std::string physicalSourceName;
    WorkerId workerId;
};
//struct containing the definition of a single physical source to remove
struct PhysicalSourceRemoval {
    std::string logicalSourceName;
    std::string physicalSourceName;
    WorkerId workeId;
};

//struct containing the definition of a logical source to add
struct LogicalSourceAddition {
    std::string logicalSourceName;
    SchemaPtr schema;
};
//struct containing the definition of a logical source to update
struct LogicalSourceUpdate {
    std::string logicalSourceName;
    SchemaPtr schema;
};
//struct containing the definition of a logical source to remove
struct LogicalSourceRemoval {
    std::string logicalSourceName;
};

using SourceActionVector = std::variant<std::vector<PhysicalSourceAddition>,
                                        std::vector<PhysicalSourceRemoval>,
                                        std::vector<LogicalSourceAddition>,
                                        std::vector<LogicalSourceUpdate>,
                                        std::vector<LogicalSourceRemoval>>;

/**
 * @brief This request allows modifying the source catalog by adding, updating or removing logical and physical sources
 */
class UpdateSourceCatalogRequest : public AbstractUniRequest {
  public:
    /**
     * @brief creates a new request
     * @param sourceActions A vector containing information about the sources to modify and the action to be performed
     * @param maxRetries the maximum number of retries to attempt
     * @return a pointer to the created request
     */
    static UpdateSourceCatalogRequestPtr create(SourceActionVector sourceActions, uint8_t maxRetries);

    /**
     * @brief constructor
     * @param sourceActions A vector containing information about the sources to modify and the action to be performed
     * @param maxRetries the maximum number of retries to attempt
     * @return a pointer to the created request
     */
    UpdateSourceCatalogRequest(SourceActionVector sourceActions, uint8_t maxRetries);

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
    //vector holding the definitions of the sources to be modified
    SourceActionVector sourceActions;
};
}// namespace RequestProcessor
}// namespace NES

#endif//UPDATESOURCECATALOGREQUEST_HPP

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

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_ADDKEYDISTRIBUTIONENTRYEVENT_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_ADDKEYDISTRIBUTIONENTRYEVENT_HPP_

#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/BaseUpdateSourceCatalogEvent.hpp>

namespace NES::RequestProcessor {
class AddKeyDistributionEntryEvent;
using AddKeyDistributionEntryEventPtr = std::shared_ptr<AddKeyDistributionEntryEvent>;

/**
 * @brief the response to the AddKeyDistributionEntry operation
 */
struct AddKeyDistributionEntryResponse : public BaseUpdateSourceCatalogResponse {
    /**
    * @brief Construct a new AddKeyDistributionEntry Response object
    */
    AddKeyDistributionEntryResponse(bool success);
};

/**
 * @brief An event to add key distribution entry
 */
class AddKeyDistributionEntryEvent : public BaseUpdateSourceCatalogEvent {
  public:
    /**
     * @brief Create a new event
     * @param logicalSourceName The name of the logical source to which the physical source belongs
     * @param physicalSourceName physical source name
     * @param workerId The id of the worker hosting the physical source
     * @param value the statistics from request
     * @return a pointer to the new event
     */
    static AddKeyDistributionEntryEventPtr
    create(std::string logicalSourceName, std::string physicalSourceName, WorkerId workerId, std::string value);

    /**
     * @brief Constructor
     * @param logicalSourceName The name of the logical source to which the physical source belongs
     * @param physicalSourceName physical source name
     * @param workerId The id of the worker hosting the physical source
     * @param value the statistics from request
     */
    AddKeyDistributionEntryEvent(std::string logicalSourceName,
                                 std::string physicalSourceName,
                                 WorkerId workerId,
                                 std::string value);

    /**
     * @brief Get the logical source name
     * @return a string representing the logical source name
     */
    std::string getLogicalSourceName() const;

    /**
     * @brief Get the physical source name
     * @return a string representing the physical source name
     */
    std::string getPhysicalSourceName() const;

    /**
     * @brief Get the worker id
     * @return WorkerId
     */
    WorkerId getWorkerId() const;

    /**
     * @brief Get the statistics from request
     * @return the statistics from request
     */
    std::string getValue() const;

  private:
    std::string logicalSourceName;
    std::string physicalSourceName;
    WorkerId workerId;
    std::string value;
};
}// namespace NES::RequestProcessor
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_ADDKEYDISTRIBUTIONENTRYEVENT_HPP_

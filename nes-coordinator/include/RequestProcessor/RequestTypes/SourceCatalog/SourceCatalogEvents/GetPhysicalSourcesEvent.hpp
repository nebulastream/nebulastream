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
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_GETPHYSICALSOURCESEVENT_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_GETPHYSICALSOURCESEVENT_HPP_

#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/BaseGetSourceCatalogEvent.hpp>
#include <memory>

namespace NES::RequestProcessor {
class GetPhysicalSourcesEvent;
using GetPhysicalSourcesEventPtr = std::shared_ptr<GetPhysicalSourcesEvent>;

/**
 * @brief An event to get the physical sources of a logical source
 */
class GetPhysicalSourcesEvent : public BaseGetSourceCatalogEvent {
  public:
    /**
     * @brief Create a new event
     * @param logicalSourceName The logical source to get the physical sources for
     * @return a pointer to the new event
     */
    static GetPhysicalSourcesEventPtr create(std::string logicalSourceName);

    /**
     * @brief constructor
     * @param logicalSourceName The logical source to get the physical sources for
     */
    GetPhysicalSourcesEvent(std::string logicalSourceName);

    /**
     * @brief Get the logical source name
     * @return The logical source name
     */
    std::string getLogicalSourceName() const;

  private:
    std::string logicalSourceName;
};
}// namespace NES::RequestProcessor
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_GETPHYSICALSOURCESEVENT_HPP_

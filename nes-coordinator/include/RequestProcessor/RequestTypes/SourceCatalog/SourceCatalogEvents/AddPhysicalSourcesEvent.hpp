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

#ifndef ADDPHYSICALSOURCESEVENT_HPP
#define ADDPHYSICALSOURCESEVENT_HPP
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/SourceCatalogEvent.hpp>
#include <optional>
namespace NES::RequestProcessor {

struct PhysicalSourceDefinition {
    std::string logicalSourceName;
    std::string physicalSourceName;
    bool operator==(const PhysicalSourceDefinition & other) const;
};
class AddPhysicalSourcesEvent;
using AddPhysicalSourcesEventPtr = std::shared_ptr<AddPhysicalSourcesEvent>;

/**
 * @brief the response to the add physical sources operation
 */
struct AddPhysicalSourcesResponse : public SourceCatalogResponse {
    /**
    * @brief Construct a new Add Physical Sources Response object
    */
    AddPhysicalSourcesResponse(bool success, std::vector<std::string> succesfulAdditions, std::optional<std::string> failed);

    AddPhysicalSourcesResponse(bool success, std::vector<std::string> succesfulAdditions);

    /**
     * @brief get the list of physical sources that were succesfully added
     */
    std::vector<std::string> getSuccesfulAdditions() const;

    /**
     * @brief get
     */
    std::optional<std::string> getFailedAddition() const;

  private:
    std::vector<std::string> succesfulAdditions;
    std::optional<std::string> failed;
};

/**
 * @brief Event to add physical sources to a logical source
 */
class AddPhysicalSourcesEvent : public SourceCatalogEvent {
  public:
    /**
     * @brief Create a new event
     * @param physicalSources The physical sources to add
     * @param workerId the id of the worker hosting the physical source
     * @return a pointer to the new event
     */
    static AddPhysicalSourcesEventPtr create(std::vector<PhysicalSourceDefinition> physicalSources, WorkerId workerId);

    /**
     * @brief constructor
     * @param physicalSources The physical sources to add
     * @param workerId the id of the worker hosting the physical source
     */
    AddPhysicalSourcesEvent(std::vector<PhysicalSourceDefinition> physicalSources, WorkerId workerId);

    /**
     * @brief Get the physical sources to add
     * @return std::vector<PhysicalSourceDefinition>
     */
    std::vector<PhysicalSourceDefinition> getPhysicalSources() const;

    /**
     * @brief Get the worker id
     * @return WorkerId
     */
    WorkerId getWorkerId() const;

  private:
    // physical sources to add
    std::vector<PhysicalSourceDefinition> physicalSources;
    // worker id
    WorkerId workerId;
};

}// namespace NES::RequestProcessor

#endif//ADDPHYSICALSOURCESEVENT_HPP

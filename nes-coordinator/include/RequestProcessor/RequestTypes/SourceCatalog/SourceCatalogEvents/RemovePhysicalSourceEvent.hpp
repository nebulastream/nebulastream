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
#ifndef REMOVEPHYSICALSOURCEEVENT_HPP
#define REMOVEPHYSICALSOURCEEVENT_HPP
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/SourceCatalogEvent.hpp>

namespace NES::RequestProcessor {
class RemovePhysicalSourceEvent;
using RemovePhysicalSourceEventPtr = std::shared_ptr<RemovePhysicalSourceEvent>;

class RemovePhysicalSourceEvent : public SourceCatalogEvent {
  public:
    /**
     * @brief Create a new event
     * @param logicalSourceName The name of the logical source to which the physical source belongs
     * @param physicalSourceName The name of the physical source to remove
     * @param workerId The id of the worker hosting the physical source
     * @return a pointer to the new event
     */
   static RemovePhysicalSourceEventPtr create(std::string logicalSourceName, std::string physicalSourceName, WorkerId workerId);

    /**
     * @brief Constructor
     * @param logicalSourceName The name of the logical source to which the physical source belongs
     * @param physicalSourceName The name of the physical source to remove
     * @param workerId The id of the worker hosting the physical source
     */
    RemovePhysicalSourceEvent(std::string logicalSourceName, std::string physicalSourceName, WorkerId workerId);

    /**
     * @brief Get the worker id
     * @return WorkerId
     */
    WorkerId getWorkerId() const;

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

  private:
    std::string logicalSourceName;
    std::string physicalSourceName;
    WorkerId workerId;
};

}// namespace NES::RequestProcessor

#endif//REMOVEPHYSICALSOURCEEVENT_HPP

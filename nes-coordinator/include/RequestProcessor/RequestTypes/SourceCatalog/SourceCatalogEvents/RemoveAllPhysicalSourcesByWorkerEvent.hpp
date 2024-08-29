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
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_REMOVEALLPHYSICALSOURCESBYWORKEREVENT_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_REMOVEALLPHYSICALSOURCESBYWORKEREVENT_HPP_

#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/BaseUpdateSourceCatalogEvent.hpp>

namespace NES::RequestProcessor {
class RemoveAllPhysicalSourcesByWorkerEvent;
using RemoveAllPhysicalSourcesByWorkerEventPtr = std::shared_ptr<RemoveAllPhysicalSourcesByWorkerEvent>;

/**
 * @brief An event to remove all physical sources of one Worker from the source catalog
 */
class RemoveAllPhysicalSourcesByWorkerEvent : public BaseUpdateSourceCatalogEvent {
  public:
    /**
     * @brief Create a new event
     * @param workerId The id of the worker hosting the physical source
     * @return a pointer to the new event
     */
    static RemoveAllPhysicalSourcesByWorkerEventPtr create(WorkerId workerId);

    /**
     * @brief Constructor
     * @param workerId The id of the worker hosting the physical source
     */
    RemoveAllPhysicalSourcesByWorkerEvent(WorkerId workerId);

    /**
     * @brief Get the worker id
     * @return WorkerId
     */
    WorkerId getWorkerId() const;

  private:
    WorkerId workerId;
};

}// namespace NES::RequestProcessor
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_REMOVEALLPHYSICALSOURCESBYWORKEREVENT_HPP_

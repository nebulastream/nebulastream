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
#ifndef GETALLLOGICALSOURCESEVENT_HPP
#define GETALLLOGICALSOURCESEVENT_HPP
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetSourceInformationEvent.hpp>
#include <memory>


namespace NES::RequestProcessor {
class GetAllLogicalSourcesEvent;
using GetAllLogicalSourcesEventPtr = std::shared_ptr<GetAllLogicalSourcesEvent>;

/**
 * @brief Event to get all logical sources from the source catalog.
 */
class GetAllLogicalSourcesEvent : public GetSourceInformationEvent {
public:
  /**
   * @brief Constructor
   */
  GetAllLogicalSourcesEvent();

  /**
  * @brief Create a new event
  */
    static GetAllLogicalSourcesEventPtr create();
};
}



#endif //GETALLLOGICALSOURCESEVENT_HPP

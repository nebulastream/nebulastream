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

#ifndef GETSCHEMAEVENT_HPP
#define GETSCHEMAEVENT_HPP
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSink.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetSourceInformationEvent.hpp>

namespace NES::RequestProcessor {
class GetSchemaEvent;
using GetSchemaEventPtr = std::shared_ptr<GetSchemaEvent>;

/**
 * @brief a response containg the schema for a logical source
 */
struct GetSchemaResponse : public GetSourceInformationResponse {
    /**
     * @brief Constructor
     * @param schema the schema for the logical source
     */
    GetSchemaResponse(bool success, SchemaPtr schema);

    /**
     * @brief Get the schema for the logical source
     * @return The schema for the logical source
     */
    SchemaPtr getSchema();

  private:
    SchemaPtr schema;
};

/**
 * @brief get the schema for a logical source in the source catalog
 */
class GetSchemaEvent : public GetSourceInformationEvent {
  public:
   /**
    * @brief Constructor
    * @param logicalSourceName the name of the logical source
    */
    GetSchemaEvent(std::string logicalSourceName);

   /**
    * @brief create new event
    * @param logicalSourceName the name of the logical source
    */
    static GetSchemaEventPtr create(std::string logicalSourceName);

    /**
     * @brief Get the logical source name
     * @return The logical source name
     */
    std::string getLogicalSourceName() const;

  private:
    std::string logicalSourceName;
};
}

#endif//GETSCHEMAEVENT_HPP

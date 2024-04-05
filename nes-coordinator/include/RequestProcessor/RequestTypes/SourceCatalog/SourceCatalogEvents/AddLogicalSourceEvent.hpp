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

#ifndef ADDLOGICALSOURCEEVENT_HPP
#define ADDLOGICALSOURCEEVENT_HPP
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSink.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/SourceCatalogEvent.hpp>

namespace NES::RequestProcessor {
class AddLogicalSourceEvent;
using AddLogicalSourceEventPtr = std::shared_ptr<AddLogicalSourceEvent>;

/**
 * @brief An event to add a logical source to the source catalog
 */
class AddLogicalSourceEvent : public SourceCatalogEvent {
  public:
    /**
     * @brief Create a new event
     * @param logicalSourceName the name of the logical source to add
     * @param schema the schema of the logical source
     * @return a pointer to the new event
     */
    static AddLogicalSourceEventPtr create(std::string logicalSourceName, SchemaPtr schema);

    /**
     * @brief Consstructor
     * @param logicalSourceName the name of the logical source to add
     * @param schema the schema of the logical source
     */
    AddLogicalSourceEvent(const std::string& logicalSourceName, SchemaPtr schema);

    /**
     * @brief Get the logical source name
     * @return a string representing the logical source name
     */
    std::string getLogicalSourceName() const;

    /**
     * @brief Get the schema of the logical source
     * @return a pointer to the schema
     */
    SchemaPtr getSchema() const;

  private:
    std::string logicalSourceName;
    SchemaPtr schema;
};
}// namespace NES::RequestProcessor

#endif//ADDLOGICALSOURCEEVENT_HPP

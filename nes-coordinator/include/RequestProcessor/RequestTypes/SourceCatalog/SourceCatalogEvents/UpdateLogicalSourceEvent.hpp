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

#ifndef UPDATELOGICALSOURCEEVENT_HPP
#define UPDATELOGICALSOURCEEVENT_HPP
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSink.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/SourceCatalogEvent.hpp>

namespace NES::RequestProcessor {
class UpdateLogicalSourceEvent;
using UpdateLogicalSourceEventPtr = std::shared_ptr<UpdateLogicalSourceEvent>;
class UpdateLogicalSourceEvent : public SourceCatalogEvent {
public:
    /**
     * @brief Create a new event
     * @param logicalSourceName the name of the logical soruce to be updated
     * @param schema the updated schema
     * @return a pointer to the new event
     */
    static UpdateLogicalSourceEventPtr create(std::string logicalSourceName, SchemaPtr schema);

    /**
     * @brief Construct a new Update Logical Source Event object
     * @param logicalSourceName the name of the logical soruce to be updated
     * @param schema the updated schema
     */
    UpdateLogicalSourceEvent(std::string logicalSourceName, SchemaPtr schema);

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

#endif//UPDATELOGICALSOURCEEVENT_HPP

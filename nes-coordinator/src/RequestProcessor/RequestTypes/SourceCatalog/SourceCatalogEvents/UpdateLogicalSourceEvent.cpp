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
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/UpdateLogicalSourceEvent.hpp>
namespace NES::RequestProcessor {
UpdateLogicalSourceEventPtr UpdateLogicalSourceEvent::create(std::string logicalSourceName, SchemaPtr schema) {
    return std::make_shared<UpdateLogicalSourceEvent>(logicalSourceName, schema);
}

UpdateLogicalSourceEvent::UpdateLogicalSourceEvent(std::string logicalSourceName, SchemaPtr schema) : logicalSourceName(logicalSourceName), schema(schema) {}

std::string UpdateLogicalSourceEvent::getLogicalSourceName() const {
    return logicalSourceName;
}

SchemaPtr UpdateLogicalSourceEvent::getSchema() const {
    return schema;
}
}// namespace NES::RequestProcessor
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

#include <Configurations/Coordinator/SchemaType.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Configurations {

SchemaType::SchemaType(std::vector<SchemaFieldDetail> schemaFieldDetails) : schemaFieldDetails(std::move(schemaFieldDetails)) {}

SchemaTypePtr SchemaType::create(const std::vector<SchemaFieldDetail>& schemaFieldDetails) {
    return std::make_shared<SchemaType>(SchemaType(schemaFieldDetails));
}

const std::vector<SchemaFieldDetail>& SchemaType::getSchemaFieldDetails() const { return schemaFieldDetails; }

bool SchemaType::contains(const std::string& fieldName) {
    for (const auto& schemaFieldDetail : this->schemaFieldDetails) {
        NES_TRACE("contain compare field={} with other={}", schemaFieldDetail.fieldName, fieldName);
        if (schemaFieldDetail.fieldName == fieldName) {
            return true;
        }
    }
    return false;
}

}// namespace NES::Configurations
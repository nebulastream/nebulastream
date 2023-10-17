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

#include <Configurations/Coordinator/LogicalSourceType.hpp>
#include <utility>

namespace NES::Configurations {

LogicalSourceType::LogicalSourceType(std::string logicalSourceName, NES::Configurations::SchemaTypePtr schemaType)
    : logicalSourceName(std::move(logicalSourceName)), schemaType(std::move(schemaType)) {}

LogicalSourceTypePtr LogicalSourceType::create(std::string logicalSourceName, NES::Configurations::SchemaTypePtr schemaType) {
    return std::make_shared<LogicalSourceType>(LogicalSourceType(std::move(logicalSourceName), std::move(schemaType)));
}

const std::string& LogicalSourceType::getLogicalSourceName() const { return logicalSourceName; }

const SchemaTypePtr& LogicalSourceType::getSchemaType() const { return schemaType; }
}// namespace NES::Configurations
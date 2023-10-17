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

#ifndef NES_LOGICALSOURCETYPE_HPP
#define NES_LOGICALSOURCETYPE_HPP

#include <memory>
#include <string>

namespace NES::Configurations {

class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;

class LogicalSourceType;
using LogicalSourceTypePtr = std::shared_ptr<LogicalSourceType>;

class LogicalSourceType {

  public:
    static LogicalSourceTypePtr create(std::string logicalSourceName, SchemaTypePtr schemaType);

    const std::string& getLogicalSourceName() const;

    const SchemaTypePtr& getSchemaType() const;

  private:
    LogicalSourceType(std::string logicalSourceName, SchemaTypePtr schemaType);

    std::string logicalSourceName;
    SchemaTypePtr schemaType;
};

}// namespace NES::Configurations

#endif//NES_LOGICALSOURCETYPE_HPP

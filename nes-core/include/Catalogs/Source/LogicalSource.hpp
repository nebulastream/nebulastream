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

#ifndef NES_INCLUDE_CATALOGS_SOURCE_LOGICALSOURCE_HPP_
#define NES_INCLUDE_CATALOGS_SOURCE_LOGICALSOURCE_HPP_

#include <iostream>
#include <memory>
#include <string>

namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

/**
 * @brief The LogicalSource wraps the stream name and the schema.
 */
class LogicalSource {
  public:

    static LogicalSourcePtr create(const std::string& logicalSourceName, const SchemaPtr& schema);

    /**
     * @brief Gets the logical source name
     */
    std::string getLogicalSourceName();

    /**
     * @brief Gets the schema
     */
    SchemaPtr getSchema();

  private:
    LogicalSource(const std::string& logicalSourceName, const SchemaPtr& schema);

    std::string logicalSourceName;
    SchemaPtr schema;
};
}// namespace NES
#endif  // NES_INCLUDE_CATALOGS_SOURCE_LOGICALSOURCE_HPP_

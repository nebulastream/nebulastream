/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_CATALOGS_LOGICAL_STREAM_HPP_
#define NES_INCLUDE_CATALOGS_LOGICAL_STREAM_HPP_

#include <iostream>
#include <memory>
#include <string>

namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

/**
 * @brief The LogicalStream wraps the stream name and the schema.
 */
class LogicalStream {
  public:
    LogicalStream(std::string name, const SchemaPtr& schema);

    /**
     * @brief Gets the stream name
     */
    std::string getName();

    /**
     * @brief Gets the stream schema
     */
    SchemaPtr getSchema();

  private:
    std::string name;
    SchemaPtr schema;
};

using LogicalStreamPtr = std::shared_ptr<LogicalStream>;

}// namespace NES
#endif  // NES_INCLUDE_CATALOGS_LOGICAL_STREAM_HPP_

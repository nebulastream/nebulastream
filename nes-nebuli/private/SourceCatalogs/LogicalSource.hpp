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

#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <API/Schema.hpp>
namespace NES
{

class Schema;
class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

/// @brief The LogicalSource wraps the source name and the schema.
class LogicalSource
{
public:
    static std::shared_ptr<LogicalSource> create(const std::string& logicalSourceName, const Schema& schema);

    [[nodiscard]] std::string getLogicalSourceName();
    [[nodiscard]] Schema getSchema();

private:
    LogicalSource(std::string logicalSourceName, Schema schema);

    std::string logicalSourceName;
    Schema schema;
};
}

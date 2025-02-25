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

#include <utility>
#include <API/Schema.hpp>
#include <SourceCatalogs/LogicalSource.hpp>

namespace NES
{

LogicalSourcePtr LogicalSource::create(const std::string& logicalSourceName, const SchemaPtr& schema)
{
    return std::make_shared<LogicalSource>(LogicalSource(std::move(logicalSourceName), std::move(schema)));
}

LogicalSource::LogicalSource(const std::string& logicalSourceName, const SchemaPtr& schemaPtr) : logicalSourceName(logicalSourceName)
{
    schema = schemaPtr->copy();
}

SchemaPtr LogicalSource::getSchema()
{
    return schema;
}

std::string LogicalSource::getLogicalSourceName()
{
    return logicalSourceName;
}

}

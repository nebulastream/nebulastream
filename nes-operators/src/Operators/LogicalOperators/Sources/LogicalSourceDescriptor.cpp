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
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>

namespace NES
{

LogicalSourceDescriptor::LogicalSourceDescriptor(std::string logicalSourceName, std::string sourceName)
    : SourceDescriptor(Schema::create(), std::move(logicalSourceName), std::move(sourceName))
{
}

std::unique_ptr<SourceDescriptor> LogicalSourceDescriptor::create(std::string logicalSourceName, std::string sourceName)
{
    return std::make_unique<LogicalSourceDescriptor>(LogicalSourceDescriptor(std::move(logicalSourceName), std::move(sourceName)));
}

bool LogicalSourceDescriptor::equal(SourceDescriptor& other) const
{
    if (!dynamic_cast<LogicalSourceDescriptor*>(&other))
    {
        return false;
    }
    const auto otherLogicalSourceName = dynamic_cast<LogicalSourceDescriptor*>(&other)->getLogicalSourceName();
    return getLogicalSourceName() == otherLogicalSourceName;
}

std::string LogicalSourceDescriptor::toString() const
{
    return "LogicalSourceDescriptor(" + getLogicalSourceName() + ")";
}

} /// namespace NES

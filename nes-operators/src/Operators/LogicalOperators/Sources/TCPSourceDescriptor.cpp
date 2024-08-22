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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>

namespace NES
{

TCPSourceDescriptor::TCPSourceDescriptor(SchemaPtr schema, TCPSourceTypePtr tcpSourceType, const std::string& logicalSourceName)
    : SourceDescriptor(std::move(schema), logicalSourceName, PLUGIN_NAME_TCP), tcpSourceType(std::move(tcpSourceType))
{
}

std::unique_ptr<SourceDescriptor>
TCPSourceDescriptor::create(SchemaPtr schema, TCPSourceTypePtr sourceConfig, const std::string& logicalSourceName)
{
    return std::make_unique<TCPSourceDescriptor>(TCPSourceDescriptor(std::move(schema), std::move(sourceConfig), logicalSourceName));
}

std::unique_ptr<SourceDescriptor> TCPSourceDescriptor::create(SchemaPtr schema, TCPSourceTypePtr sourceConfig)
{
    return std::make_unique<TCPSourceDescriptor>(TCPSourceDescriptor(std::move(schema), std::move(sourceConfig), ""));
}

TCPSourceTypePtr TCPSourceDescriptor::getSourceConfig() const
{
    return tcpSourceType;
}

std::string TCPSourceDescriptor::toString() const
{
    return "TCPSourceDescriptor(" + tcpSourceType->toString() + ")";
}

bool TCPSourceDescriptor::equal(SourceDescriptor& other) const
{
    if (!dynamic_cast<TCPSourceDescriptor*>(&other))
    {
        return false;
    }
    const auto otherLogicalSourceName = dynamic_cast<TCPSourceDescriptor*>(&other)->getLogicalSourceName();
    return getLogicalSourceName() == otherLogicalSourceName;
}

} /// namespace NES
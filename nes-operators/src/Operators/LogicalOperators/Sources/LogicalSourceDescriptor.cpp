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

SourceDescriptorPtr LogicalSourceDescriptor::create(std::string logicalSourceName, std::string sourceName)
{
    return std::make_shared<LogicalSourceDescriptor>(LogicalSourceDescriptor(std::move(logicalSourceName), std::move(sourceName)));
}

bool LogicalSourceDescriptor::equal(SourceDescriptorPtr const& other) const
{
    if (!other->instanceOf<LogicalSourceDescriptor>())
    {
        return false;
    }
    auto otherLogicalSource = other->as<LogicalSourceDescriptor>();
    return getLogicalSourceName() == otherLogicalSource->getLogicalSourceName();
}

std::string LogicalSourceDescriptor::toString() const
{
    return "LogicalSourceDescriptor(" + getLogicalSourceName() + ")";
}

SourceDescriptorPtr LogicalSourceDescriptor::copy()
{
    auto copy = LogicalSourceDescriptor::create(getLogicalSourceName(), getSourceName());
    copy->setSchema(getSchema()->copy());
    return copy;
}

} /// namespace NES

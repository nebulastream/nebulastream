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
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <utility>
namespace NES {

LogicalSourceDescriptor::LogicalSourceDescriptor(std::string logicalSourceName)
    : SourceDescriptor(Schema::create(), std::move(logicalSourceName)) {}

SourceDescriptorPtr LogicalSourceDescriptor::create(std::string logicalSourceName) {
    return std::make_shared<LogicalSourceDescriptor>(LogicalSourceDescriptor(std::move(logicalSourceName)));
}

bool LogicalSourceDescriptor::equal(SourceDescriptorPtr const& other) const {
    if (!other->instanceOf<LogicalSourceDescriptor>()) {
        return false;
    }
    auto otherLogicalSource = other->as<LogicalSourceDescriptor>();
    return getLogicalSourceName() == otherLogicalSource->getLogicalSourceName();
}

std::string LogicalSourceDescriptor::toString() const {
    return "LogicalSourceDescriptor(" + logicalSourceName + ", " + physicalSourceName + ")";
}

SourceDescriptorPtr LogicalSourceDescriptor::copy() {
    auto copy = LogicalSourceDescriptor::create(logicalSourceName);
    copy->setPhysicalSourceName(physicalSourceName);
    copy->setSchema(schema->copy());
    return copy;
}

}// namespace NES
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

namespace NES {

TCPSourceDescriptor::TCPSourceDescriptor(SchemaPtr schema,
                                         TCPSourceTypePtr tcpSourceType,
                                         const std::string& logicalSourceName,
                                         const std::string& physicalSourceName)
    : SourceDescriptor(std::move(schema), logicalSourceName, physicalSourceName), tcpSourceType(std::move(tcpSourceType)) {}

SourceDescriptorPtr TCPSourceDescriptor::create(SchemaPtr schema,
                                                TCPSourceTypePtr sourceConfig,
                                                const std::string& logicalSourceName,
                                                const std::string& physicalSourceName) {
    return std::make_shared<TCPSourceDescriptor>(
        TCPSourceDescriptor(std::move(schema), std::move(sourceConfig), logicalSourceName, physicalSourceName));
}

SourceDescriptorPtr TCPSourceDescriptor::create(SchemaPtr schema, TCPSourceTypePtr sourceConfig) {
    return std::make_shared<TCPSourceDescriptor>(TCPSourceDescriptor(std::move(schema), std::move(sourceConfig), "", ""));
}

TCPSourceTypePtr TCPSourceDescriptor::getSourceConfig() const { return tcpSourceType; }

std::string TCPSourceDescriptor::toString() const { return "TCPSourceDescriptor(" + tcpSourceType->toString() + ")"; }

bool TCPSourceDescriptor::equal(SourceDescriptorPtr const& other) const {

    if (!other->instanceOf<TCPSourceDescriptor>()) {
        return false;
    }
    auto otherTCPSource = other->as<TCPSourceDescriptor>();
    return tcpSourceType->equal(otherTCPSource->tcpSourceType);
}

SourceDescriptorPtr TCPSourceDescriptor::copy() {
    auto copy = TCPSourceDescriptor::create(schema->copy(), tcpSourceType);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}

}// namespace NES

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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorPlugin.hpp>
#include <Sources/Arrow/ArrowSourceDescriptor.hpp>

namespace NES {

ArrowSourceDescriptor::ArrowSourceDescriptor(SchemaPtr schema,
                                             ArrowSourceTypePtr sourceConfig,
                                             std::string logicalSourceName,
                                             std::string physicalSourceName)
    : SourceDescriptor(std::move(schema), logicalSourceName, physicalSourceName), arrowSourceType(std::move(sourceConfig)) {}

SourceDescriptorPtr ArrowSourceDescriptor::create(SchemaPtr schema,
                                                  ArrowSourceTypePtr arrowSourceType,
                                                  const std::string logicalSourceName,
                                                  const std::string physicalSourceName) {
    return std::make_shared<ArrowSourceDescriptor>(
        ArrowSourceDescriptor(std::move(schema), std::move(arrowSourceType), logicalSourceName, physicalSourceName));
}

SourceDescriptorPtr ArrowSourceDescriptor::create(SchemaPtr schema, ArrowSourceTypePtr arrowSourceType) {
    return std::make_shared<ArrowSourceDescriptor>(ArrowSourceDescriptor(std::move(schema), std::move(arrowSourceType), "", ""));
}

ArrowSourceTypePtr ArrowSourceDescriptor::getSourceConfig() const { return arrowSourceType; }

bool ArrowSourceDescriptor::equal(SourceDescriptorPtr const& other) const {
    if (!other->instanceOf<ArrowSourceDescriptor>()) {
        return false;
    }
    auto otherSource = other->as<ArrowSourceDescriptor>();
    return arrowSourceType->equal(otherSource->getSourceConfig());
}

std::string ArrowSourceDescriptor::toString() const { return "ArrowSourceDescriptor(" + arrowSourceType->toString() + ")"; }

SourceDescriptorPtr ArrowSourceDescriptor::copy() {
    auto copy = ArrowSourceDescriptor::create(schema->copy(), arrowSourceType, logicalSourceName, physicalSourceName);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}

class ArrowSourceDescriptorPlugin : public SourceDescriptorPlugin {
  public:
    SourceDescriptorPtr create(SchemaPtr schema, PhysicalSourcePtr physicalSource) override {
        if (physicalSource->getPhysicalSourceType()->getSourceType() != SourceType::ARROW_SOURCE) {
            return nullptr;
        }
        auto physicalSourceType = physicalSource->getPhysicalSourceType();
        auto arrowSourceType = physicalSourceType->as<ArrowSourceType>();
        return ArrowSourceDescriptor::create(schema, arrowSourceType);
    }
};

// Register source descriptor plugin
[[maybe_unused]] static SourceDescriptorPluginRegistry ::Add<ArrowSourceDescriptorPlugin> arrowSourceDescriptorPlugin;

}// namespace NES

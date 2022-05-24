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
#include <Operators/LogicalOperators/Sources/TensorSourceDescriptor.hpp>

namespace NES {

TensorSourceDescriptor::TensorSourceDescriptor(SchemaPtr schema, TensorSourceTypePtr sourceConfig)
    : SourceDescriptor(std::move(schema)), tensorSourceType(std::move(sourceConfig)) {}

SourceDescriptorPtr TensorSourceDescriptor::create(SchemaPtr schema, TensorSourceTypePtr csvSourceType) {
    return std::make_shared<TensorSourceDescriptor>(TensorSourceDescriptor(std::move(schema), std::move(csvSourceType)));
}

TensorSourceTypePtr TensorSourceDescriptor::getSourceConfig() const { return tensorSourceType; }

bool TensorSourceDescriptor::equal(SourceDescriptorPtr const& other) {
    if (!other->instanceOf<TensorSourceDescriptor>()) {
        return false;
    }
    auto otherSource = other->as<TensorSourceDescriptor>();
    return tensorSourceType->equal(otherSource->getSourceConfig());
}

std::string TensorSourceDescriptor::toString() { return "TensorSourceDescriptor(" + tensorSourceType->toString() + ")"; }

SourceDescriptorPtr TensorSourceDescriptor::copy() {
    auto copy = TensorSourceDescriptor::create(schema->copy(), tensorSourceType);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}

}// namespace NES

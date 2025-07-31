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

#include <SourceDescriptorPhysicalOperator.hpp>

#include <memory>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

SourceDescriptorPhysicalOperator::SourceDescriptorPhysicalOperator(SourceDescriptor descriptor, OriginId id)
    : originId(id), descriptor(std::move(descriptor)) { };

SourceDescriptor SourceDescriptorPhysicalOperator::getDescriptor() const
{
    return descriptor;
};

OriginId SourceDescriptorPhysicalOperator::getOriginId() const
{
    return originId;
};

bool SourceDescriptorPhysicalOperator::operator==(const SourceDescriptorPhysicalOperator& other) const
{
    return descriptor == other.descriptor;
}

std::optional<PhysicalOperator> SourceDescriptorPhysicalOperator::getChild() const
{
    return child;
}

void SourceDescriptorPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}

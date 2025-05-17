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

#include <memory>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <PhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>

namespace NES
{

SourcePhysicalOperator::SourcePhysicalOperator(std::shared_ptr<Sources::SourceDescriptor> descriptor, OriginId id)
    : originId(id), descriptor(std::move(std::move(descriptor))) { };

std::shared_ptr<Sources::SourceDescriptor> SourcePhysicalOperator::getDescriptor() const
{
    return descriptor;
};

OriginId SourcePhysicalOperator::getOriginId() const
{
    return originId;
};

bool SourcePhysicalOperator::operator==(const SourcePhysicalOperator& other) const
{
    return *descriptor == *other.descriptor;
}

std::optional<PhysicalOperator> SourcePhysicalOperator::getChild() const
{
    return child;
}

void SourcePhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}

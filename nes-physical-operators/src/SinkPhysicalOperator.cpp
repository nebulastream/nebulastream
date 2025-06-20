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

#include <SinkPhysicalOperator.hpp>

#include <memory>
#include <optional>
#include <utility>
#include <Sinks/SinkDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

SinkPhysicalOperator::SinkPhysicalOperator(const Sinks::SinkDescriptor& descriptor) : descriptor(descriptor) { };

Sinks::SinkDescriptor SinkPhysicalOperator::getDescriptor() const
{
    return descriptor;
};

bool SinkPhysicalOperator::operator==(const SinkPhysicalOperator& other) const
{
    return descriptor == other.descriptor;
}

std::optional<PhysicalOperator> SinkPhysicalOperator::getChild() const
{
    return std::nullopt;
}

void SinkPhysicalOperator::setChild(PhysicalOperator)
{
    PRECONDITION(false, "Sink has no child.");
}

}

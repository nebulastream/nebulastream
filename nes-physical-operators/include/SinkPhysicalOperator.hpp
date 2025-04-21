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
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
class SinkPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit SinkPhysicalOperator(std::shared_ptr<Sinks::SinkDescriptor> descriptor) : descriptor(descriptor) {};
    std::optional<PhysicalOperator> getChild() const override { return std::nullopt; }
    void setChild(PhysicalOperator) override { PRECONDITION(false, "Sink has no child."); }

    std::shared_ptr<Sinks::SinkDescriptor> getDescriptor() const { return descriptor; };

    bool operator==(const SinkPhysicalOperator& other) const { return *descriptor == *other.descriptor; }

private:
    std::shared_ptr<Sinks::SinkDescriptor> descriptor;
};
}

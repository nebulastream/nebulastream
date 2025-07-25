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
#include <optional>
#include <Nautilus/Interface/Record.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
class SinkPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit SinkPhysicalOperator(const Sinks::SinkDescriptor& descriptor);
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator) override;

    [[nodiscard]] Sinks::SinkDescriptor getDescriptor() const;

    bool operator==(const SinkPhysicalOperator& other) const;

private:
    Sinks::SinkDescriptor descriptor;
};
}

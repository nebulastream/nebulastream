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
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
class SourceDescriptorPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit SourceDescriptorPhysicalOperator(SourceDescriptor descriptor, OriginId id);
    explicit SourceDescriptorPhysicalOperator(SourceDescriptor descriptor, std::vector<OriginId> originIds);
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

    [[nodiscard]] SourceDescriptor getDescriptor() const;
    [[nodiscard]] OriginId getOriginId() const;
    [[nodiscard]] const std::vector<OriginId>& getOriginIds() const;

    bool operator==(const SourceDescriptorPhysicalOperator& other) const;

private:
    std::optional<PhysicalOperator> child;
    std::vector<OriginId> originIds;
    SourceDescriptor descriptor;
};
}

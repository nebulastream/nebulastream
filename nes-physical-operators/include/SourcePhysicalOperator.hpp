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
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
class SourcePhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit SourcePhysicalOperator(std::shared_ptr<Sources::SourceDescriptor> descriptor, OriginId id);
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

    [[nodiscard]] std::shared_ptr<Sources::SourceDescriptor> getDescriptor() const;
    [[nodiscard]] OriginId getOriginId() const;

    bool operator==(const SourcePhysicalOperator& other) const;

private:
    std::optional<PhysicalOperator> child;
    OriginId originId;
    std::shared_ptr<Sources::SourceDescriptor> descriptor;
};
}

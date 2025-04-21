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

#include <variant>
#include <optional>
#include <string_view>
#include <vector>
#include <memory>
#include <API/Schema.hpp>
#include <Plans/Operator.hpp>
#include <Traits/Trait.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES
{
class SerializableOperator;

struct LogicalOperatorConcept : public OperatorConcept
{
    ~LogicalOperatorConcept() override = default;
    virtual bool operator==(struct LogicalOperatorConcept const& rhs) const = 0;

    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;
    [[nodiscard]] virtual SerializableOperator serialize() const = 0;
    [[nodiscard]] virtual Optimizer::TraitSet getTraitSet() const = 0;

    [[nodiscard]] virtual std::vector<Schema> getInputSchemas() const = 0;
    [[nodiscard]] virtual Schema getOutputSchema() const = 0;

    [[nodiscard]] virtual std::vector<std::vector<OriginId>> getInputOriginIds() const = 0;
    [[nodiscard]] virtual std::vector<OriginId> getOutputOriginIds() const = 0;
};

}

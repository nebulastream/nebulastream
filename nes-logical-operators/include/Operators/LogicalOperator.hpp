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

#include <vector>
#include <memory>
#include <API/Schema.hpp>
#include <Plans/Operator.hpp>
#include <Traits/Trait.hpp>

namespace NES
{
class SerializableOperator;

/// Logical operator, enables schema inference and signature computation.
class LogicalOperator : public virtual Operator
{
public:
    explicit LogicalOperator() : outputSchema(Schema()) {};
    explicit LogicalOperator(std::shared_ptr<LogicalOperator> logicalOp);
    virtual std::string_view getName() const noexcept = 0;

    /// If this operator does not assign new origin ids, e.g., windowing,
    /// this function collects the origin ids from all upstream operators.
    virtual void inferInputOrigins() = 0;
    virtual std::vector<OriginId> getOutputOriginIds() const = 0;

    /// @brief infers the input and out schema of this operator depending on its child.
    /// @param typeInferencePhaseContext needed for stamp inferring
    /// @return true if schema was correctly inferred
    virtual bool inferSchema() = 0;

    virtual Schema getOutputSchema() const = 0;
    virtual void setOutputSchema(const Schema& outputSchema) = 0;

    virtual SerializableOperator serialize() const = 0;

    virtual bool operator==(Operator const& rhs) const = 0;
    virtual bool isIdentical(Operator const& rhs) const = 0;

    /// Holds physical operator properties
    std::vector<std::unique_ptr<Optimizer::AbstractTrait>> traitSet;

protected:
    /// The output schema of this operator
    Schema outputSchema;
};
}
FMT_OSTREAM(NES::LogicalOperator);

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

#include <Operators/Operator.hpp>

namespace NES
{
class SerializableOperator;

/// Logical operator, enables schema inference and signature computation.
class LogicalOperator : public virtual Operator
{
public:
    explicit LogicalOperator(OperatorId id);

    /// @brief Infers the input origin of a logical operator.
    /// If this operator does not assign new origin ids, e.g., windowing,
    /// this function collects the origin ids from all upstream operators.
    virtual void inferInputOrigins() = 0;

    /// @brief infers the input and out schema of this operator depending on its child.
    /// @param typeInferencePhaseContext needed for stamp inferring
    /// @return true if schema was correctly inferred
    virtual bool inferSchema() = 0;

    virtual SerializableOperator serialize() const = 0;
};
}

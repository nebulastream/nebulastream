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

#include <cstdint>
#include <memory>
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/UnaryLogicalOperator.hpp>

namespace NES
{

class LimitLogicalOperator : public UnaryLogicalOperator
{
public:
    explicit LimitLogicalOperator(uint64_t limit, OperatorId id);
    ~LimitLogicalOperator() override = default;

    uint64_t getLimit() const;

    [[nodiscard]] bool operator==(Operator const& rhs) const override;
    [[nodiscard]] bool isIdentical(Operator const& rhs) const override;

    /// @brief Infers the input and output schema of this operator depending on its child.
    /// @param typeInferencePhaseContext needed for stamp inferring
    /// @return true if schema was correctly inferred
    bool inferSchema() override;
    std::shared_ptr<Operator> clone() const override;

protected:
    std::string toString() const override;

private:
    uint64_t limit;
};

}

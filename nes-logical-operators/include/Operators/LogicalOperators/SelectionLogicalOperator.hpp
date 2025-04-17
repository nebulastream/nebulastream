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
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/UnaryLogicalOperator.hpp>

namespace NES
{

/// @brief Selection operator, which contains an function as a predicate.
class SelectionLogicalOperator : public UnaryLogicalOperator
{
public:
    explicit SelectionLogicalOperator(std::shared_ptr<LogicalFunction> const&, OperatorId id);
    ~SelectionLogicalOperator() override = default;

    /// @brief get the filter predicate.
    /// @return PredicatePtr
    std::shared_ptr<LogicalFunction> getPredicate() const;

    /// @brief exchanges the predicate of a filter with a new predicate
    /// @param newPredicate the predicate which will be the new predicate of the filter
    void setPredicate(std::shared_ptr<LogicalFunction> newPredicate);
    float getSelectivity() const;
    void setSelectivity(float newSelectivity);

    /// @brief check if two operators have the same filter predicate.
    /// @param rhs the operator to compare
    /// @return bool true if they are the same otherwise false
    [[nodiscard]] bool operator==(Operator const& rhs) const override;
    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;

    /// @brief infers the input and output schema of this operator depending on its child.
    /// @throws Exception the predicate function has to return a boolean.
    /// @param typeInferencePhaseContext needed for stamp inferring
    /// @return true if schema was correctly inferred
    bool inferSchema() override;
    std::shared_ptr<Operator> clone() const override;


    /// @brief returns the names of every attribute that is accessed in the predicate of this filter
    /// @return a vector containing every attribute name that is accessed by the predicate
    std::vector<std::string> getFieldNamesUsedByFilterPredicate() const;

protected:
    std::string toString() const override;

private:
    std::shared_ptr<LogicalFunction> predicate;
    float selectivity = 1.0f;
};
}

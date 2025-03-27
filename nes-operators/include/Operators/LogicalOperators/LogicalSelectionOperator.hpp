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
#include <Functions/NodeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES
{

/**
 * @brief Filter operator, which contains an function as a predicate.
 */
class LogicalSelectionOperator : public LogicalUnaryOperator
{
public:
    explicit LogicalSelectionOperator(std::shared_ptr<NodeFunction>, OperatorId id);
    ~LogicalSelectionOperator() override = default;

    /**
   * @brief get the filter predicate.
   * @return PredicatePtr
   */
    std::shared_ptr<NodeFunction> getPredicate() const;
    /**
     * @brief exchanges the predicate of a filter with a new predicate
     * @param newPredicate the predicate which will be the new predicate of the filter
     */
    void setPredicate(std::shared_ptr<NodeFunction> newPredicate);
    float getSelectivity() const;
    void setSelectivity(float newSelectivity);

    /**
     * @brief check if two operators have the same filter predicate.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    [[nodiscard]] bool isIdentical(const std::shared_ptr<Node>& rhs) const override;

    /**
    * @brief infers the input and output schema of this operator depending on its child.
    * @throws Exception the predicate function has to return a boolean.
    * @param typeInferencePhaseContext needed for stamp inferring
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
    std::shared_ptr<Operator> copy() override;

    /**
     * @brief returns the names of every attribute that is accessed in the predicate of this filter
     * @return a vector containing every attribute name that is accessed by the predicate
     */
    std::vector<IdentifierList> getFieldNamesUsedByFilterPredicate() const;

protected:
    std::string toString() const override;

private:
    std::shared_ptr<NodeFunction> predicate;
    float selectivity = 1.0F;
};
}

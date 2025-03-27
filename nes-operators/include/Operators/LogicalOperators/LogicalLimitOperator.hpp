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
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES
{

/**
 * @brief Limit operator
 */
class LogicalLimitOperator : public LogicalUnaryOperator
{
public:
    explicit LogicalLimitOperator(uint64_t limit, OperatorId id);
    ~LogicalLimitOperator() override = default;

    /**
   * @brief get the limit count.
   * @return limit
   */
    uint64_t getLimit() const;

    /**
     * @brief check if two operators have the same limit predicate.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    [[nodiscard]] bool isIdentical(const std::shared_ptr<Node>& rhs) const override;

    /**
    * @brief Infers the input and output schema of this operator depending on its child.
    * @throws Exception the predicate function has to return a boolean.
    * @param typeInferencePhaseContext needed for stamp inferring
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
    std::shared_ptr<Operator> copy() override;

protected:
    std::string toString() const override;

private:
    uint64_t limit;
};

}

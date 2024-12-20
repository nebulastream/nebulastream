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
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES
{

/**
 * @brief DelayTuples operator, which contains an expression as a predicate.
 */
class LogicalDelayTuplesOperator : public LogicalUnaryOperator
{
public:
    explicit LogicalDelayTuplesOperator(OperatorId id);
    ~LogicalDelayTuplesOperator() override = default;

    /**
     * @brief check if two operators have the same parameters.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    std::string toString() const override;

    /**
    * @brief infers the input and output schema of this operator depending on its child.
    * @throws Exception the predicate expression has to return a boolean.
    * @param typeInferencePhaseContext needed for stamp inferring
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
    OperatorPtr copy() override;
    void inferStringSignature() override;
};
using LogicalDelayTuplesOperatorPtr = std::shared_ptr<LogicalDelayTuplesOperator>;
} // namespace NES
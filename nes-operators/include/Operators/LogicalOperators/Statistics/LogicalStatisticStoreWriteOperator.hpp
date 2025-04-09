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

#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES
{

class LogicalStatisticStoreWriteOperator final : public LogicalUnaryOperator
{
public:
    explicit LogicalStatisticStoreWriteOperator(OperatorId id);

    /**
    * @brief Infers the input and output schema of this operator depending on its child.
    * @throws Exception the predicate function has to return a boolean.
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
    void inferStringSignature() override;
    std::shared_ptr<Operator> copy() override;
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    [[nodiscard]] bool isIdentical(const std::shared_ptr<Node>& rhs) const override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;
};

}

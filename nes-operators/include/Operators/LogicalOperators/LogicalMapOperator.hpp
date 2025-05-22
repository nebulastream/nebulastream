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
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES
{

/**
 * @brief Map operator, which contains an field assignment function that manipulates a field of the record.
 */
class LogicalMapOperator : public LogicalUnaryOperator
{
public:
    LogicalMapOperator(const std::shared_ptr<NodeFunctionFieldAssignment>& mapFunction, OperatorId id);

    /**
    * @brief Returns the function of this map operator
    * @return std::shared_ptr<NodeFunctionFieldAssignment>
    */
    std::shared_ptr<NodeFunctionFieldAssignment> getMapFunction() const;

    /**
     * @brief Infers the schema of the map operator. We support two cases:
     * 1. the assignment statement manipulates a already existing field. In this case the data type of the field can change.
     * 2. the assignment statement creates a new field with an inferred data type.
     * @throws throws exception if inference was not possible.
     * @param typeInferencePhaseContext needed for stamp inferring
     * @return true if inference was possible
     */
    bool inferSchema() override;
    void inferStringSignature() override;
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    [[nodiscard]] bool isIdentical(const std::shared_ptr<Node>& rhs) const override;
    std::shared_ptr<Operator> copy() override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;

private:
    std::shared_ptr<NodeFunctionFieldAssignment> mapFunction;
};
}

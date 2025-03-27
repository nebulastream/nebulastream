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
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES
{

/**
 * @brief this operator renames the source
 */
class RenameSourceOperator : public LogicalUnaryOperator
{
public:
    explicit RenameSourceOperator(const IdentifierList& newSourceName, OperatorId id);
    ~RenameSourceOperator() override = default;

    /**
     * @brief check if two operators have the same output schema
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    [[nodiscard]] bool isIdentical(const std::shared_ptr<Node>& rhs) const override;

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @param typeInferencePhaseContext needed for stamp inferring
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
    std::shared_ptr<Operator> copy() override;
    IdentifierList getNewSourceName() const;

protected:
    [[nodiscard]] std::string toString() const override;

private:
    const IdentifierList newSourceName;
};

}

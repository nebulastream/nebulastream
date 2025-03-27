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
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>

namespace NES
{

/**
 * @brief Union operator that Union two sources together. This operator behaves similar to the unionWith operator in RDBMS.
 */
class LogicalUnionOperator : public LogicalBinaryOperator
{
public:
    explicit LogicalUnionOperator(OperatorId id);
    ~LogicalUnionOperator() override = default;

    [[nodiscard]] bool isIdentical(const std::shared_ptr<Node>& rhs) const override;
    ///infer schema of two child operators
    bool inferSchema() override;
    void inferInputOrigins() override;
    std::shared_ptr<Operator> copy() override;
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;

protected:
    std::string toString() const override;
};
}

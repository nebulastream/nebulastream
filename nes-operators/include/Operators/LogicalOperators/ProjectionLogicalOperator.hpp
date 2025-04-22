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
#include <Operators/LogicalOperators/UnaryLogicalOperator.hpp>


namespace NES
{

/// The projection operator only narrows down the fields of an input schema to a smaller subset. The map operator handles renaming and adding new fields.
class ProjectionLogicalOperator : public UnaryLogicalOperator
{
public:
    explicit ProjectionLogicalOperator(std::vector<std::shared_ptr<LogicalFunction>> functions, OperatorId id);
    ~ProjectionLogicalOperator() override = default;

    const std::vector<std::shared_ptr<LogicalFunction>>& getFunctions() const;

    [[nodiscard]] bool operator==(const Operator& rhs) const override;
    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;


    bool inferSchema() override;
    std::shared_ptr<Operator> clone() const override;

protected:
    [[nodiscard]] std::string toString() const override;

private:
    std::vector<std::shared_ptr<LogicalFunction>> functions;
};

}

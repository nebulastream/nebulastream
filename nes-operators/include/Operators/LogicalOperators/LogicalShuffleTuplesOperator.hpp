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


class LogicalShuffleTuplesOperator : public LogicalUnaryOperator
{
public:
    explicit LogicalShuffleTuplesOperator(OperatorId id);
    ~LogicalShuffleTuplesOperator() override = default;
    [[nodiscard]] bool equal(std::shared_ptr<Node> const& rhs) const override;
    [[nodiscard]] bool isIdentical(std::shared_ptr<Node> const& rhs) const override;
    std::string toString() const override;
    bool inferSchema() override;
    std::shared_ptr<Operator> copy() override;
    void inferStringSignature() override;
};
using LogicalShuffleTuplesOperatorPtr = std::shared_ptr<LogicalShuffleTuplesOperator>;
} // namespace NES
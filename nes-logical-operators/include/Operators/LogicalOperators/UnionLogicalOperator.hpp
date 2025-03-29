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
#include <Operators/LogicalOperators/BinaryLogicalOperator.hpp>
#include <Configurations/Descriptor.hpp>

namespace NES
{

class UnionLogicalOperator : public BinaryLogicalOperator
{
public:
    explicit UnionLogicalOperator(OperatorId id);
    ~UnionLogicalOperator() override = default;

    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;
    ///infer schema of two child operators
    bool inferSchema() override;
    void inferInputOrigins() override;
    std::shared_ptr<Operator> clone() const override;
    [[nodiscard]] bool operator==(Operator const& rhs) const override;

protected:
    std::string toString() const override;
};
}

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

#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

class PhysicalShuffleTuplesOperator : public PhysicalUnaryOperator, public AbstractScanOperator, public AbstractEmitOperator
{
public:
    PhysicalShuffleTuplesOperator(OperatorId id, const std::shared_ptr<Schema>& inputSchema, const float& unorderedness);
    static std::shared_ptr<PhysicalOperator> create(OperatorId id, const std::shared_ptr<Schema>& inputSchema, const float& unorderedness);
    static std::shared_ptr<PhysicalOperator> create(std::shared_ptr<Schema> inputSchema, const float& unorderedness);
    float getUnorderedness() const;
    std::string toString() const override;
    std::shared_ptr<Operator> copy() override;

protected:
    float unorderedness;
};
}

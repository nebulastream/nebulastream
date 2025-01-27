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
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>

namespace NES
{

/**
 * @brief Watermark assignment operator, creates a watermark timestamp per input buffer.
 */
class WatermarkAssignerLogicalOperator : public LogicalUnaryOperator
{
public:
    WatermarkAssignerLogicalOperator(std::shared_ptr<Windowing::WatermarkStrategyDescriptor>, OperatorId id);
    std::shared_ptr<Windowing::WatermarkStrategyDescriptor> getWatermarkStrategyDescriptor() const;
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    [[nodiscard]] bool isIdentical(const std::shared_ptr<Node>& rhs) const override;
    std::shared_ptr<Operator> copy() override;
    bool inferSchema() override;
    void inferStringSignature() override;

protected:
    [[nodiscard]] std::string toString() const override;

private:
    std::shared_ptr<Windowing::WatermarkStrategyDescriptor> watermarkStrategyDescriptor;
};


}

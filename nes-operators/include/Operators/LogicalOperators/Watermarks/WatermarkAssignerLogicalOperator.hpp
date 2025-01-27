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

#include <Operators/AbstractOperators/Arity/UnaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include "Identifiers/Identifiers.hpp"
#include "Nodes/Node.hpp"

namespace NES
{

/**
 * @brief Watermark assignment operator, creates a watermark timestamp per input buffer.
 */
class WatermarkAssignerLogicalOperator : public LogicalUnaryOperator
{
public:
    WatermarkAssignerLogicalOperator(Windowing::WatermarkStrategyDescriptorPtr, OperatorId id);
    Windowing::WatermarkStrategyDescriptorPtr getWatermarkStrategyDescriptor() const;
    [[nodiscard]] bool equal(const NodePtr& rhs) const override;
    [[nodiscard]] bool isIdentical(const NodePtr& rhs) const override;
    std::shared_ptr<Operator> copy() override;
    bool inferSchema() override;
    void inferStringSignature() override;

protected:
    [[nodiscard]] std::string toString() const override;

private:
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor;
};

using WatermarkAssignerLogicalOperatorPtr = std::shared_ptr<WatermarkAssignerLogicalOperator>;

}

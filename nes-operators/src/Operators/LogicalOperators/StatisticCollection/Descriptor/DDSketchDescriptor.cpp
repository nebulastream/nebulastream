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
#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Expressions/ArithmeticalExpressions/AbsExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/FloorExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/Functions/FunctionExpressionNode.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/DDSketchDescriptor.hpp>
#include <Util/StdInt.hpp>
#include <cmath>

namespace NES::Statistic {

bool DDSketchDescriptor::equal(const WindowStatisticDescriptorPtr& rhs) const {
    if (rhs->instanceOf<DDSketchDescriptor>()) {
        auto rhsDDSketchDescriptor = rhs->as<DDSketchDescriptor>();
        return field->equal(rhsDDSketchDescriptor->field)
            && relativeError == rhsDDSketchDescriptor->relativeError
            && width == rhsDDSketchDescriptor->width;
    }
    return false;
}

void DDSketchDescriptor::addDescriptorFields(Schema&, Schema& outputSchema, const std::string& qualifierNameWithSeparator) {
    using enum BasicType;
    outputSchema.addField(qualifierNameWithSeparator + WIDTH_FIELD_NAME, UINT64);
    outputSchema.addField(qualifierNameWithSeparator + GAMMA_FIELD_NAME, FLOAT64);
    outputSchema.addField(qualifierNameWithSeparator + STATISTIC_DATA_FIELD_NAME, TEXT);
}

std::string DDSketchDescriptor::toString() { return "DDSketchDescriptor"; }

double DDSketchDescriptor::getRelativeError() const { return relativeError; }
double DDSketchDescriptor::calculateGamma() const { return (1.0 + relativeError) / (1.0 - relativeError); }
ExpressionNodePtr DDSketchDescriptor::getCalcLogFloorIndexExpressions() const { return calcLogFloorIndexExpressions; }
ExpressionNodePtr DDSketchDescriptor::getGreaterThanZeroExpression() const { return greaterThanZeroExpression; }
ExpressionNodePtr DDSketchDescriptor::getLessThanZeroExpression() const { return lessThanZeroExpression; }

uint64_t DDSketchDescriptor::getNumberOfPreAllocatedBuckets() const { return width; }

WindowStatisticDescriptorPtr DDSketchDescriptor::create(FieldAccessExpressionNodePtr field) {
    return create(field, DEFAULT_RELATIVE_ERROR, NUMBER_OF_PRE_ALLOCATED_BUCKETS);
}

WindowStatisticDescriptorPtr
DDSketchDescriptor::create(FieldAccessExpressionNodePtr field, double error, uint64_t numberOfPreAllocatedBuckets) {
    return std::make_shared<DDSketchDescriptor>(DDSketchDescriptor(field, error, numberOfPreAllocatedBuckets));
}

void DDSketchDescriptor::inferStamps(const SchemaPtr& inputSchema) {
    WindowStatisticDescriptor::inferStamps(inputSchema);
    calcLogFloorIndexExpressions->inferStamp(inputSchema);
    greaterThanZeroExpression->inferStamp(inputSchema);
    lessThanZeroExpression->inferStamp(inputSchema);
}

DDSketchDescriptor::~DDSketchDescriptor() = default;

DDSketchDescriptor::DDSketchDescriptor(FieldAccessExpressionNodePtr field, double error, uint64_t numberOfPreAllocatedBuckets)
: WindowStatisticDescriptor(field, numberOfPreAllocatedBuckets), relativeError(error) {
    const auto log10GammaValueExpr = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::FLOAT64, std::to_string(std::log10(calculateGamma()))));
    const auto zeroConstantValueExpression = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, std::to_string(0)));
    const auto absField = AbsExpressionNode::create(field);
    const auto log10AbsFieldExpr = FunctionExpression::create(DataTypeFactory::createUndefined(), "log10", {absField});
    const auto divLog10AbsFieldAndLog10Gamma = DivExpressionNode::create(std::move(log10AbsFieldExpr), std::move(log10GammaValueExpr));
    calcLogFloorIndexExpressions = FloorExpressionNode::create(divLog10AbsFieldAndLog10Gamma);
    greaterThanZeroExpression = GreaterExpressionNode::create(field, zeroConstantValueExpression);
    lessThanZeroExpression = LessExpressionNode::create(field, zeroConstantValueExpression);
}
} // namespace NES::Statistic
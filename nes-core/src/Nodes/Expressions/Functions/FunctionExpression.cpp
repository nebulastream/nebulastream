#include <Nodes/Expressions/Functions/FunctionExpressionNode.hpp>
#include <Nodes/Expressions/Functions/FunctionRegistry.hpp>
#include <utility>

namespace NES {

FunctionExpression::FunctionExpression(DataTypePtr stamp,
                                       std::string functionName,
                                       std::vector<ExpressionNodePtr> arguments,
                                       std::unique_ptr<LogicalFunction> function)
    : ExpressionNode(std::move(stamp)), functionName(std::move(functionName)), arguments(std::move(arguments)),
      function(std::move(function)) {}

ExpressionNodePtr FunctionExpression::create(const DataTypePtr& stamp,
                                             const std::string& functionName,
                                             const std::vector<ExpressionNodePtr>& arguments) {
    auto function = FunctionRegistry::getFunction(functionName);
    return std::make_shared<FunctionExpression>(stamp, functionName, arguments, std::move(function));
}

void FunctionExpression::inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext, SchemaPtr schema) {
    std::vector<DataTypePtr> argumentTypes;
    for (const auto& input : arguments) {
        input->inferStamp(typeInferencePhaseContext, schema);
        argumentTypes.emplace_back(input->getStamp());
    }
    auto resultStamp = function->inferStamp(argumentTypes);
    setStamp(resultStamp);
}

std::string FunctionExpression::toString() const { return functionName; }

bool FunctionExpression::equal(const NodePtr& rhs) const {
    if (rhs->instanceOf<FunctionExpression>()) {
        auto otherAddNode = rhs->as<FunctionExpression>();
        return functionName == otherAddNode->functionName;
    }
    return false;
}

ExpressionNodePtr FunctionExpression::copy() { return FunctionExpression::create(stamp, functionName, arguments); }

}// namespace NES
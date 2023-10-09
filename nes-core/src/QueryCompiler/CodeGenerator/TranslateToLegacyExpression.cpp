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

#include <API/AttributeField.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/CeilExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/ExpExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/FloorExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/ModExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/RoundExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/SqrtExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Operators/Expressions/CaseExpressionNode.hpp>
#include <Operators/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/Functions/FunctionExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/Expressions/WhenExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <QueryCompiler/CodeGenerator/LegacyExpression.hpp>
#include <QueryCompiler/CodeGenerator/TranslateToLegacyExpression.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::QueryCompilation {
TranslateToLegacyExpressionPtr TranslateToLegacyExpression::create() { return std::make_shared<TranslateToLegacyExpression>(); }

TranslateToLegacyExpression::TranslateToLegacyExpression() = default;

/**
 * Translate the expression node into the corresponding user api expression of the legacy api.
 * To this end we first cast the expression node in the right subtype and then translate it.
 */
LegacyExpressionPtr TranslateToLegacyExpression::transformExpression(const ExpressionNodePtr& expression) {
    if (expression->instanceOf<LogicalExpressionNode>()) {
        // Translate logical expressions to the legacy representation
        return transformLogicalExpressions(expression);
    }
    if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        // Translate arithmetical expressions to the legacy representation
        return transformArithmeticalExpressions(expression);
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        // Translate constant value expression node.
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        return PredicateItem(value).copy();
    } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
        // Translate field read expression node.
        auto fieldReadExpression = expression->as<FieldAccessExpressionNode>();
        auto fieldName = fieldReadExpression->getFieldName();
        auto stamp = fieldReadExpression->getStamp();
        NES_DEBUG("TranslateToLegacyPhase: Translate FieldAccessExpressionNode:  {}", expression->toString());
        return Field(AttributeField::create(fieldName, stamp)).copy();
    } else if (expression->instanceOf<WhenExpressionNode>()) {
        auto whenExpression = expression->as<WhenExpressionNode>();
        auto legacyLeft = transformExpression(whenExpression->getLeft());
        auto legacyRight = transformExpression(whenExpression->getRight());
        NES_DEBUG("TranslateToLegacyPhase: Translate WhenExpressionNode:  {}", whenExpression->toString());
        return WhenPredicate(legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<CaseExpressionNode>()) {
        auto caseExpressionNode = expression->as<CaseExpressionNode>();
        std::vector<LegacyExpressionPtr> whenExprs;
        for (const auto& elem : caseExpressionNode->getWhenChildren()) {
            whenExprs.push_back(transformExpression(elem));
        }
        auto legacyDefault = transformExpression(caseExpressionNode->getDefaultExp());
        NES_DEBUG("TranslateToLegacyPhase: Translate CaseExpressionNode:  {}", caseExpressionNode->toString());
        return CasePredicate(whenExprs, legacyDefault).copy();
    } else if (expression->instanceOf<FunctionExpression>()) {
        // Translate function expressions to legacy expressions in old query compiler.
        auto function = expression->as<FunctionExpression>();
        if (function->getFunctionName() == "log10") {
            // Translate LOG10 expression node.
            auto legacyChild = transformExpression(function->getArguments()[0]);
            return UnaryPredicate(UnaryOperatorType::LOG10_OP, legacyChild).copy();
        } else if (function->getFunctionName() == "ln") {
            // Translate LOG expression node.
            auto legacyChild = transformExpression(function->getArguments()[0]);
            return UnaryPredicate(UnaryOperatorType::LOG_OP, legacyChild).copy();
        } else if (function->getFunctionName() == "abs") {
            // Translate ABS expression node.
            auto legacyChild = transformExpression(function->getArguments()[0]);
            return UnaryPredicate(UnaryOperatorType::ABSOLUTE_VALUE_OF_OP, legacyChild).copy();
        } else if (function->getFunctionName() == "power") {
            // Translate POWER expression node.
            auto legacyLeft = transformExpression(function->getArguments()[0]);
            auto legacyRight = transformExpression(function->getArguments()[1]);
            return Predicate(BinaryOperatorType::POWER_OP, legacyLeft, legacyRight).copy();
        }
    }
    NES_FATAL_ERROR("TranslateToLegacyPhase: No transformation implemented for this expression node: {}", expression->toString());
    NES_NOT_IMPLEMENTED();
    ;
}

LegacyExpressionPtr TranslateToLegacyExpression::transformArithmeticalExpressions(const ExpressionNodePtr& expression) {
    if (expression->instanceOf<AddExpressionNode>()) {
        // Translate add expression node.
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto legacyLeft = transformExpression(addExpressionNode->getLeft());
        auto legacyRight = transformExpression(addExpressionNode->getRight());
        return Predicate(BinaryOperatorType::PLUS_OP, legacyLeft, legacyRight).copy();
    }
    if (expression->instanceOf<SubExpressionNode>()) {
        // Translate sub expression node.
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto legacyLeft = transformExpression(subExpressionNode->getLeft());
        auto legacyRight = transformExpression(subExpressionNode->getRight());
        return Predicate(BinaryOperatorType::MINUS_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<MulExpressionNode>()) {
        // Translate mul expression node.
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto legacyLeft = transformExpression(mulExpressionNode->getLeft());
        auto legacyRight = transformExpression(mulExpressionNode->getRight());
        return Predicate(BinaryOperatorType::MULTIPLY_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<DivExpressionNode>()) {
        // Translate div expression node.
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto legacyLeft = transformExpression(divExpressionNode->getLeft());
        auto legacyRight = transformExpression(divExpressionNode->getRight());
        return Predicate(BinaryOperatorType::DIVISION_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<ModExpressionNode>()) {
        // Translate MODULO expression node.
        auto modExpressionNode = expression->as<ModExpressionNode>();
        auto legacyLeft = transformExpression(modExpressionNode->getLeft());
        auto legacyRight = transformExpression(modExpressionNode->getRight());
        return Predicate(BinaryOperatorType::MODULO_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<CeilExpressionNode>()) {
        // Translate CEIL expression node.
        auto ceilExpressionNode = expression->as<CeilExpressionNode>();
        auto legacyChild = transformExpression(ceilExpressionNode->child());
        return UnaryPredicate(UnaryOperatorType::CEIL_OP, legacyChild).copy();
    } else if (expression->instanceOf<ExpExpressionNode>()) {
        // Translate EXP expression node.
        auto expExpressionNode = expression->as<ExpExpressionNode>();
        auto legacyChild = transformExpression(expExpressionNode->child());
        return UnaryPredicate(UnaryOperatorType::EXP_OP, legacyChild).copy();
    } else if (expression->instanceOf<FloorExpressionNode>()) {
        // Translate FLOOR expression node.
        auto floorExpressionNode = expression->as<FloorExpressionNode>();
        auto legacyChild = transformExpression(floorExpressionNode->child());
        return UnaryPredicate(UnaryOperatorType::FLOOR_OP, legacyChild).copy();
    } else if (expression->instanceOf<RoundExpressionNode>()) {
        // Translate ROUND expression node.
        auto roundExpressionNode = expression->as<RoundExpressionNode>();
        auto legacyChild = transformExpression(roundExpressionNode->child());
        return UnaryPredicate(UnaryOperatorType::ROUND_OP, legacyChild).copy();
    } else if (expression->instanceOf<SqrtExpressionNode>()) {
        // Translate SQRT expression node.
        auto sqrtExpressionNode = expression->as<SqrtExpressionNode>();
        auto legacyChild = transformExpression(sqrtExpressionNode->child());
        return UnaryPredicate(UnaryOperatorType::SQRT_OP, legacyChild).copy();
    }
    NES_FATAL_ERROR("TranslateToLegacyPhase: No transformation implemented for this arithmetical expression node: {}",
                    expression->toString());
    NES_NOT_IMPLEMENTED();
}

LegacyExpressionPtr TranslateToLegacyExpression::transformLogicalExpressions(const ExpressionNodePtr& expression) {
    if (expression->instanceOf<AndExpressionNode>()) {
        // Translate and expression node.
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto legacyLeft = transformExpression(andExpressionNode->getLeft());
        auto legacyRight = transformExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LOGICAL_AND_OP, legacyLeft, legacyRight).copy();
    }
    if (expression->instanceOf<OrExpressionNode>()) {
        // Translate or expression node.
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto legacyLeft = transformExpression(orExpressionNode->getLeft());
        auto legacyRight = transformExpression(orExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LOGICAL_OR_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<LessExpressionNode>()) {
        // Translate less expression node.
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto legacyLeft = transformExpression(lessExpressionNode->getLeft());
        auto legacyRight = transformExpression(lessExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LESS_THAN_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        // Translate less equals expression node.
        auto andExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto legacyLeft = transformExpression(andExpressionNode->getLeft());
        auto legacyRight = transformExpression(andExpressionNode->getRight());
        return Predicate(BinaryOperatorType::LESS_THAN_EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        // Translate greater expression node.
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto legacyLeft = transformExpression(greaterExpressionNode->getLeft());
        auto legacyRight = transformExpression(greaterExpressionNode->getRight());
        return Predicate(BinaryOperatorType::GREATER_THAN_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        // Translate greater equals expression node.
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto legacyLeft = transformExpression(greaterEqualsExpressionNode->getLeft());
        auto legacyRight = transformExpression(greaterEqualsExpressionNode->getRight());
        return Predicate(BinaryOperatorType::GREATER_THAN_EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        // Translate equals expression node.
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto legacyLeft = transformExpression(equalsExpressionNode->getLeft());
        auto legacyRight = transformExpression(equalsExpressionNode->getRight());
        return Predicate(BinaryOperatorType::EQUAL_OP, legacyLeft, legacyRight).copy();
    } else if (expression->instanceOf<NegateExpressionNode>()) {
        auto const negateExpressionNode = expression->as<NegateExpressionNode>();
        (void) negateExpressionNode;
        NES_FATAL_ERROR("TranslateToLegacyPhase: Unary expressions not supported in "
                        "legacy expressions: {}",
                        expression->toString());
        NES_NOT_IMPLEMENTED();
    }
    NES_FATAL_ERROR("TranslateToLegacyPhase: No transformation implemented for this "
                    "logical expression node: {}",
                    expression->toString());
    NES_NOT_IMPLEMENTED();
    ;
}
}// namespace NES::QueryCompilation
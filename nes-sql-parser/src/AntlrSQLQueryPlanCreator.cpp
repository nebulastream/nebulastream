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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <regex>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <ParserRuleContext.h>
#include <AntlrSQLParser/AntlrSQLHelper.hpp>
#include <AntlrSQLParser/AntlrSQLQueryPlanCreator.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Functions/ConcatLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Functions/LogicalFunctions/AndLogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsLogicalFunction.hpp>
#include <Functions/LogicalFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/LogicalFunctions/GreaterLogicalFunction.hpp>
#include <Functions/LogicalFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/LogicalFunctions/LessLogicalFunction.hpp>
#include <Functions/LogicalFunctions/NegateLogicalFunction.hpp>
#include <Functions/LogicalFunctions/OrLogicalFunction.hpp>
#include <Functions/RenameLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MedianAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MinAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Plans/QueryPlan.hpp>
#include <Plans/QueryPlanBuilder.hpp>
#include <Util/Common.hpp>
#include <Util/Strings.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/ThresholdWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <ParserUtil.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::Parsers
{

// Helper function to convert a QueryPlan to string (for debugging)
std::string queryPlanToString(const std::unique_ptr<QueryPlan>& queryPlan)
{
    const std::regex regex1("  ");
    const std::regex regex2("[0-9]");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), regex1, "");
    queryPlanStr = std::regex_replace(queryPlanStr, regex2, "");
    return queryPlanStr;
}

QueryPlan AntlrSQLQueryPlanCreator::getQueryPlan() const
{
    // For simplicity, assume that sinkNames and queryPlans have been populated.
    return QueryPlanBuilder::addSink(sinkNames.front(), std::move(const_cast<std::stack<QueryPlan>&>(queryPlans).top()));
}

Windowing::TimeMeasure buildTimeMeasure(const int size, const uint64_t timebase)
{
    switch (timebase)
    {
        case AntlrSQLLexer::MS:
            return API::Milliseconds(size);
        case AntlrSQLLexer::SEC:
            return API::Seconds(size);
        case AntlrSQLLexer::MINUTE:
            return API::Minutes(size);
        case AntlrSQLLexer::HOUR:
            return API::Hours(size);
        case AntlrSQLLexer::DAY:
            return API::Days(size);
        default: {
            const AntlrSQLLexer lexer(nullptr);
            const std::string tokenName = std::string(lexer.getVocabulary().getSymbolicName(timebase));
            throw InvalidQuerySyntax("Unknown time unit: {}", tokenName);
        }
    }
}

std::unique_ptr<LogicalFunction> createFunctionFromOpBoolean(
    std::unique_ptr<LogicalFunction> leftFunction, std::unique_ptr<LogicalFunction> rightFunction, const uint64_t tokenType)
{
    switch (tokenType)
    {
        case AntlrSQLLexer::EQ:
            return std::make_unique<EqualsLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::NEQJ:
            return std::make_unique<NegateLogicalFunction>(
                std::make_unique<EqualsLogicalFunction>(std::move(leftFunction), std::move(rightFunction)));
        case AntlrSQLLexer::LT:
            return std::make_unique<LessLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::GT:
            return std::make_unique<GreaterLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::GTE:
            return std::make_unique<GreaterEqualsLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::LTE:
            return std::make_unique<LessEqualsLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
        default: {
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown Comparison Operator: {} of type: {}", lexer.getVocabulary().getSymbolicName(tokenType), tokenType);
        }
    }
}

std::unique_ptr<LogicalFunction> createLogicalBinaryFunction(
    std::unique_ptr<LogicalFunction> leftFunction, std::unique_ptr<LogicalFunction> rightFunction, const uint64_t tokenType)
{
    switch (tokenType)
    {
        case AntlrSQLLexer::AND:
            return std::make_unique<AndLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::OR:
            return std::make_unique<OrLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
        default: {
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown binary function in SQL query for op {} with type: {} and left {} and right {}",
                lexer.getVocabulary().getSymbolicName(tokenType),
                tokenType,
                *leftFunction,
                *rightFunction);
        }
    }
}

void AntlrSQLQueryPlanCreator::enterSelectClause(AntlrSQLParser::SelectClauseContext* context)
{
    helper.isSelect = true;
    AntlrSQLBaseListener::enterSelectClause(context);
}

void AntlrSQLQueryPlanCreator::exitSelectClause(AntlrSQLParser::SelectClauseContext* context)
{
    for (auto& selectFunction : helper.functionBuilder)
    {
        helper.addProjectionField(std::move(selectFunction));
    }
    helper.functionBuilder.clear();
    helper.isSelect = false;
    AntlrSQLBaseListener::exitSelectClause(context);
}

void AntlrSQLQueryPlanCreator::enterFromClause(AntlrSQLParser::FromClauseContext* context)
{
    helper.isFrom = true;
    AntlrSQLBaseListener::enterFromClause(context);
}

void AntlrSQLQueryPlanCreator::exitFromClause(AntlrSQLParser::FromClauseContext* context)
{
    helper.isFrom = false;
    AntlrSQLBaseListener::exitFromClause(context);
}

void AntlrSQLQueryPlanCreator::enterWhereClause(AntlrSQLParser::WhereClauseContext* context)
{
    helper.isWhereOrHaving = true;
    AntlrSQLBaseListener::enterWhereClause(context);
}

void AntlrSQLQueryPlanCreator::exitWhereClause(AntlrSQLParser::WhereClauseContext* context)
{
    if (helper.functionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There were more than 1 functions in the functionBuilder in exitWhereClause.");
    }
    helper.addWhereClause(std::move(helper.functionBuilder.back()));
    helper.functionBuilder.clear();
    helper.isWhereOrHaving = false;
    AntlrSQLBaseListener::exitWhereClause(context);
}

void AntlrSQLQueryPlanCreator::enterComparisonOperator(AntlrSQLParser::ComparisonOperatorContext* context)
{
    auto opTokenType = context->getStart()->getType();
    helper.opBoolean = opTokenType;
    AntlrSQLBaseListener::enterComparisonOperator(context);
}

void AntlrSQLQueryPlanCreator::exitArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext* context)
{
    std::unique_ptr<LogicalFunction> function;
    auto& rightFunction = helper.functionBuilder.back();
    helper.functionBuilder.pop_back();
    auto& leftFunction = helper.functionBuilder.back();
    helper.functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType)
    {
        case AntlrSQLLexer::ASTERISK:
            function = std::make_unique<MulLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
            break;
        case AntlrSQLLexer::SLASH:
            function = std::make_unique<DivLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
            break;
        case AntlrSQLLexer::PLUS:
            function = std::make_unique<AddLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
            break;
        case AntlrSQLLexer::MINUS:
            function = std::make_unique<SubLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
            break;
        case AntlrSQLLexer::PERCENT:
            function = std::make_unique<ModuloLogicalFunction>(std::move(leftFunction), std::move(rightFunction));
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    helper.functionBuilder.push_back(std::move(function));
    AntlrSQLBaseListener::exitArithmeticBinary(context);
}

void AntlrSQLQueryPlanCreator::exitArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext* context)
{
    std::unique_ptr<LogicalFunction> function;
    auto innerFunction = std::move(helper.functionBuilder.back());
    helper.functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType)
    {
        case AntlrSQLLexer::PLUS:
            function = std::move(innerFunction);
            break;
        case AntlrSQLLexer::MINUS:
            function = std::make_unique<MulLogicalFunction>(
                std::make_unique<ConstantValueLogicalFunction>(DataTypeProvider::provideBasicType(BasicType::UINT64), "-1"),
                std::move(innerFunction));
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    helper.functionBuilder.push_back(std::move(function));
    AntlrSQLBaseListener::exitArithmeticUnary(context);
}

void AntlrSQLQueryPlanCreator::enterUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext* context)
{
    const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent);
    const bool isParentRuleTableAlias = (parentContext != nullptr) && (parentContext->getRuleIndex() == AntlrSQLParser::RuleTableAlias);
    if (helper.isFrom && !helper.isJoinRelation)
    {
        helper.newSourceName = context->getText();
    }
    else if (helper.isJoinRelation && isParentRuleTableAlias)
    {
        helper.joinSourceRenames.emplace_back(context->getText());
    }
    AntlrSQLBaseListener::enterUnquotedIdentifier(context);
}


void AntlrSQLQueryPlanCreator::exitLogicalBinary(AntlrSQLParser::LogicalBinaryContext* context)
{
    /// If we are exiting a logical binary operator in a join relation, we need to build the binary function for the joinKey and
    /// not for the general function
    if (helper.isJoinRelation)
    {
        auto& rightFunction = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();
        auto& leftFunction = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();

        const auto opTokenType = context->op->getType();
        auto function = createLogicalBinaryFunction(std::move(leftFunction), std::move(rightFunction), opTokenType);
        helper.joinKeyRelationHelper.push_back(function->clone());
        helper.joinFunction = std::move(function);
    }
    else
    {
        auto& rightFunction = helper.functionBuilder.back();
        helper.functionBuilder.pop_back();
        auto& leftFunction = helper.functionBuilder.back();
        helper.functionBuilder.pop_back();

        const auto opTokenType = context->op->getType();
        helper.functionBuilder.push_back(createLogicalBinaryFunction(std::move(leftFunction), std::move(rightFunction), opTokenType));
    }
}


void AntlrSQLQueryPlanCreator::enterSinkClause(AntlrSQLParser::SinkClauseContext* context)
{
    if (context->sink().empty())
        throw InvalidQuerySyntax("INTO must be followed by at least one sink-identifier.");
    /// Store all specified sinks.
    for (const auto& sink : context->sink())
    {
        const auto sinkIdentifier = sink->identifier();
        sinkNames.emplace_back(sinkIdentifier->getText());
    }
}

void AntlrSQLQueryPlanCreator::enterIdentifier(AntlrSQLParser::IdentifierContext* context)
{
    std::optional<size_t> parentRuleIndex;
    if (const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }
    if (helper.isGroupBy)
    {
        helper.groupByFields.push_back(std::make_unique<FieldAccessLogicalFunction>(context->getText()));
    }
    else if (
        (helper.isWhereOrHaving || helper.isSelect || helper.isWindow) && parentRuleIndex
        && *parentRuleIndex == AntlrSQLParser::RulePrimaryExpression)
    {
        helper.functionBuilder.push_back(std::make_unique<FieldAccessLogicalFunction>(context->getText()));
    }
    else if (helper.isFrom && !helper.isJoinRelation && parentRuleIndex && *parentRuleIndex == AntlrSQLParser::RuleErrorCapturingIdentifier)
    {
        helper.setSource(context->getText());
    }
    else if (
        parentRuleIndex && *parentRuleIndex == AntlrSQLParser::RuleErrorCapturingIdentifier && !helper.isFunctionCall
        && !helper.isJoinRelation)
    {
        if (helper.isArithmeticBinary)
        {
            throw InvalidQuerySyntax("There must not be a binary arithmetic token at this point: {}", context->getText());
        }
        if ((helper.isWhereOrHaving || helper.isSelect))
        {
            auto attr = std::move(helper.functionBuilder.back());
            helper.functionBuilder.pop_back();
            if (helper.identCountHelper == 1)
            {
                helper.functionBuilder.push_back(std::make_unique<RenameLogicalFunction>(
                    NES::Util::unique_ptr_dynamic_cast<FieldAccessLogicalFunction>(std::move(attr)), context->getText()));
            }
            else
            {
                auto renamedAttribute = std::make_unique<FieldAssignmentLogicalFunction>(
                    std::make_unique<FieldAccessLogicalFunction>(context->getText()), std::move(attr));
                helper.functionBuilder.push_back(renamedAttribute->clone());
                helper.mapBuilder.push_back(std::move(renamedAttribute));
            }
        }
    }
    else if (helper.isFunctionCall && parentRuleIndex && *parentRuleIndex == AntlrSQLParser::RuleErrorCapturingIdentifier)
    {
        if (!helper.windowAggs.empty())
        {
            auto aggFunc = std::move(helper.windowAggs.back());
            helper.windowAggs.pop_back();
            aggFunc = aggFunc->as(std::make_unique<FieldAccessLogicalFunction>(context->getText()));
            helper.windowAggs.push_back(std::move(aggFunc));
        }
        else
        {
            auto projection = std::move(helper.functionBuilder.back());
            helper.functionBuilder.pop_back();
            auto renamedAttribute = std::make_unique<FieldAssignmentLogicalFunction>(
                std::make_unique<FieldAccessLogicalFunction>(context->getText()), std::move(projection));
            helper.functionBuilder.push_back(renamedAttribute->clone());
            helper.mapBuilder.push_back(std::move(renamedAttribute));
        }
    }
    else if (helper.isJoinRelation && parentRuleIndex && *parentRuleIndex == AntlrSQLParser::RulePrimaryExpression)
    {
        helper.joinKeyRelationHelper.push_back(std::make_unique<FieldAccessLogicalFunction>(context->getText()));
    }
    else if (helper.isJoinRelation && parentRuleIndex && *parentRuleIndex == AntlrSQLParser::RuleErrorCapturingIdentifier)
    {
        helper.joinSources.push_back(context->getText());
    }
    else if (helper.isJoinRelation && parentRuleIndex && *parentRuleIndex == AntlrSQLParser::RuleTableAlias)
    {
        helper.joinSourceRenames.push_back(context->getText());
    }
    AntlrSQLBaseListener::enterIdentifier(context);
}

void AntlrSQLQueryPlanCreator::enterPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    if (!helper.isFrom && !helper.isSetOperation)
    {
        throw InvalidQuerySyntax("Subqueries are only supported in FROM clauses, but got {}", context->getText());
    }
    // In this simplified design we use a single helper instance.
    AntlrSQLBaseListener::enterPrimaryQuery(context);
}

void AntlrSQLQueryPlanCreator::exitPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    QueryPlan queryPlan;
    if (!helper.queryPlans.empty())
    {
        queryPlan = std::move(helper.queryPlans[0]);
    }
    else
    {
        queryPlan = QueryPlanBuilder::createQueryPlan(helper.getSource());
    }
    for (auto whereExpr = helper.getWhereClauses().rbegin(); whereExpr != helper.getWhereClauses().rend(); ++whereExpr)
    {
        queryPlan = QueryPlanBuilder::addSelection(std::move(*whereExpr), queryPlan);
    }
    for (auto& mapExpr : helper.mapBuilder)
    {
        queryPlan = QueryPlanBuilder::addMap(std::move(mapExpr), queryPlan);
    }
    if (!helper.getProjectionFields().empty() && helper.windowType == nullptr)
    {
        queryPlan = QueryPlanBuilder::addProjection(std::move(helper.getProjectionFields()), std::move(queryPlan));
    }
    if (!helper.windowAggs.empty())
    {
        queryPlan = QueryPlanBuilder::addWindowAggregation(
            queryPlan, std::move(helper.windowType), std::move(helper.windowAggs), std::move(helper.groupByFields));
    }
    if (helper.windowType != nullptr)
    {
        for (auto havingExpr = helper.getHavingClauses().rbegin(); havingExpr != helper.getHavingClauses().rend(); ++havingExpr)
        {
            queryPlan = QueryPlanBuilder::addSelection(std::move(*havingExpr), queryPlan);
        }
    }
    if (!helper.isSetOperation)
    {
        queryPlans.push(queryPlan);
    }
    else
    {
        helper.queryPlans.push_back(queryPlan);
    }
    AntlrSQLBaseListener::exitPrimaryQuery(context);
}

void AntlrSQLQueryPlanCreator::enterWindowClause(AntlrSQLParser::WindowClauseContext* context)
{
    helper.isWindow = true;
    AntlrSQLBaseListener::enterWindowClause(context);
}

void AntlrSQLQueryPlanCreator::exitWindowClause(AntlrSQLParser::WindowClauseContext* context)
{
    helper.isWindow = false;
    AntlrSQLBaseListener::exitWindowClause(context);
}

void AntlrSQLQueryPlanCreator::enterTimeUnit(AntlrSQLParser::TimeUnitContext* context)
{
    std::optional<size_t> parentRuleIndex;
    if (const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }
    auto* token = context->getStop();
    auto timeunit = token->getType();
    if (parentRuleIndex && *parentRuleIndex == AntlrSQLParser::RuleAdvancebyParameter)
    {
        helper.timeUnitAdvanceBy = timeunit;
    }
    else
    {
        helper.timeUnit = timeunit;
    }
    AntlrSQLBaseListener::enterTimeUnit(context);
}

void AntlrSQLQueryPlanCreator::exitSizeParameter(AntlrSQLParser::SizeParameterContext* context)
{
    helper.size = std::stoi(context->children[1]->getText());
    AntlrSQLBaseListener::exitSizeParameter(context);
}

void AntlrSQLQueryPlanCreator::exitAdvancebyParameter(AntlrSQLParser::AdvancebyParameterContext* context)
{
    if (context->children.size() < 3)
    {
        throw InvalidQuerySyntax("AdvancebyParameter must have at least 3 children.");
    }
    helper.advanceBy = std::stoi(context->children[2]->getText());
    AntlrSQLBaseListener::exitAdvancebyParameter(context);
}

void AntlrSQLQueryPlanCreator::exitTimestampParameter(AntlrSQLParser::TimestampParameterContext* context)
{
    helper.timestamp = context->getText();
    AntlrSQLBaseListener::exitTimestampParameter(context);
}

void AntlrSQLQueryPlanCreator::exitTumblingWindow(AntlrSQLParser::TumblingWindowContext* context)
{
    const auto timeMeasure = buildTimeMeasure(helper.size, helper.timeUnit);
    if (helper.timestamp.empty())
    {
        helper.windowType = Windowing::TumblingWindow::of(API::IngestionTime(), timeMeasure);
    }
    else
    {
        helper.windowType = Windowing::TumblingWindow::of(
            Windowing::TimeCharacteristic::createEventTime(std::make_unique<FieldAccessLogicalFunction>(helper.timestamp)), timeMeasure);
    }
    AntlrSQLBaseListener::exitTumblingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitSlidingWindow(AntlrSQLParser::SlidingWindowContext* context)
{
    const auto timeMeasure = buildTimeMeasure(helper.size, helper.timeUnit);
    const auto slidingLength = buildTimeMeasure(helper.advanceBy, helper.timeUnitAdvanceBy);
    if (helper.timestamp.empty())
    {
        helper.windowType = Windowing::SlidingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(), timeMeasure, slidingLength);
    }
    else
    {
        helper.windowType = Windowing::SlidingWindow::of(
            Windowing::TimeCharacteristic::createEventTime(std::make_unique<FieldAccessLogicalFunction>(helper.timestamp)),
            timeMeasure,
            slidingLength);
    }
    AntlrSQLBaseListener::exitSlidingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitThresholdBasedWindow(AntlrSQLParser::ThresholdBasedWindowContext* context)
{
    if (helper.minimumCount.has_value())
    {
        helper.windowType = Windowing::ThresholdWindow::of(std::move(helper.functionBuilder.back()), helper.minimumCount.value());
    }
    else
    {
        helper.windowType = Windowing::ThresholdWindow::of(std::move(helper.functionBuilder.back()));
    }
    helper.isTimeBasedWindow = false;
    AntlrSQLBaseListener::exitThresholdBasedWindow(context);
}

void AntlrSQLQueryPlanCreator::enterNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    helper.identCountHelper = 0;
    AntlrSQLBaseListener::enterNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::exitNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    if (!helper.isFunctionCall && !helper.functionBuilder.empty() && helper.isSelect && helper.identCountHelper > 1
        && context->children.size() == 1)
    {
        std::string implicitFieldName;
        auto mapFunction = std::move(helper.functionBuilder.back());
        for (size_t countNodeFieldAccess = 0; auto& child : mapFunction->getChildren())
        {
            if (NES::Util::uniquePtrInstanceOf<LogicalFunction, FieldAccessLogicalFunction>(child))
            {
                auto fieldAccessNodePtr = NES::Util::unique_ptr_dynamic_cast<FieldAccessLogicalFunction>(child->clone());
                implicitFieldName = fmt::format("{}_{}", fieldAccessNodePtr->getFieldName(), helper.implicitMapCountHelper);
                ++countNodeFieldAccess;
                INVARIANT(
                    countNodeFieldAccess < 2, "The function of a named function must only have one child that is a field access function.");
            }
        }
        INVARIANT(!implicitFieldName.empty(), "");
        helper.functionBuilder.pop_back();
        helper.mapBuilder.push_back(std::make_unique<FieldAssignmentLogicalFunction>(
            std::make_unique<FieldAccessLogicalFunction>(implicitFieldName), std::move(mapFunction)));
        helper.implicitMapCountHelper++;
    }
    helper.isFunctionCall = false;
    AntlrSQLBaseListener::exitNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::enterFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    helper.isFunctionCall = true;
    helper.functionBuilder.clear();
    AntlrSQLBaseListener::enterFunctionCall(context);
}

void AntlrSQLQueryPlanCreator::enterHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helper.isWhereOrHaving = true;
    AntlrSQLBaseListener::enterHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helper.isWhereOrHaving = false;
    if (helper.functionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There was more than one function in the functionBuilder in exitWhereClause.");
    }
    helper.addHavingClause(std::move(helper.functionBuilder.back()));
    helper.functionBuilder.clear();
    AntlrSQLBaseListener::exitHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitComparison(AntlrSQLParser::ComparisonContext* context)
{
    if (helper.isJoinRelation)
    {
        INVARIANT(helper.joinKeyRelationHelper.size() >= 2, "Requires two functions but got {}", helper.joinKeyRelationHelper.size());
        auto rightFunction = std::move(helper.joinKeyRelationHelper.back());
        helper.joinKeyRelationHelper.pop_back();
        auto leftFunction = std::move(helper.joinKeyRelationHelper.back());
        helper.joinKeyRelationHelper.pop_back();
        auto function = createFunctionFromOpBoolean(std::move(leftFunction), std::move(rightFunction), helper.opBoolean);
        helper.joinKeyRelationHelper.push_back(function->clone());
        helper.joinFunction = std::move(function);
    }
    else
    {
        INVARIANT(helper.functionBuilder.size() >= 2, "Requires two functions");
        auto rightFunction = std::move(helper.functionBuilder.back());
        helper.functionBuilder.pop_back();
        auto leftFunction = std::move(helper.functionBuilder.back());
        helper.functionBuilder.pop_back();
        auto function = createFunctionFromOpBoolean(std::move(leftFunction), std::move(rightFunction), helper.opBoolean);
        helper.functionBuilder.push_back(std::move(function));
    }
    AntlrSQLBaseListener::exitComparison(context);
}

void AntlrSQLQueryPlanCreator::enterJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    helper.joinKeyRelationHelper.clear();
    helper.isJoinRelation = true;
    AntlrSQLBaseListener::enterJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::enterJoinCriteria(AntlrSQLParser::JoinCriteriaContext* context)
{
    INVARIANT(helper.isJoinRelation, "Join criteria must be inside a join relation.");
    AntlrSQLBaseListener::enterJoinCriteria(context);
}

void AntlrSQLQueryPlanCreator::enterJoinType(AntlrSQLParser::JoinTypeContext* context)
{
    if (!helper.isJoinRelation)
    {
        throw InvalidQuerySyntax("Join type must be inside a join relation.");
    }
    AntlrSQLBaseListener::enterJoinType(context);
}

void AntlrSQLQueryPlanCreator::exitJoinType(AntlrSQLParser::JoinTypeContext* context)
{
    const auto joinType = context->getText();
    auto tokenType = context->getStop()->getType();
    if (joinType.empty() || tokenType == AntlrSQLLexer::INNER)
    {
        helper.joinType = Join::LogicalJoinDescriptor::JoinType::INNER_JOIN;
    }
    else
    {
        throw InvalidQuerySyntax("Unknown join type: {}, resolved to token type: {}", joinType, tokenType);
    }
    AntlrSQLBaseListener::exitJoinType(context);
}

void AntlrSQLQueryPlanCreator::exitJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    helper.isJoinRelation = false;
    if (helper.joinSources.size() == helper.joinSourceRenames.size() + 1)
    {
        helper.joinSourceRenames.emplace_back("");
    }
    INVARIANT(helper.queryPlans.size() == 2, "Join relation requires exactly two subqueries, but got {}", helper.queryPlans.size());
    const auto leftQueryPlan = std::move(helper.queryPlans[0]);
    const auto rightQueryPlan = std::move(helper.queryPlans[1]);
    helper.queryPlans.clear();
    const auto queryPlan = QueryPlanBuilder::addJoin(
        leftQueryPlan, rightQueryPlan, std::move(helper.joinFunction), std::move(helper.windowType), std::move(helper.joinType));
    if (!helper.isSetOperation)
    {
        helper.queryPlans.push_back(queryPlan);
    }
    else
    {
        helper.queryPlans.push_back(queryPlan);
    }
    AntlrSQLBaseListener::exitJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::exitLogicalNot(AntlrSQLParser::LogicalNotContext* context)
{
    if (helper.isJoinRelation)
    {
        auto innerFunction = std::move(helper.joinKeyRelationHelper.back());
        helper.joinKeyRelationHelper.pop_back();
        auto negatedFunction = std::make_unique<NegateLogicalFunction>(std::move(helper.joinFunction));
        helper.joinKeyRelationHelper.push_back(negatedFunction->clone());
        helper.joinFunction = std::move(negatedFunction);
    }
    else
    {
        auto innerFunction = std::move(helper.functionBuilder.back());
        helper.functionBuilder.pop_back();
        helper.functionBuilder.push_back(std::make_unique<NegateLogicalFunction>(std::move(innerFunction)));
    }
    AntlrSQLBaseListener::exitLogicalNot(context);
}

void AntlrSQLQueryPlanCreator::exitConstantDefault(AntlrSQLParser::ConstantDefaultContext* context)
{
    if (const auto valueAsNumeric = dynamic_cast<AntlrSQLParser::NumericLiteralContext*>(context->constant()))
    {
        const auto concreteValue = valueAsNumeric->number();
        std::unique_ptr<DataType> dataType = nullptr;
        if (dynamic_cast<AntlrSQLParser::TinyIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::INT8);
        }
        else if (dynamic_cast<AntlrSQLParser::SmallIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::INT16);
        }
        else if (dynamic_cast<AntlrSQLParser::IntegerLiteralContext*>(concreteValue))
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::INT32);
        }
        else if (dynamic_cast<AntlrSQLParser::BigIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::INT64);
        }
        else if (dynamic_cast<AntlrSQLParser::UnsignedTinyIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::INT8);
        }
        else if (dynamic_cast<AntlrSQLParser::UnsignedSmallIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::INT16);
        }
        else if (dynamic_cast<AntlrSQLParser::UnsignedIntegerLiteralContext*>(concreteValue))
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::INT32);
        }
        else if (dynamic_cast<AntlrSQLParser::UnsignedBigIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::INT64);
        }
        else if (dynamic_cast<AntlrSQLParser::DoubleLiteralContext*>(concreteValue))
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::FLOAT64);
        }
        else if (dynamic_cast<AntlrSQLParser::FloatLiteralContext*>(concreteValue))
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::FLOAT32);
        }
        else
        {
            throw InvalidQuerySyntax("Unknown numerical data type: {}", concreteValue->getText());
        }
        const auto constantText = context->getText();
        helper.functionBuilder.push_back(
            std::make_unique<ConstantValueLogicalFunction>(std::move(dataType), constantText.substr(0, constantText.find('_'))));
    }
    else if (dynamic_cast<AntlrSQLParser::StringLiteralContext*>(context->constant()) != nullptr)
    {
        const auto constantText = std::string(NES::Util::trimCharacters(context->getText(), '\"'));
        auto dataType = DataTypeProvider::provideDataType(LogicalType::VARSIZED);
        auto constFunctionItem = std::make_unique<ConstantValueLogicalFunction>(std::move(dataType), constantText);
        helper.functionBuilder.push_back(std::move(constFunctionItem));
    }
    AntlrSQLBaseListener::exitConstantDefault(context);
}

void AntlrSQLQueryPlanCreator::exitFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    std::string funcName = NES::Util::toLowerCase(context->children[0]->getText());
    auto tokenType = context->getStart()->getType();
    switch (tokenType)
    {
        case AntlrSQLLexer::COUNT:
            helper.windowAggs.push_back(CountAggregationLogicalFunction::create(std::move(helper.functionBuilder.back())));
            break;
        case AntlrSQLLexer::AVG:
            helper.windowAggs.push_back(AvgAggregationLogicalFunction::create(std::move(helper.functionBuilder.back())));
            break;
        case AntlrSQLLexer::MAX:
            helper.windowAggs.push_back(MaxAggregationLogicalFunction::create(std::move(helper.functionBuilder.back())));
            break;
        case AntlrSQLLexer::MIN:
            helper.windowAggs.push_back(MinAggregationLogicalFunction::create(std::move(helper.functionBuilder.back())));
            break;
        case AntlrSQLLexer::SUM:
            helper.windowAggs.push_back(SumAggregationLogicalFunction::create(std::move(helper.functionBuilder.back())));
            break;
        case AntlrSQLLexer::MEDIAN:
            helper.windowAggs.push_back(MedianAggregationLogicalFunction::create(std::move(helper.functionBuilder.back())));
            break;
        default:
            if (funcName == "concat")
            {
                INVARIANT(helper.functionBuilder.size() == 2, "Concat requires two arguments, but got {}", helper.functionBuilder.size());
                auto rightFunction = std::move(helper.functionBuilder.back());
                helper.functionBuilder.pop_back();
                auto leftFunction = std::move(helper.functionBuilder.back());
                helper.functionBuilder.pop_back();

                parentHelper.functionBuilder.push_back(std::make_shared<ConcatLogicalFunction>(leftFunction, rightFunction));
            }
            else
            {
                throw InvalidQuerySyntax("Unknown aggregation function: {}, resolved to token type: {}", funcName, tokenType);
            }
    }
    AntlrSQLBaseListener::exitFunctionCall(context);
}

void AntlrSQLQueryPlanCreator::exitThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext* context)
{
    helper.minimumCount = std::stoi(context->getText());
    AntlrSQLBaseListener::exitThresholdMinSizeParameter(context);
}

void AntlrSQLQueryPlanCreator::enterValueExpressionDefault(AntlrSQLParser::ValueExpressionDefaultContext* context)
{
    helper.identCountHelper++;
    AntlrSQLBaseListener::enterValueExpressionDefault(context);
}

void AntlrSQLQueryPlanCreator::exitSetOperation(AntlrSQLParser::SetOperationContext* context)
{
    if (queryPlans.size() < 2)
    {
        throw InvalidQuerySyntax("Union does not have sufficient amount of QueryPlans for unifying.");
    }
    auto rightQuery = std::move(queryPlans.top());
    queryPlans.pop();
    auto leftQuery = std::move(queryPlans.top());
    queryPlans.pop();
    auto queryPlan = QueryPlanBuilder::addUnion(leftQuery, rightQuery);
    if (!helper.isSetOperation)
    {
        queryPlans.push(queryPlan);
    }
    else
    {
        helper.queryPlans.push_back(queryPlan);
    }
    AntlrSQLBaseListener::exitSetOperation(context);
}

void AntlrSQLQueryPlanCreator::enterAggregationClause(AntlrSQLParser::AggregationClauseContext* context)
{
    helper.isGroupBy = true;
    AntlrSQLBaseListener::enterAggregationClause(context);
}

void AntlrSQLQueryPlanCreator::exitAggregationClause(AntlrSQLParser::AggregationClauseContext* context)
{
    helper.isGroupBy = false;
    AntlrSQLBaseListener::exitAggregationClause(context);
}

}

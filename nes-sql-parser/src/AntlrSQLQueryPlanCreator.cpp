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
#include <Operators/Windows/JoinLogicalOperator.hpp>
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
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>

namespace NES::Parsers
{
std::string queryPlanToString(const std::shared_ptr<LogicalPlan>& queryPlan)
{
    const std::regex regex1("  ");
    const std::regex regex2("[0-9]");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), regex1, "");
    queryPlanStr = std::regex_replace(queryPlanStr, regex2, "");
    return queryPlanStr;
}

LogicalPlan AntlrSQLQueryPlanCreator::getQueryPlan() const
{
    /// Todo #421: support multiple sinks
    return LogicalPlanBuilder::addSink(std::move(sinkNames.front()), queryPlans.top());
}

Windowing::TimeMeasure buildTimeMeasure(const int size, const uint64_t timebase)
{
    switch (timebase) /// TODO #619: improve this switch case
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
        default:
            const AntlrSQLLexer lexer(nullptr);
            const std::string tokenName = std::string(lexer.getVocabulary().getSymbolicName(timebase));
            throw InvalidQuerySyntax("Unknown time unit: {}", tokenName);
    }
}

LogicalFunction createFunctionFromOpBoolean(
    LogicalFunction leftFunction, LogicalFunction rightFunction, const uint64_t tokenType)
{
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::EQ:
            return EqualsLogicalFunction(leftFunction, rightFunction);
        case AntlrSQLLexer::NEQJ:
            return NegateLogicalFunction(
                EqualsLogicalFunction(leftFunction, rightFunction));
        case AntlrSQLLexer::LT:
            return LessLogicalFunction(leftFunction, rightFunction);
        case AntlrSQLLexer::GT:
            return GreaterLogicalFunction(leftFunction, rightFunction);
        case AntlrSQLLexer::GTE:
            return GreaterEqualsLogicalFunction(leftFunction, rightFunction);
        case AntlrSQLLexer::LTE:
            return LessEqualsLogicalFunction(leftFunction, rightFunction);
        default:
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown Comparison Operator: {} of type: {}", lexer.getVocabulary().getSymbolicName(tokenType), tokenType);
    }
}

LogicalFunction createLogicalBinaryFunction(
    LogicalFunction leftFunction, LogicalFunction rightFunction, const uint64_t tokenType)
{
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::AND:
            return AndLogicalFunction(leftFunction, rightFunction);
        case AntlrSQLLexer::OR:
            return OrLogicalFunction(leftFunction, rightFunction);
        default:
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown binary function in SQL query for op {} with type: {} and left {} and right {}",
                lexer.getVocabulary().getSymbolicName(tokenType),
                tokenType,
                leftFunction,
                rightFunction);
    }
}

void AntlrSQLQueryPlanCreator::poppush(const AntlrSQLHelper& helper)
{
    helpers.pop();
    helpers.push(helper);
}

void AntlrSQLQueryPlanCreator::enterSelectClause(AntlrSQLParser::SelectClauseContext* context)
{
    helpers.top().isSelect = true;
    AntlrSQLBaseListener::enterSelectClause(context);
}

void AntlrSQLQueryPlanCreator::enterFromClause(AntlrSQLParser::FromClauseContext* context)
{
    helpers.top().isFrom = true;
    AntlrSQLBaseListener::enterFromClause(context);
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

void AntlrSQLQueryPlanCreator::exitLogicalBinary(AntlrSQLParser::LogicalBinaryContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    /// If we are exiting a logical binary operator in a join relation, we need to build the binary function for the joinKey and
    /// not for the general function
    if (helper.isJoinRelation)
    {
        const auto rightFunction = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();
        const auto leftFunction = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();

        const auto opTokenType = context->op->getType();
        const auto function = createLogicalBinaryFunction(leftFunction, rightFunction, opTokenType);
        helper.joinKeyRelationHelper.push_back(function);
        helper.joinFunction = function;
    }
    else
    {
        const auto rightFunction = helper.functionBuilder.back();
        helper.functionBuilder.pop_back();
        const auto leftFunction = helper.functionBuilder.back();
        helper.functionBuilder.pop_back();

        const auto opTokenType = context->op->getType();
        const auto function = createLogicalBinaryFunction(leftFunction, rightFunction, opTokenType);
        helper.functionBuilder.push_back(function);
    }

    poppush(helper);
}

void AntlrSQLQueryPlanCreator::exitSelectClause(AntlrSQLParser::SelectClauseContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    for (auto& selectFunction : helper.functionBuilder)
    {
        helper.addProjectionField(selectFunction);
    }
    helper.functionBuilder.clear();
    poppush(helper);
    helpers.top().isSelect = false;
    AntlrSQLBaseListener::exitSelectClause(context);
}

void AntlrSQLQueryPlanCreator::exitFromClause(AntlrSQLParser::FromClauseContext* context)
{
    helpers.top().isFrom = false;
    AntlrSQLBaseListener::exitFromClause(context);
}

void AntlrSQLQueryPlanCreator::enterWhereClause(AntlrSQLParser::WhereClauseContext* context)
{
    helpers.top().isWhereOrHaving = true;
    const AntlrSQLHelper helper = helpers.top();
    poppush(helper);
    AntlrSQLBaseListener::enterWhereClause(context);
}

void AntlrSQLQueryPlanCreator::exitWhereClause(AntlrSQLParser::WhereClauseContext* context)
{
    helpers.top().isWhereOrHaving = false;
    AntlrSQLHelper helper = helpers.top();
    if (helper.functionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There were more than 1 functions in the functionBuilder in exitWhereClause.");
    }
    helper.addWhereClause(helper.functionBuilder.back());
    helper.functionBuilder.clear();
    poppush(helper);
    AntlrSQLBaseListener::exitWhereClause(context);
}

void AntlrSQLQueryPlanCreator::enterComparisonOperator(AntlrSQLParser::ComparisonOperatorContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    auto opTokenType = context->getStart()->getType();
    helper.opBoolean = opTokenType;
    poppush(helper);
    AntlrSQLBaseListener::enterComparisonOperator(context);
}
void AntlrSQLQueryPlanCreator::exitArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext* context)
{
    auto helper = helpers.top();
    LogicalFunction function;

    const auto rightFunction = helper.functionBuilder.back();
    helper.functionBuilder.pop_back();
    const auto leftFunction = helper.functionBuilder.back();
    helper.functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::ASTERISK:
            function = MulLogicalFunction(std::move(leftFunction), std::move(rightFunction));
            break;
        case AntlrSQLLexer::SLASH:
            function = DivLogicalFunction(std::move(leftFunction), std::move(rightFunction));
            break;
        case AntlrSQLLexer::PLUS:
            function = AddLogicalFunction(std::move(leftFunction), std::move(rightFunction));
            break;
        case AntlrSQLLexer::MINUS:
            function = SubLogicalFunction(std::move(leftFunction), std::move(rightFunction));
            break;
        case AntlrSQLLexer::PERCENT:
            function = ModuloLogicalFunction(std::move(leftFunction), std::move(rightFunction));
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    helper.functionBuilder.push_back(function);
    poppush(helper);
}

void AntlrSQLQueryPlanCreator::exitArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    LogicalFunction function;

    const auto innerFunction = helper.functionBuilder.back();
    helper.functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::PLUS:
            function = innerFunction;
            break;
        case AntlrSQLLexer::MINUS:
            function = MulLogicalFunction(
                ConstantValueLogicalFunction(DataTypeProvider::provideBasicType(BasicType::UINT64), "-1"),
                innerFunction);
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    helper.functionBuilder.push_back(function);
    poppush(helper);
}

void AntlrSQLQueryPlanCreator::enterUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext* context)
{
    AntlrSQLHelper helper = helpers.top();

    /// Get Index of Parent Rule to check type of parent rule in conditions
    const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent);
    const bool isParentRuleTableAlias = (parentContext != nullptr) && parentContext->getRuleIndex() == AntlrSQLParser::RuleTableAlias;
    if (helper.isFrom && !helper.isJoinRelation)
    {
        helper.newSourceName = context->getText();
    }
    else if (helper.isJoinRelation && isParentRuleTableAlias)
    {
        helper.joinSourceRenames.emplace_back(context->getText());
    }
    poppush(helper);
    AntlrSQLBaseListener::enterUnquotedIdentifier(context);
}


void AntlrSQLQueryPlanCreator::enterIdentifier(AntlrSQLParser::IdentifierContext* context)
{
    AntlrSQLHelper helper = helpers.top();

    /// Get Index of Parent Rule to check type of parent rule in conditions
    std::optional<size_t> parentRuleIndex;
    if (const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }
    if (helper.isGroupBy)
    {
        helper.groupByFields.push_back(FieldAccessLogicalFunction(context->getText()));
    }
    else if ((helper.isWhereOrHaving || helper.isSelect || helper.isWindow) && AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helper.functionBuilder.push_back(FieldAccessLogicalFunction(context->getText()));
    }
    else if (helper.isFrom and not helper.isJoinRelation and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        /// get main source name
        helper.setSource(context->getText());
    }
    else if (AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex and not helper.isFunctionCall and not helper.isJoinRelation)
    {
        /// handle renames of identifiers
        if (helper.isArithmeticBinary)
        {
            throw InvalidQuerySyntax("There must not be a binary arithmetic token at this point: {}", context->getText());
        }
        if ((helper.isWhereOrHaving || helper.isSelect))
        {
            auto attr = helper.functionBuilder.back();
            helper.functionBuilder.pop_back();
            if (helper.identCountHelper == 1)
            {
                helper.functionBuilder.push_back(RenameLogicalFunction(attr.get<FieldAccessLogicalFunction>(), context->getText()));
            }
            else
            {
                auto renamedAttribute = FieldAssignmentLogicalFunction(FieldAccessLogicalFunction(context->getText()), attr);
                helper.functionBuilder.push_back(renamedAttribute);
                helper.mapBuilder.push_back(renamedAttribute);
            }
        }
    }
    else if (helper.isFunctionCall and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        if (!helper.windowAggs.empty())
        {
            auto aggFunc = helper.windowAggs.back();
            helper.windowAggs.pop_back();
            aggFunc->asField = FieldAccessLogicalFunction(context->getText());
            helper.windowAggs.push_back(std::move(aggFunc));
        }
        else
        {
            auto projection = helper.functionBuilder.back();
            helper.functionBuilder.pop_back();
            auto renamedAttribute = FieldAssignmentLogicalFunction(
                FieldAccessLogicalFunction(context->getText()),
                std::move(projection));
            helper.functionBuilder.push_back(renamedAttribute);
            helper.mapBuilder.push_back(renamedAttribute);
        }
    }
    else if (helper.isJoinRelation and AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helper.joinKeyRelationHelper.push_back(FieldAccessLogicalFunction(context->getText()));
    }
    else if (helper.isJoinRelation and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        helper.joinSources.push_back(context->getText());
    }
    else if (helper.isJoinRelation and AntlrSQLParser::RuleTableAlias == parentRuleIndex)
    {
        helper.joinSourceRenames.push_back(context->getText());
    }
    poppush(helper);
}

void AntlrSQLQueryPlanCreator::enterPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    if (!helpers.empty() && !helpers.top().isFrom && !helpers.top().isSetOperation)
    {
        throw InvalidQuerySyntax("Subqueries are only supported in FROM clauses, but got {}", context->getText());
    }

    AntlrSQLHelper helper;

    /// Get Index of  Parent Rule to check type of parent rule in conditions
    const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent);
    std::optional<size_t> parentRuleIndex;
    if (parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }

    /// PrimaryQuery is a queryterm too, but if it's a child of a queryterm we are in a union!
    if (parentRuleIndex == AntlrSQLParser::RuleQueryTerm)
    {
        helper.isSetOperation = true;
    }

    helpers.push(helper);
    AntlrSQLBaseListener::enterPrimaryQuery(context);
}
void AntlrSQLQueryPlanCreator::exitPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    LogicalPlan queryPlan;

    if (!helper.queryPlans.empty())
    {
        queryPlan = std::move(helper.queryPlans[0]);
    }
    else
    {
        queryPlan = LogicalPlanBuilder::createLogicalPlan(helper.getSource());
    }
    for (auto whereExpr = helper.getWhereClauses().rbegin(); whereExpr != helper.getWhereClauses().rend(); ++whereExpr)
    {
        queryPlan = LogicalPlanBuilder::addSelection(std::move(*whereExpr), queryPlan);
    }

    for (auto& mapExpr : helper.mapBuilder)
    {
        queryPlan = LogicalPlanBuilder::addMap(mapExpr, queryPlan);
    }
    /// We handle projections AFTER map functions, because:
    /// SELECT (id * 3) as new_id FROM ...
    ///     we project on new_id, but new_id is the result of an function, so we need to execute the function before projecting.
    if (!helper.getProjectionFields().empty() && helper.windowType == nullptr)
    {
        queryPlan = LogicalPlanBuilder::addProjection(std::move(helper.getProjectionFields()), queryPlan);
    }
    if (not helper.windowAggs.empty())
    {
        queryPlan = LogicalPlanBuilder::addWindowAggregation(queryPlan, std::move(helper.windowType), std::move(helper.windowAggs), std::move(helper.groupByFields));
    }

    if (helper.windowType != nullptr)
    {
        for (auto havingExpr = helper.getHavingClauses().rbegin(); havingExpr != helper.getHavingClauses().rend(); ++havingExpr)
        {
            queryPlan = LogicalPlanBuilder::addSelection(std::move(*havingExpr), queryPlan);
        }
    }
    helpers.pop();
    if (helpers.empty() || helper.isSetOperation)
    {
        queryPlans.push(queryPlan);
    }
    else
    {
        AntlrSQLHelper subQueryHelper = helpers.top();
        subQueryHelper.queryPlans.push_back(queryPlan);
        poppush(subQueryHelper);
    }
    AntlrSQLBaseListener::exitPrimaryQuery(context);
}
void AntlrSQLQueryPlanCreator::enterWindowClause(AntlrSQLParser::WindowClauseContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.isWindow = true;
    poppush(helper);
    AntlrSQLBaseListener::enterWindowClause(context);
}
void AntlrSQLQueryPlanCreator::exitWindowClause(AntlrSQLParser::WindowClauseContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.isWindow = false;
    poppush(helper);
    AntlrSQLBaseListener::exitWindowClause(context);
}

void AntlrSQLQueryPlanCreator::enterTimeUnit(AntlrSQLParser::TimeUnitContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    /// Get Index of Parent Rule to check type of parent rule in conditions
    std::optional<size_t> parentRuleIndex;
    if (const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }

    auto* token = context->getStop();
    auto timeunit = token->getType();
    if (parentRuleIndex == AntlrSQLParser::RuleAdvancebyParameter)
    {
        helper.timeUnitAdvanceBy = timeunit;
    }
    else
    {
        helper.timeUnit = timeunit;
    }

    poppush(helper);
}

void AntlrSQLQueryPlanCreator::exitSizeParameter(AntlrSQLParser::SizeParameterContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.size = std::stoi(context->children[1]->getText());
    poppush(helper);

    AntlrSQLBaseListener::exitSizeParameter(context);
}

void AntlrSQLQueryPlanCreator::exitAdvancebyParameter(AntlrSQLParser::AdvancebyParameterContext* context)
{
    PRECONDITION(
        context->children.size() >= 3, "AdvancebyParameter must have at least 3 children, as we expect a number, 'BY' and a time unit.");
    AntlrSQLHelper helper = helpers.top();
    helper.advanceBy = std::stoi(context->children[2]->getText());
    poppush(helper);
    AntlrSQLBaseListener::exitAdvancebyParameter(context);
}

void AntlrSQLQueryPlanCreator::exitTimestampParameter(AntlrSQLParser::TimestampParameterContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.timestamp = context->getText();
    poppush(helper);
}

/// WINDOWS
void AntlrSQLQueryPlanCreator::exitTumblingWindow(AntlrSQLParser::TumblingWindowContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    const auto timeMeasure = buildTimeMeasure(helper.size, helper.timeUnit);
    /// We use the ingestion time if the query does not have a timestamp fieldname specified
    if (helper.timestamp.empty())
    {
        helper.windowType = std::make_shared<Windowing::TumblingWindow>(API::IngestionTime(), timeMeasure);
    }
    else
    {
        helper.windowType = std::make_shared<Windowing::TumblingWindow>(
            Windowing::TimeCharacteristic::createEventTime(
                FieldAccessLogicalFunction(helper.timestamp)),
            timeMeasure);
    }
    poppush(helper);
    AntlrSQLBaseListener::exitTumblingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitSlidingWindow(AntlrSQLParser::SlidingWindowContext* context)
{
    auto helper = helpers.top();
    const auto timeMeasure = buildTimeMeasure(helper.size, helper.timeUnit);
    const auto slidingLength = buildTimeMeasure(helper.advanceBy, helper.timeUnitAdvanceBy);
    /// We use the ingestion time if the query does not have a timestamp fieldname specified
    if (helper.timestamp.empty())
    {
        helper.windowType = Windowing::SlidingWindow::of(API::IngestionTime(), timeMeasure, slidingLength);
    }
    else
    {
        helper.windowType = Windowing::SlidingWindow::of(
            Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction(helper.timestamp)), timeMeasure, slidingLength);
    }
    poppush(helper);
    AntlrSQLBaseListener::exitSlidingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitThresholdBasedWindow(AntlrSQLParser::ThresholdBasedWindowContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    if (helper.minimumCount.has_value())
    {
        helper.windowType = std::make_shared<Windowing::ThresholdWindow>(std::move(helper.functionBuilder.back()),
                                                           helper.minimumCount.value());
    }
    else
    {
        helper.windowType = std::make_shared<Windowing::ThresholdWindow>(std::move(helper.functionBuilder.back()));
    }
    //helper.isTimeBasedWindow = false;
    poppush(helper);
    AntlrSQLBaseListener::exitThresholdBasedWindow(context);
}

void AntlrSQLQueryPlanCreator::enterNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.identCountHelper = 0;
    poppush(helper);
    AntlrSQLBaseListener::enterNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::exitNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    /// handle implicit maps when no "AS" is supplied, but a rename is needed
    if (!helper.isFunctionCall && !helper.functionBuilder.empty() && helper.isSelect && helper.identCountHelper > 1
        && context->children.size() == 1)
    {
        std::string implicitFieldName;
        auto mapFunction = std::move(helper.functionBuilder.back());
        for (size_t countNodeFieldAccess = 0; auto& child : mapFunction.getChildren())
        {
            if (child.tryGet<FieldAccessLogicalFunction>())
            {
                auto fieldAccessNodePtr = child.get<FieldAccessLogicalFunction>();
                implicitFieldName = fmt::format("{}_{}", fieldAccessNodePtr.getFieldName(), helper.implicitMapCountHelper);
                ++countNodeFieldAccess;
                INVARIANT(
                    countNodeFieldAccess < 2, "The function of a named function must only have one child that is a field access function.");
            }
        }
        INVARIANT(not implicitFieldName.empty(), "");
        helper.functionBuilder.pop_back();
        helper.mapBuilder.push_back(
            FieldAssignmentLogicalFunction(FieldAccessLogicalFunction(implicitFieldName), mapFunction));
        helper.implicitMapCountHelper++;
    }
    helper.isFunctionCall = false;
    poppush(helper);

    AntlrSQLBaseListener::exitNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::enterFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    helpers.top().isFunctionCall = true;
    AntlrSQLHelper helper = helpers.top();
    helper.functionBuilder.clear();
    helper.isFunctionCall = true;
    helpers.push(helper);
    AntlrSQLBaseListener::enterFunctionCall(context);
}

void AntlrSQLQueryPlanCreator::enterHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helpers.top().isWhereOrHaving = true;
    const AntlrSQLHelper helper = helpers.top();
    poppush(helper);
    AntlrSQLBaseListener::enterHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helpers.top().isWhereOrHaving = false;
    AntlrSQLHelper helper = helpers.top();
    if (helper.functionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There was more than one function in the functionBuilder in exitWhereClause.");
    }
    helper.addHavingClause(helper.functionBuilder.back());
    helper.functionBuilder.clear();
    poppush(helper);
    AntlrSQLBaseListener::exitHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitComparison(AntlrSQLParser::ComparisonContext* context)
{
    if (auto helper = helpers.top(); helper.isJoinRelation)
    {
        INVARIANT(helper.joinKeyRelationHelper.size() >= 2, "Requires two functions but got {}", helper.joinKeyRelationHelper.size());
        const auto rightFunction = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();
        const auto leftFunction = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();
        const auto function = createFunctionFromOpBoolean(leftFunction, rightFunction, helper.opBoolean);
        helper.joinKeyRelationHelper.push_back(function);
        helper.joinFunction = function;
        poppush(helper);
    }
    else
    {
        INVARIANT(helper.functionBuilder.size() >= 2, "Requires two functions");
        const auto rightFunction = helper.functionBuilder.back();
        helper.functionBuilder.pop_back();
        const auto leftFunction = helper.functionBuilder.back();
        helper.functionBuilder.pop_back();

        const auto function = createFunctionFromOpBoolean(leftFunction, rightFunction, helper.opBoolean);
        helper.functionBuilder.push_back(function);
        poppush(helper);
    }
    AntlrSQLBaseListener::exitComparison(context);
}

void AntlrSQLQueryPlanCreator::enterJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    auto helper = helpers.top();
    helper.joinKeyRelationHelper.clear();
    helper.isJoinRelation = true;
    poppush(helper);
    AntlrSQLBaseListener::enterJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::enterJoinCriteria(AntlrSQLParser::JoinCriteriaContext* context)
{
    const auto helper = helpers.top();
    INVARIANT(helper.isJoinRelation, "Join criteria must be inside a join relation.");
    AntlrSQLBaseListener::enterJoinCriteria(context);
}

void AntlrSQLQueryPlanCreator::enterJoinType(AntlrSQLParser::JoinTypeContext* context)
{
    if (const auto helper = helpers.top(); not helper.isJoinRelation)
    {
        throw InvalidQuerySyntax("Join type must be inside a join relation.");
    }
    AntlrSQLBaseListener::enterJoinType(context);
}

void AntlrSQLQueryPlanCreator::exitJoinType(AntlrSQLParser::JoinTypeContext* context)
{
    auto helper = helpers.top();
    const auto joinType = context->getText();
    auto tokenType = context->getStop()->getType();

    if (joinType.empty() || tokenType == AntlrSQLLexer::INNER)
    {
        helper.joinType = JoinLogicalOperator::JoinType::INNER_JOIN;
    }
    else
    {
        throw InvalidQuerySyntax("Unknown join type: {}, resolved to token type: {}", joinType, tokenType);
    }
    poppush(helper);
    AntlrSQLBaseListener::exitJoinType(context);
}

void AntlrSQLQueryPlanCreator::exitJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    auto helper = helpers.top();
    helper.isJoinRelation = false;
    if (helper.joinSources.size() == helper.joinSourceRenames.size() + 1)
    {
        helper.joinSourceRenames.emplace_back("");
    }

    /// we assume that the left query plan is the first element in the queryPlans vector and the right query plan is the second element
    INVARIANT(helper.queryPlans.size() == 2, "Join relation requires exactly two subqueries, but got {}", helper.queryPlans.size());
    const auto leftQueryPlan = helper.queryPlans[0];
    const auto rightQueryPlan = helper.queryPlans[1];
    helper.queryPlans.clear();

    auto queryPlan
        = LogicalPlanBuilder::addJoin(leftQueryPlan, rightQueryPlan, helper.joinFunction.value(), std::move(helper.windowType), std::move(helper.joinType));
    if (not helpers.empty())
    {
        /// we are in a subquery
        helper.queryPlans.push_back(queryPlan);
        poppush(helper);
    }
    else
    {
        /// for now, we will never enter this branch, because we always have a subquery
        /// as we require the join relations to always be a sub-query
        queryPlans.push(queryPlan);
    }

    poppush(helper);
    AntlrSQLBaseListener::exitJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::exitLogicalNot(AntlrSQLParser::LogicalNotContext* context)
{
    AntlrSQLHelper helper = helpers.top();

    if (helper.isJoinRelation)
    {
        const auto innerFunction = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();
        auto negatedFunction = NegateLogicalFunction(helper.joinFunction.value());
        helper.joinKeyRelationHelper.push_back(negatedFunction);
        helper.joinFunction = negatedFunction;
    }
    else
    {
        const auto innerFunction = helper.functionBuilder.back();
        helper.functionBuilder.pop_back();
        helper.functionBuilder.push_back(NegateLogicalFunction(innerFunction));
    }
    poppush(helper);
    AntlrSQLBaseListener::exitLogicalNot(context);
}

void AntlrSQLQueryPlanCreator::exitConstantDefault(AntlrSQLParser::ConstantDefaultContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    if (const auto valueAsNumeric = dynamic_cast<AntlrSQLParser::NumericLiteralContext*>(context->constant()))
    {
        const auto concreteValue = valueAsNumeric->number();
        std::shared_ptr<DataType> dataType = nullptr;
        /// Signed Integers
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

        /// Unsigned Integers
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

        /// Floating Point
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
        /// Getting the constant value without the type,e .g., 42.0_D, 42.0_F, 42_U or 42_I --> 42.0, 42.0, 42, 42
        const auto constantText = context->getText();
        helper.functionBuilder.push_back(
            ConstantValueLogicalFunction(std::move(dataType), constantText.substr(0, constantText.find('_'))));
    }
    else if (dynamic_cast<AntlrSQLParser::StringLiteralContext*>(context->constant()) != nullptr)
    {
        const auto constantText = std::string(NES::Util::trimCharacters(context->getText(), '\"'));
        auto dataType = DataTypeProvider::provideDataType(LogicalType::VARSIZED);
        auto constFunctionItem = ConstantValueLogicalFunction(std::move(dataType), constantText);
        helper.functionBuilder.push_back(constFunctionItem);
    }

    poppush(helper);
}

void AntlrSQLQueryPlanCreator::exitFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helpers.pop();
    AntlrSQLHelper& parentHelper = helpers.top();

    const auto funcName = Util::toLowerCase(context->children[0]->getText());
    auto tokenType = context->getStart()->getType();

    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::COUNT:
            parentHelper.windowAggs.push_back(CountAggregationLogicalFunction::create(helper.functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::AVG:
            parentHelper.windowAggs.push_back(AvgAggregationLogicalFunction::create(helper.functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::MAX:
            parentHelper.windowAggs.push_back(MaxAggregationLogicalFunction::create(helper.functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::MIN:
            parentHelper.windowAggs.push_back(MinAggregationLogicalFunction::create(helper.functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::SUM:
            parentHelper.windowAggs.push_back(SumAggregationLogicalFunction::create(helper.functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::MEDIAN:
            parentHelper.windowAggs.push_back(MedianAggregationLogicalFunction::create(helper.functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        default:
            if (funcName == "concat")
            {
                INVARIANT(helper.functionBuilder.size() == 2, "Concat requires two arguments, but got {}", helper.functionBuilder.size());
                const auto rightFunction = helper.functionBuilder.back();
                helper.functionBuilder.pop_back();
                const auto leftFunction = helper.functionBuilder.back();
                helper.functionBuilder.pop_back();
                parentHelper.functionBuilder.push_back(ConcatLogicalFunction(leftFunction, rightFunction));
            }
            else
            {
                throw InvalidQuerySyntax("Unknown aggregation function: {}, resolved to token type: {}", funcName, tokenType);
            }
    }
}

void AntlrSQLQueryPlanCreator::exitThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.minimumCount = std::stoi(context->getText());
    poppush(helper);
}

void AntlrSQLQueryPlanCreator::enterValueExpressionDefault(AntlrSQLParser::ValueExpressionDefaultContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.identCountHelper++;
    poppush(helper);
    AntlrSQLBaseListener::enterValueExpressionDefault(context);
}

void AntlrSQLQueryPlanCreator::exitSetOperation(AntlrSQLParser::SetOperationContext* context)
{
    if (queryPlans.size() < 2)
    {
        throw InvalidQuerySyntax("Union does not have sufficient amount of QueryPlans for unifying.");
    }

    const auto rightQuery = queryPlans.top();
    queryPlans.pop();
    const auto leftQuery = queryPlans.top();
    queryPlans.pop();
    const auto queryPlan = LogicalPlanBuilder::addUnion(leftQuery, rightQuery);
    if (!helpers.empty())
    {
        /// we are in a subquery
        auto helper = helpers.top();
        helper.queryPlans.push_back(queryPlan);
        poppush(helper);
    }
    else
    {
        queryPlans.push(queryPlan);
    }
    AntlrSQLBaseListener::exitSetOperation(context);
}

void AntlrSQLQueryPlanCreator::enterAggregationClause(AntlrSQLParser::AggregationClauseContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.isGroupBy = true;
    poppush(helper);
    AntlrSQLBaseListener::enterAggregationClause(context);
}

void AntlrSQLQueryPlanCreator::exitAggregationClause(AntlrSQLParser::AggregationClauseContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.isGroupBy = false;
    poppush(helper);
    AntlrSQLBaseListener::exitAggregationClause(context);
}
}
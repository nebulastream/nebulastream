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

#include <algorithm>
#include <ranges>
#include <regex>
#include <string>

#include <Functions/LogicalFunctions/NodeFunctionGreater.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreaterEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLess.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLessEquals.hpp>

#include <AntlrSQLParser.h>
#include <API/Functions/ArithmeticalFunctions.hpp>
#include <API/Functions/Functions.hpp>
#include <API/Functions/LogicalFunctions.hpp>
#include <API/Query.hpp>
#include <API/Windowing.hpp>
#include <AntlrSQLParser/AntlrSQLHelper.hpp>
#include <AntlrSQLParser/AntlrSQLQueryPlanCreator.hpp>
#include <Functions/LogicalFunctions/NodeFunctionAnd.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Functions/LogicalFunctions/NodeFunctionOr.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Types/TumblingWindow.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <ParserRuleContext.h>

namespace NES::Parsers
{
std::string queryPlanToString(const QueryPlanPtr& queryPlan)
{
    const std::regex r2("[0-9]");
    const std::regex r1("  ");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r1, "");
    queryPlanStr = std::regex_replace(queryPlanStr, r2, "");
    return queryPlanStr;
}

QueryPlanPtr AntlrSQLQueryPlanCreator::getQueryPlan() const
{
    /// Todo #421: support multiple sinks
    return QueryPlanBuilder::addSink(std::move(sinkNames.front()), queryPlans.top());
}

Windowing::TimeMeasure buildTimeMeasure(const int size, const std::string& timebase)
{
    if (timebase == "MS" || timebase == "ms")
    {
        return API::Milliseconds(size);
    }
    if (timebase == "SEC" || timebase == "sec")
    {
        return API::Seconds(size);
    }
    if (timebase == "MIN" || timebase == "min")
    {
        return API::Minutes(size);
    }
    if (timebase == "HOUR" || timebase == "hour" || timebase == "HOURS" || timebase == "hours")
    {
        return API::Hours(size);
    }
    if (timebase == "DAY" || timebase == "day" || timebase == "DAYS" || timebase == "days")
    {
        return API::Days(size);
    }

    throw InvalidQuerySyntax("Unknown time unit: {}", timebase);
}

std::shared_ptr<NodeFunction> createFunctionFromOpBoolean(
    const std::shared_ptr<NodeFunction>& leftFunction, const std::shared_ptr<NodeFunction>& rightFunction, const std::string& opStr)
{
    if (opStr == "==" || opStr == "=")
    {
        return NodeFunctionEquals::create(leftFunction, rightFunction);
    }
    if (opStr == "!=")
    {
        const auto equalsExpression = NodeFunctionEquals::create(leftFunction, rightFunction);
        return NodeFunctionNegate::create(equalsExpression);
    }
    if (opStr == "<")
    {
        return NodeFunctionLess::create(leftFunction, rightFunction);
    }
    if (opStr == ">")
    {
        return NodeFunctionGreater::create(leftFunction, rightFunction);
    }
    if (opStr == ">=")
    {
        return NodeFunctionGreaterEquals::create(leftFunction, rightFunction);
    }
    if (opStr == "<=")
    {
        return NodeFunctionLessEquals::create(leftFunction, rightFunction);
    }

    throw InvalidQuerySyntax("Unknown Comparison Operator: {}", opStr);
}

std::shared_ptr<NodeFunction> createLogicalBinaryFunction(
    const std::shared_ptr<NodeFunction>& leftFunction, const std::shared_ptr<NodeFunction>& rightFunction, const std::string& opStr)
{
    if (opStr == "AND" or opStr == "and")
    {
        return NodeFunctionAnd::create(leftFunction, rightFunction);
    }
    if (opStr == "OR" or opStr == "or")
    {
        return NodeFunctionOr::create(leftFunction, rightFunction);
    }

    throw InvalidQuerySyntax(
        "Unknown binary function in SQL query for op {} and left {} and right {}",
        opStr,
        leftFunction->toString(),
        rightFunction->toString());
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


void AntlrSQLQueryPlanCreator::enterNamedExpressionSeq(AntlrSQLParser::NamedExpressionSeqContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.expressionBuilder.clear();
    poppush(helper);
    AntlrSQLBaseListener::enterNamedExpressionSeq(context);
}

void AntlrSQLQueryPlanCreator::exitLogicalBinary(AntlrSQLParser::LogicalBinaryContext* context)
{
    AntlrSQLHelper helper = helpers.top();

    if (not helper.isJoinRelation)
    {
        const auto rightExpression = helper.expressionBuilder.back();
        helper.expressionBuilder.pop_back();
        const auto leftExpression = helper.expressionBuilder.back();
        helper.expressionBuilder.pop_back();

        const std::string opText = context->op->getText();
        const auto function = createLogicalBinaryFunction(leftExpression, rightExpression, opText);
        helper.expressionBuilder.push_back(function);
    }
    else
    {
        const auto rightExpression = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();
        const auto leftExpression = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();

        const std::string opText = context->op->getText();
        const auto function = createLogicalBinaryFunction(leftExpression, rightExpression, opText);
        helper.joinKeyRelationHelper.push_back(function);
        helper.joinFunction = function;
    }

    poppush(helper);
}

void AntlrSQLQueryPlanCreator::exitSelectClause(AntlrSQLParser::SelectClauseContext* context)
{
    NodeFunctionPtr expressionNode;
    AntlrSQLHelper helper = helpers.top();
    for (const NodeFunctionPtr& selectExpression : helper.expressionBuilder)
    {
        helper.addProjectionField(selectExpression);
    }
    helper.expressionBuilder.clear();
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
    if (helper.expressionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There were more than 1 functions in the expressionBuilder in exitWhereClause.");
    }
    helper.addWhereClause(helper.expressionBuilder.back());
    helper.expressionBuilder.clear();
    poppush(helper);
    AntlrSQLBaseListener::exitWhereClause(context);
}

void AntlrSQLQueryPlanCreator::enterComparisonOperator(AntlrSQLParser::ComparisonOperatorContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.opBoolean = context->getText();
    poppush(helper);
    AntlrSQLBaseListener::enterComparisonOperator(context);
}
void AntlrSQLQueryPlanCreator::exitArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext* context)
{
    auto helper = helpers.top();
    NodeFunctionPtr expression;

    const auto rightExpression = helper.expressionBuilder.back();
    helper.expressionBuilder.pop_back();
    const auto leftExpression = helper.expressionBuilder.back();
    helper.expressionBuilder.pop_back();

    if (const std::string opText = context->op->getText(); opText == "*")
    {
        expression = leftExpression * rightExpression;
    }
    else if (opText == "/")
    {
        expression = leftExpression / rightExpression;
    }
    else if (opText == "+")
    {
        expression = leftExpression + rightExpression;
    }
    else if (opText == "-")
    {
        expression = leftExpression - rightExpression;
    }
    else if (opText == "%")
    {
        expression = leftExpression % rightExpression;
    }
    else
    {
        throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {}", opText);
    }
    helper.expressionBuilder.push_back(expression);
    poppush(helper);
}

void AntlrSQLQueryPlanCreator::exitArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    NodeFunctionPtr expression;

    const auto innerExpression = helper.expressionBuilder.back();
    helper.expressionBuilder.pop_back();

    if (const std::string opText = context->op->getText(); opText == "+")
    {
        expression = innerExpression;
    }
    else if (opText == "-")
    {
        expression = -1 * innerExpression;
    }
    else
    {
        throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {}", opText);
    }
    helper.expressionBuilder.push_back(expression);
    poppush(helper);
}

void AntlrSQLQueryPlanCreator::enterUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext* context)
{
    AntlrSQLHelper helper = helpers.top();

    /// Get Index of Parent Rule to check type of parent rule in conditions
    const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent);
    std::optional<size_t> parentRuleIndex;
    if (parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }

    if (helper.isFrom && !helper.isJoinRelation)
    {
        helper.newSourceName = context->getText();
    }
    else if (helper.isJoinRelation && parentRuleIndex == AntlrSQLParser::RuleTableAlias)
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
        const auto key = Util::as<NodeFunctionFieldAccess>(NodeFunctionFieldAccess::create(context->getText()));
        helper.groupByFields.push_back(key);
    }
    else if ((helper.isWhereOrHaving || helper.isSelect || helper.isWindow) && AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        /// add identifiers in select, window, where and having clauses to the expression builder list
        helper.expressionBuilder.push_back(Attribute(context->getText()));
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
            const NodeFunctionPtr attr = helper.expressionBuilder.back();
            helper.expressionBuilder.pop_back();
            if (helper.identCountHelper == 1)
            {
                /// rename of a single attribute
                auto cattr = static_cast<FunctionItem>(attr);
                cattr = cattr.as(context->getText());
                helper.expressionBuilder.push_back(cattr);
            }
            else
            {
                /// renaming an expression (mapBuilder) and adding a projection (expressionBuilder) on the renamed expression.
                const auto renamedAttribute = Attribute(context->getText()) = attr;
                helper.expressionBuilder.push_back(renamedAttribute);
                helper.mapBuilder.push_back(renamedAttribute);
            }
        }
    }
    else if (helper.isFunctionCall and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        auto aggFunc = helper.windowAggs.back();
        helper.windowAggs.pop_back();
        aggFunc = aggFunc->as(Attribute(context->getText()));
        helper.windowAggs.push_back(aggFunc);
    }
    else if (helper.isJoinRelation and AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helper.joinKeyRelationHelper.push_back(Attribute(context->getText()));
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
    const auto parentContext = static_cast<antlr4::ParserRuleContext*>(context->parent);
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
    QueryPlanPtr queryPlan;

    if (!helper.queryPlans.empty())
    {
        queryPlan = helper.queryPlans[0];
    }
    else
    {
        queryPlan = QueryPlanBuilder::createQueryPlan(helper.getSource());
    }
    if (!helper.newSourceName.empty() && helper.newSourceName != helper.getSource())
    {
        queryPlan = QueryPlanBuilder::addRename(helper.newSourceName, queryPlan);
    }

    for (auto& whereExpr : helper.getWhereClauses())
    {
        queryPlan = QueryPlanBuilder::addFilter(whereExpr, queryPlan);
    }

    for (const auto& mapExpr : helper.mapBuilder)
    {
        queryPlan = QueryPlanBuilder::addMap(mapExpr, queryPlan);
    }
    /// We handle projections AFTER map expressions, because:
    /// SELECT (id * 3) as new_id FROM ...
    ///     we project on new_id, but new_id is the result of an expression, so we need to execute the expression before projecting.
    if (!helper.getProjectionFields().empty() && helper.windowType == nullptr)
    {
        queryPlan = QueryPlanBuilder::addProjection(helper.getProjectionFields(), queryPlan);
    }

    if (helper.windowType != nullptr)
    {
        for (auto& havingExpr : helper.getHavingClauses())
        {
            queryPlan = QueryPlanBuilder::addFilter(havingExpr, queryPlan);
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

    if (parentRuleIndex == AntlrSQLParser::RuleAdvancebyParameter)
    {
        helper.timeUnitAdvanceBy = context->getText();
    }
    else
    {
        helper.timeUnit = context->getText();
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
    const Windowing::TimeMeasure timeMeasure = buildTimeMeasure(helper.size, helper.timeUnit);
    /// We use the ingestion time if no timestamp is specified
    if (helper.timestamp.empty())
    {
        helper.windowType = Windowing::TumblingWindow::of(API::IngestionTime(), timeMeasure);
    }
    else
    {
        helper.windowType = Windowing::TumblingWindow::of(API::EventTime(Attribute(helper.timestamp)), timeMeasure);
    }
    poppush(helper);
    AntlrSQLBaseListener::exitTumblingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitSlidingWindow(AntlrSQLParser::SlidingWindowContext* context)
{
    auto helper = helpers.top();
    const auto timeMeasure = buildTimeMeasure(helper.size, helper.timeUnit);
    const auto slidingLength = buildTimeMeasure(helper.advanceBy, helper.timeUnitAdvanceBy);
    /// We use the ingestion time if no timestamp is specified
    if (helper.timestamp.empty())
    {
        helper.windowType = Windowing::SlidingWindow::of(API::IngestionTime(), timeMeasure, slidingLength);
    }
    else
    {
        helper.windowType = Windowing::SlidingWindow::of(API::EventTime(Attribute(helper.timestamp)), timeMeasure, slidingLength);
    }
    poppush(helper);
    AntlrSQLBaseListener::exitSlidingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitThresholdBasedWindow(AntlrSQLParser::ThresholdBasedWindowContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    if (helper.minimumCount != -1)
    {
        helper.windowType = Windowing::ThresholdWindow::of(helper.expressionBuilder.back(), helper.minimumCount);
    }
    else
    {
        helper.windowType = Windowing::ThresholdWindow::of(helper.expressionBuilder.back());
    }
    helper.isTimeBasedWindow = false;
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
    if (!helper.isFunctionCall && !helper.expressionBuilder.empty() && helper.isSelect && helper.identCountHelper > 1
        && context->children.size() == 1)
    {
        std::string implicitFieldName;
        const NodeFunctionPtr mapExpression = helper.expressionBuilder.back();
        /// there must be a field access function node in mapExpression.
        for (size_t i = 0; const auto& child : mapExpression->getChildren())
        {
            if (NES::Util::instanceOf<NodeFunctionFieldAccess>(child))
            {
                const auto fieldAccessNodePtr = NES::Util::as<NodeFunctionFieldAccess>(child);
                implicitFieldName = fmt::format("{}_{}", fieldAccessNodePtr->getFieldName(), helper.implicitMapCountHelper);
                INVARIANT(++i < 2, "The expression of a named expression must only have one child that is a field access expression.")
            }
        }
        INVARIANT(not implicitFieldName.empty(), "")
        helper.expressionBuilder.pop_back();
        helper.mapBuilder.push_back(Attribute(implicitFieldName) = mapExpression);

        helper.implicitMapCountHelper++;
    }
    helper.isFunctionCall = false;
    poppush(helper);

    AntlrSQLBaseListener::exitNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::enterFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.isFunctionCall = true;
    poppush(helper);
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
    if (helper.expressionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There was more than one function in the expressionBuilder in exitWhereClause.");
    }
    helper.addHavingClause(helper.expressionBuilder.back());
    helper.expressionBuilder.clear();
    poppush(helper);
    AntlrSQLBaseListener::exitHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitComparison(AntlrSQLParser::ComparisonContext* context)
{
    if (auto helper = helpers.top(); not helper.isJoinRelation)
    {
        INVARIANT(helper.expressionBuilder.size() >= 2, "Requires two expressions")
        const auto rightExpression = helper.expressionBuilder.back();
        helper.expressionBuilder.pop_back();
        const auto leftExpression = helper.expressionBuilder.back();
        helper.expressionBuilder.pop_back();

        const auto function = createFunctionFromOpBoolean(leftExpression, rightExpression, helper.opBoolean);
        helper.expressionBuilder.push_back(function);
        poppush(helper);
    }
    else
    {
        INVARIANT(helper.joinKeyRelationHelper.size() >= 2, "Requires two expressions but got {}", helper.joinKeyRelationHelper.size());
        const auto rightExpression = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();
        const auto leftExpression = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();
        const auto function = createFunctionFromOpBoolean(leftExpression, rightExpression, helper.opBoolean);
        helper.joinKeyRelationHelper.push_back(function);
        helper.joinFunction = function;
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
    if (const auto helper = helpers.top(); not helper.isJoinRelation)
    {
        throw InvalidQuerySyntax("Join type must be inside a join relation.");
    }
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
    if (const auto joinType = context->getText(); joinType == "INNER" || joinType == "inner" || joinType.empty())
    {
        helper.joinType = Join::LogicalJoinDescriptor::JoinType::INNER_JOIN;
    }
    else
    {
        throw InvalidQuerySyntax("Unknown join type: {}", joinType);
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

    const auto queryPlan
        = QueryPlanBuilder::addJoin(leftQueryPlan, rightQueryPlan, helper.joinFunction, helper.windowType, helper.joinType);
    if (!helpers.empty())
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

    if (not helper.isJoinRelation)
    {
        const auto innerExpression = helper.expressionBuilder.back();
        helper.expressionBuilder.pop_back();
        const auto expression = !innerExpression;
        helper.expressionBuilder.push_back(expression);
    }
    else
    {
        const auto innerFunction = helper.joinKeyRelationHelper.back();
        helper.joinKeyRelationHelper.pop_back();
        const auto negatedFunction = !helper.joinFunction;
        helper.joinKeyRelationHelper.push_back(negatedFunction);
        helper.joinFunction = negatedFunction;
    }
    poppush(helper);
    AntlrSQLBaseListener::exitLogicalNot(context);
}

void AntlrSQLQueryPlanCreator::exitConstantDefault(AntlrSQLParser::ConstantDefaultContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    std::shared_ptr<DataType> dataType;
    if (const auto valueAsNumeric = dynamic_cast<AntlrSQLParser::NumericLiteralContext*>(context->constant()))
    {
        const auto concreteValue = valueAsNumeric->number();
        /// Signed Integers
        if (dynamic_cast<AntlrSQLParser::TinyIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeFactory::createInt8();
        }
        else if (dynamic_cast<AntlrSQLParser::SmallIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeFactory::createInt16();
        }
        else if (dynamic_cast<AntlrSQLParser::IntegerLiteralContext*>(concreteValue))
        {
            dataType = DataTypeFactory::createInt32();
        }
        else if (dynamic_cast<AntlrSQLParser::BigIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeFactory::createInt64();
        }

        /// Unsigned Integers
        else if (dynamic_cast<AntlrSQLParser::UnsignedTinyIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeFactory::createInt8();
        }
        else if (dynamic_cast<AntlrSQLParser::UnsignedSmallIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeFactory::createInt16();
        }
        else if (dynamic_cast<AntlrSQLParser::UnsignedIntegerLiteralContext*>(concreteValue))
        {
            dataType = DataTypeFactory::createInt32();
        }
        else if (dynamic_cast<AntlrSQLParser::UnsignedBigIntLiteralContext*>(concreteValue))
        {
            dataType = DataTypeFactory::createInt64();
        }

        /// Floating Point
        else if (dynamic_cast<AntlrSQLParser::DoubleLiteralContext*>(concreteValue))
        {
            dataType = DataTypeFactory::createDouble();
        }
        else if (dynamic_cast<AntlrSQLParser::FloatLiteralContext*>(concreteValue))
        {
            dataType = DataTypeFactory::createFloat();
        }
        else
        {
            throw InvalidQuerySyntax("Unknown data type: {}", concreteValue->getText());
        }
    }
    const auto valueType = std::make_shared<BasicValue>(dataType, context->getText());
    auto constFunctionItem = FunctionItem(NodeFunctionConstantValue::create(valueType));
    helper.expressionBuilder.push_back(constFunctionItem);
    poppush(helper);
}

void AntlrSQLQueryPlanCreator::exitFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    auto funcName = context->children[0]->getText();
    for (char& c : funcName)
    {
        c = std::tolower(static_cast<unsigned char>(c));
    }
    if (funcName == "count")
    {
        helper.windowAggs.push_back(API::Count()->aggregation);
    }
    else if (funcName == "avg")
    {
        helper.windowAggs.push_back(API::Avg(helper.expressionBuilder.back())->aggregation);
        helper.expressionBuilder.pop_back();
    }
    else if (funcName == "max")
    {
        helper.windowAggs.push_back(API::Max(helper.expressionBuilder.back())->aggregation);
        helper.expressionBuilder.pop_back();
    }
    else if (funcName == "min")
    {
        helper.windowAggs.push_back(API::Min(helper.expressionBuilder.back())->aggregation);
        helper.expressionBuilder.pop_back();
    }
    else if (funcName == "sum")
    {
        helper.windowAggs.push_back(API::Sum(helper.expressionBuilder.back())->aggregation);
        helper.expressionBuilder.pop_back();
    }
    else if (funcName == "median")
    {
        helper.windowAggs.push_back(API::Median(helper.expressionBuilder.back())->aggregation);
        helper.expressionBuilder.pop_back();
    }
    else
    {
        throw InvalidQuerySyntax("Unknown aggregation function: {}", funcName);
    }
    poppush(helper);
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
    const auto queryPlan = QueryPlanBuilder::addUnion(leftQuery, rightQuery);
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

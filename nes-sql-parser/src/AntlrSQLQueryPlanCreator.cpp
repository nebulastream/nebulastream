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
#include <ranges>
#include <regex>
#include <string>
#include <utility>
#include <fmt/format.h>

#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <ParserRuleContext.h>
#include <API/Functions/ArithmeticalFunctions.hpp>
#include <API/Functions/Functions.hpp>
#include <API/Functions/LogicalFunctions.hpp>
#include <API/Windowing.hpp>
#include <AntlrSQLParser/AntlrSQLHelper.hpp>
#include <AntlrSQLParser/AntlrSQLQueryPlanCreator.hpp>
#include <Functions/LogicalFunctions/NodeFunctionAnd.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreater.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreaterEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLess.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLessEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Functions/LogicalFunctions/NodeFunctionOr.hpp>
#include <Functions/NodeFunctionConcat.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Types/TumblingWindow.hpp>
#include <Util/Common.hpp>
#include <Util/Ranges.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::Parsers
{
std::string queryPlanToString(const std::shared_ptr<QueryPlan>& queryPlan)
{
    const std::regex regex1("  ");
    const std::regex regex2("[0-9]");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), regex1, "");
    queryPlanStr = std::regex_replace(queryPlanStr, regex2, "");
    return queryPlanStr;
}

std::shared_ptr<QueryPlan> AntlrSQLQueryPlanCreator::getQueryPlan() const
{
    /// Todo #421: support multiple sinks
    return QueryPlanBuilder::addSink(std::move(sinkNames.front()), queryPlans.top());
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

std::shared_ptr<NodeFunction> createFunctionFromOpBoolean(
    const std::shared_ptr<NodeFunction>& leftFunction, const std::shared_ptr<NodeFunction>& rightFunction, const uint64_t tokenType)
{
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::EQ:
            return NodeFunctionEquals::create(leftFunction, rightFunction);
        case AntlrSQLLexer::NEQJ:
            return NodeFunctionNegate::create(NodeFunctionEquals::create(leftFunction, rightFunction));
        case AntlrSQLLexer::LT:
            return NodeFunctionLess::create(leftFunction, rightFunction);
        case AntlrSQLLexer::GT:
            return NodeFunctionGreater::create(leftFunction, rightFunction);
        case AntlrSQLLexer::GTE:
            return NodeFunctionGreaterEquals::create(leftFunction, rightFunction);
        case AntlrSQLLexer::LTE:
            return NodeFunctionLessEquals::create(leftFunction, rightFunction);
        default:
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown Comparison Operator: {} of type: {}", lexer.getVocabulary().getSymbolicName(tokenType), tokenType);
    }
}

std::shared_ptr<NodeFunction> createLogicalBinaryFunction(
    const std::shared_ptr<NodeFunction>& leftFunction, const std::shared_ptr<NodeFunction>& rightFunction, const uint64_t tokenType)
{
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::AND:
            return NodeFunctionAnd::create(leftFunction, rightFunction);
        case AntlrSQLLexer::OR:
            return NodeFunctionOr::create(leftFunction, rightFunction);
        default:
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown binary function in SQL query for op {} with type: {} and left {} and right {}",
                lexer.getVocabulary().getSymbolicName(tokenType),
                tokenType,
                *leftFunction,
                *rightFunction);
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

void AntlrSQLQueryPlanCreator::enterNamedExpressionSeq(AntlrSQLParser::NamedExpressionSeqContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.functionBuilder.clear();
    poppush(helper);
    AntlrSQLBaseListener::enterNamedExpressionSeq(context);
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
    std::shared_ptr<NodeFunction> function;

    if (helper.functionBuilder.size() < 2)
    {
        if (helper.functionBuilder.size() + helper.constantBuilder.size() == 2)
        {
            throw InvalidQuerySyntax(
                "Attempted to use a raw constant in a binary expression. {} in `{}`.",
                fmt::join(helper.constantBuilder, ", "),
                context->getText());
        }
        throw InvalidQuerySyntax(
            "There were less than 2 functions in the functionBuilder in exitArithmeticBinary. `{}`.", context->getText());
    }
    const auto rightFunction = helper.functionBuilder.back();
    helper.functionBuilder.pop_back();
    const auto leftFunction = helper.functionBuilder.back();
    helper.functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::ASTERISK:
            function = leftFunction * rightFunction;
            break;
        case AntlrSQLLexer::SLASH:
            function = leftFunction / rightFunction;
            break;
        case AntlrSQLLexer::PLUS:
            function = leftFunction + rightFunction;
            break;
        case AntlrSQLLexer::MINUS:
            function = leftFunction - rightFunction;
            break;
        case AntlrSQLLexer::PERCENT:
            function = leftFunction % rightFunction;
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
    std::shared_ptr<NodeFunction> function;

    const auto innerFunction = helper.functionBuilder.back();
    helper.functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::PLUS:
            function = innerFunction;
            break;
        case AntlrSQLLexer::MINUS:
            function = -1 * innerFunction;
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
    if (const auto* const parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parentContext != nullptr)
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
        helper.functionBuilder.push_back(Attribute(context->getText()));
    }
    else if (helper.isFrom and not helper.isJoinRelation and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        /// get main source name
        helper.setSource(context->getText());
    }
    else if (AntlrSQLParser::RuleNamedExpression == parentRuleIndex and helper.isInFunctionCall() and not helper.isJoinRelation)
    {
        /// handle renames of identifiers
        if (helper.isArithmeticBinary)
        {
            throw InvalidQuerySyntax("There must not be a binary arithmetic token at this point: {}", context->getText());
        }
        if ((helper.isWhereOrHaving || helper.isSelect))
        {
            /// The user specified named expression (field access or function) with 'AS THE_NAME'
            /// (we handle cases where the user did not specify a name via 'AS' in 'exitNamedExpression')
            const auto attribute = helper.functionBuilder.back();
            helper.functionBuilder.pop_back();
            const auto renamedAttribute = Attribute(context->getText()) = attribute;
            helper.addProjectionField(renamedAttribute->getField());
            helper.mapBuilder.push_back(renamedAttribute);
        }
    }
    else if (helper.isInAggFunction() and AntlrSQLParser::RuleNamedExpression == parentRuleIndex)
    {
        if (not helper.windowAggs.empty())
        {
            auto aggFunc = helper.windowAggs.back();
            helper.windowAggs.pop_back();
            aggFunc = aggFunc->as(Attribute(context->getText()));
            helper.windowAggs.push_back(aggFunc);
        }
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
    std::shared_ptr<QueryPlan> queryPlan;

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

    for (auto whereExpr = helper.getWhereClauses().rbegin(); whereExpr != helper.getWhereClauses().rend(); ++whereExpr)
    {
        queryPlan = QueryPlanBuilder::addSelection(*whereExpr, queryPlan);
    }

    for (const auto& mapExpr : helper.mapBuilder)
    {
        queryPlan = QueryPlanBuilder::addMap(mapExpr, queryPlan);
    }
    /// We handle projections AFTER map functions, because:
    /// SELECT (id * 3) as new_id FROM ...
    ///     we project on new_id, but new_id is the result of an function, so we need to execute the function before projecting.
    if (not helper.getProjectionFields().empty() && helper.windowType == nullptr)
    {
        queryPlan = QueryPlanBuilder::addProjection(helper.getProjectionFields(), queryPlan);
    }
    if (not helper.windowAggs.empty())
    {
        queryPlan = QueryPlanBuilder::addWindowAggregation(queryPlan, helper.windowType, helper.windowAggs, helper.groupByFields);
    }

    if (helper.windowType != nullptr)
    {
        for (auto havingExpr = helper.getHavingClauses().rbegin(); havingExpr != helper.getHavingClauses().rend(); ++havingExpr)
        {
            queryPlan = QueryPlanBuilder::addSelection(*havingExpr, queryPlan);
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
    /// We use the ingestion time if the query does not have a timestamp fieldname specified
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
    if (helper.minimumCount.has_value())
    {
        helper.windowType = Windowing::ThresholdWindow::of(helper.functionBuilder.back(), helper.minimumCount.value());
    }
    else
    {
        helper.windowType = Windowing::ThresholdWindow::of(helper.functionBuilder.back());
    }
    helper.isTimeBasedWindow = false;
    poppush(helper);
    AntlrSQLBaseListener::exitThresholdBasedWindow(context);
}

void AntlrSQLQueryPlanCreator::enterNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    poppush(helper);
    AntlrSQLBaseListener::enterNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::exitNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    /// If the current functions consist of a single field access, the user simply specified a field/attribute to access
    if (helper.functionBuilder.size() == 1 and Util::instanceOf<NodeFunctionFieldAccess>(helper.functionBuilder.back()))
    {
        /// Project onto the specified field and remove the field access from the active functions.
        helper.addProjectionField(Util::as<NodeFunctionFieldAccess>(helper.functionBuilder.back()));
        helper.functionBuilder.pop_back();
    }
    /// The user either specified a '*', in which case the functionBuilder should be empty, or a function on the attribute
    /// (e.g., SELECT id + 2 ...). If the user did not specify a name (... AS THE_NAME), we need to generate a name.
    else if (context->name == nullptr and not helper.functionBuilder.empty())
    {
        const std::shared_ptr<NodeFunction> mapFunction = helper.functionBuilder.back();
        const auto fieldAccessFunctions = mapFunction->getChildren()
            | std::views::transform([](auto& child) { return Util::as_if<NodeFunctionFieldAccess>(child); })
            | std::ranges::to<std::vector>();

        if (std::ranges::count_if(fieldAccessFunctions, [](const auto& fieldAccessNode) { return fieldAccessNode.get() != nullptr; }) != 1)
        {
            throw InvalidQuerySyntax("A named function must have exactly one valid FieldAccessNode child.");
        }
        const auto implicitFieldName = fmt::format("{}_{}", fieldAccessFunctions.front()->getFieldName(), helper.implicitMapCountHelper++);
        const auto mapFunctionWithFieldAssignment = Attribute(implicitFieldName) = mapFunction;
        helper.mapBuilder.push_back(mapFunctionWithFieldAssignment);
        /// Projections always follow map functions. Thus, we need to project on the field assigned by the map function.
        helper.addProjectionField(mapFunctionWithFieldAssignment->getField());
        helper.functionBuilder.pop_back();
    }
    poppush(helper);
    AntlrSQLBaseListener::exitNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::enterFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.functionBuilder.clear();
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
        helper.joinType = Join::LogicalJoinDescriptor::JoinType::INNER_JOIN;
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

    const auto queryPlan
        = QueryPlanBuilder::addJoin(leftQueryPlan, rightQueryPlan, helper.joinFunction, helper.windowType, helper.joinType);
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
        const auto negatedFunction = !helper.joinFunction;
        helper.joinKeyRelationHelper.push_back(negatedFunction);
        helper.joinFunction = negatedFunction;
    }
    else
    {
        const auto innerFunction = helper.functionBuilder.back();
        helper.functionBuilder.pop_back();
        const auto function = !innerFunction;
        helper.functionBuilder.push_back(function);
    }
    poppush(helper);
    AntlrSQLBaseListener::exitLogicalNot(context);
}

void AntlrSQLQueryPlanCreator::exitConstantDefault(AntlrSQLParser::ConstantDefaultContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    INVARIANT(context->children.size() == 1, "When exiting a constant, there must be exactly one children in the context");
    if (const auto stringLiteralContext = dynamic_cast<AntlrSQLParser::StringLiteralContext*>(context->children.at(0)))
    {
        INVARIANT(
            stringLiteralContext->getText().size() > 2,
            "A constant string literal must contain at least two quotes and must not be empty.");
        helper.constantBuilder.push_back(context->getText().substr(1, stringLiteralContext->getText().size() - 2));
    }
    else
    {
        helper.constantBuilder.push_back(context->getText());
    }
    poppush(helper);
}

void AntlrSQLQueryPlanCreator::exitFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helpers.pop();
    AntlrSQLHelper& parentHelper = helpers.top();

    const auto funcName = Util::toUpperCase(context->children[0]->getText());
    const auto tokenType = context->getStart()->getType();

    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::COUNT:
            parentHelper.windowAggs.push_back(API::Count(helper.functionBuilder.back())->aggregation);
            break;
        case AntlrSQLLexer::AVG:
            parentHelper.windowAggs.push_back(API::Avg(helper.functionBuilder.back())->aggregation);
            break;
        case AntlrSQLLexer::MAX:
            parentHelper.windowAggs.push_back(API::Max(helper.functionBuilder.back())->aggregation);
            break;
        case AntlrSQLLexer::MIN:
            parentHelper.windowAggs.push_back(API::Min(helper.functionBuilder.back())->aggregation);
            break;
        case AntlrSQLLexer::SUM:
            parentHelper.windowAggs.push_back(API::Sum(helper.functionBuilder.back())->aggregation);
            break;
        case AntlrSQLLexer::MEDIAN:
            parentHelper.windowAggs.push_back(API::Median(helper.functionBuilder.back())->aggregation);
            break;
        default:
            /// Check if the function is a constructor for a datatype
            if (const auto dataType = DataTypeProvider::tryProvideDataType(funcName); dataType.has_value())
            {
                auto value = std::move(helper.constantBuilder.back());
                helper.constantBuilder.pop_back();
                auto constFunctionItem = FunctionItem(NES::NodeFunctionConstantValue::create(*dataType, std::move(value)));
                parentHelper.functionBuilder.push_back(constFunctionItem);
                break;
            }
            if (funcName == "CONCAT")
            {
                INVARIANT(helper.functionBuilder.size() == 2, "Concat requires two arguments, but got {}", helper.functionBuilder.size());
                const auto rightFunction = helper.functionBuilder.back();
                helper.functionBuilder.pop_back();
                const auto leftFunction = helper.functionBuilder.back();
                helper.functionBuilder.pop_back();

                parentHelper.functionBuilder.push_back(NodeFunctionConcat::create(leftFunction, rightFunction));
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

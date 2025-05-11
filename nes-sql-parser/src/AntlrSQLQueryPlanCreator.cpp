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

#include <AntlrSQLParser/AntlrSQLQueryPlanCreator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <AntlrSQLBaseListener.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <ParserRuleContext.h>
#include <AntlrSQLParser/AntlrSQLHelper.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConcatLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Operators/Windows/Aggregations/ArrayAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MedianAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MinAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Util/Strings.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <ParserUtil.hpp>

namespace NES::Parsers
{

LogicalPlan AntlrSQLQueryPlanCreator::getQueryPlan() const
{
    if (sinkNames.empty())
    {
        throw InvalidQuerySyntax("Query does not contain sink");
    }
    if (queryPlans.empty())
    {
        throw InvalidQuerySyntax("Query could not be parsed");
    }
    /// Todo #421: support multiple sinks
    INVARIANT(!sinkNames.empty(), "Need at least one sink!");
    return LogicalPlanBuilder::addSink(sinkNames.front(), queryPlans.top());
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

static LogicalFunction createFunctionFromOpBoolean(LogicalFunction leftFunction, LogicalFunction rightFunction, const uint64_t tokenType)
{
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::EQ:
            return EqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::NEQJ:
            return NegateLogicalFunction(EqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction)));
        case AntlrSQLLexer::LT:
            return LessLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::GT:
            return GreaterLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::GTE:
            return GreaterEqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::LTE:
            return LessEqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        default:
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown Comparison Operator: {} of type: {}", lexer.getVocabulary().getSymbolicName(tokenType), tokenType);
    }
}

static LogicalFunction createLogicalBinaryFunction(LogicalFunction leftFunction, LogicalFunction rightFunction, const uint64_t tokenType)
{
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::AND:
            return AndLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::OR:
            return OrLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        default:
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown binary function in SQL query for op {} with type: {} and left {} and right {}",
                lexer.getVocabulary().getSymbolicName(tokenType),
                tokenType,
                std::move(leftFunction),
                std::move(rightFunction));
    }
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
    /// If we are exiting a logical binary operator in a join relation, we need to build the binary function for the joinKey and
    /// not for the general function
    if (helpers.top().isJoinRelation)
    {
        if (helpers.top().joinKeyRelationHelper.size() < 2)
        {
            throw InvalidQuerySyntax(
                "Expected two operands for binary op, got {}: {}", helpers.top().joinKeyRelationHelper.size(), context->getText());
        }
        const auto rightFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        const auto leftFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();

        const auto opTokenType = context->op->getType();
        const auto function = createLogicalBinaryFunction(leftFunction, rightFunction, opTokenType);
        helpers.top().joinKeyRelationHelper.push_back(function);
        helpers.top().joinFunction = function;
    }
    else
    {
        if (helpers.top().functionBuilder.size() < 2)
        {
            throw InvalidQuerySyntax(
                "Expected two operands for binary op, got {}: {}", helpers.top().joinKeyRelationHelper.size(), context->getText());
        }
        const auto rightFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        const auto leftFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();

        const auto opTokenType = context->op->getType();
        const auto function = createLogicalBinaryFunction(leftFunction, rightFunction, opTokenType);
        helpers.top().functionBuilder.push_back(function);
    }
}

void AntlrSQLQueryPlanCreator::exitSelectClause(AntlrSQLParser::SelectClauseContext* context)
{
    helpers.top().functionBuilder.clear();
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
    AntlrSQLBaseListener::enterWhereClause(context);
}

void AntlrSQLQueryPlanCreator::exitWhereClause(AntlrSQLParser::WhereClauseContext* context)
{
    helpers.top().isWhereOrHaving = false;
    if (helpers.top().functionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There were more than 1 functions in the functionBuilder in exitWhereClause.");
    }
    helpers.top().addWhereClause(helpers.top().functionBuilder.back());
    helpers.top().functionBuilder.clear();
    AntlrSQLBaseListener::exitWhereClause(context);
}

void AntlrSQLQueryPlanCreator::enterComparisonOperator(AntlrSQLParser::ComparisonOperatorContext* context)
{
    auto opTokenType = context->getStart()->getType();
    helpers.top().opBoolean = opTokenType;
    AntlrSQLBaseListener::enterComparisonOperator(context);
}
void AntlrSQLQueryPlanCreator::exitArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext* context)
{
    if (helpers.empty())
    {
        throw InvalidQuerySyntax("Parser is confused at {}", context->getText());
    }
    LogicalFunction function;

    if (helpers.top().functionBuilder.size() < 2)
    {
        if (helpers.top().functionBuilder.size() + helpers.top().constantBuilder.size() == 2)
        {
            throw InvalidQuerySyntax(
                "Attempted to use a raw constant in a binary expression. {} in `{}`.",
                fmt::join(helpers.top().constantBuilder, ", "),
                context->getText());
        }
        throw InvalidQuerySyntax(
            "There were less than 2 functions in the functionBuilder in exitArithmeticBinary. `{}`.", context->getText());
    }
    const auto rightFunction = helpers.top().functionBuilder.back();
    helpers.top().functionBuilder.pop_back();
    const auto leftFunction = helpers.top().functionBuilder.back();
    helpers.top().functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::ASTERISK:
            function = MulLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::SLASH:
            function = DivLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::PLUS:
            function = AddLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::MINUS:
            function = SubLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::PERCENT:
            function = ModuloLogicalFunction(leftFunction, rightFunction);
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    helpers.top().functionBuilder.push_back(function);
}

void AntlrSQLQueryPlanCreator::exitArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext* context)
{
    if (helpers.empty())
    {
        throw InvalidQuerySyntax("Parser is confused at {}", context->getText());
    }
    LogicalFunction function;

    if (helpers.top().functionBuilder.empty())
    {
        throw InvalidQuerySyntax("Expected unary operator, got nothing: {}", context->getText());
    }
    const auto innerFunction = helpers.top().functionBuilder.back();
    helpers.top().functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::PLUS:
            function = innerFunction;
            break;
        case AntlrSQLLexer::MINUS:
            function = MulLogicalFunction(
                ConstantValueLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::UINT64), "-1"), innerFunction);
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    helpers.top().functionBuilder.push_back(function);
}

void AntlrSQLQueryPlanCreator::enterUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext* context)
{
    /// Get Index of Parent Rule to check type of parent rule in conditions
    const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent);
    const bool isParentRuleTableAlias = (parentContext != nullptr) && parentContext->getRuleIndex() == AntlrSQLParser::RuleTableAlias;
    if (helpers.top().isFrom && !helpers.top().isJoinRelation)
    {
        helpers.top().newSourceName = context->getText();
    }
    else if (helpers.top().isJoinRelation && isParentRuleTableAlias)
    {
        helpers.top().joinSourceRenames.emplace_back(context->getText());
    }
    AntlrSQLBaseListener::enterUnquotedIdentifier(context);
}


void AntlrSQLQueryPlanCreator::enterIdentifier(AntlrSQLParser::IdentifierContext* context)
{
    /// Get Index of Parent Rule to check type of parent rule in conditions
    std::optional<size_t> parentRuleIndex;
    if (const auto* const parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }
    if (helpers.top().isGroupBy)
    {
        helpers.top().groupByFields.emplace_back(context->getText());
    }
    else if (
        (helpers.top().isWhereOrHaving || helpers.top().isSelect || helpers.top().isWindow)
        && AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helpers.top().functionBuilder.emplace_back(FieldAccessLogicalFunction(context->getText()));
    }
    else if (helpers.top().isFrom and not helpers.top().isJoinRelation and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        /// get main source name
        helpers.top().setSource(context->getText());
    }
    else if (
        AntlrSQLParser::RuleNamedExpression == parentRuleIndex and helpers.top().isInFunctionCall() and not helpers.top().isJoinRelation
        and not helpers.top().isInAggFunction())
    {
        /// handle renames of identifiers
        if (helpers.top().isArithmeticBinary)
        {
            throw InvalidQuerySyntax("There must not be a binary arithmetic token at this point: {}", context->getText());
        }
        if ((helpers.top().isWhereOrHaving || helpers.top().isSelect))
        {
            /// The user specified named expression (field access or function) with 'AS THE_NAME'
            /// (we handle cases where the user did not specify a name via 'AS' in 'exitNamedExpression')
            const auto attribute = helpers.top().functionBuilder.back();
            helpers.top().functionBuilder.pop_back();
            auto renamedAttribute = FieldAssignmentLogicalFunction(FieldAccessLogicalFunction(context->getText()), attribute);
            helpers.top().addProjectionField(renamedAttribute.getField());
            helpers.top().mapBuilder.push_back(renamedAttribute);
        }
    }
    else if (helpers.top().isInAggFunction() and AntlrSQLParser::RuleNamedExpression == parentRuleIndex)
    {
        auto aggFunc = helpers.top().windowAggs.back();
        helpers.top().windowAggs.pop_back();
        aggFunc->asField = (FieldAccessLogicalFunction(context->getText()));
        helpers.top().windowAggs.push_back(aggFunc);
        INVARIANT(
            std::nullopt != helpers.top().functionBuilder.back().tryGet<FieldAccessLogicalFunction>(),
            "The functionBuilder should hold the AccessFunction of the name of the field the aggregation is executed on.");
        helpers.top().functionBuilder.pop_back();
        helpers.top().hasUnnamedAggregation = false;
        helpers.top().addProjectionField(aggFunc->asField);
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helpers.top().joinKeyRelationHelper.emplace_back(FieldAccessLogicalFunction(context->getText()));
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        helpers.top().joinSources.push_back(context->getText());
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RuleTableAlias == parentRuleIndex)
    {
        helpers.top().joinSourceRenames.push_back(context->getText());
    }
}

void AntlrSQLQueryPlanCreator::enterPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    if (not helpers.empty() and not helpers.top().isFrom and not helpers.top().isSetOperation)
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
    LogicalPlan queryPlan;

    if (not helpers.top().queryPlans.empty())
    {
        queryPlan = std::move(helpers.top().queryPlans[0]);
    }
    else
    {
        queryPlan = LogicalPlanBuilder::createLogicalPlan(helpers.top().getSource());
    }

    for (auto whereExpr = helpers.top().getWhereClauses().rbegin(); whereExpr != helpers.top().getWhereClauses().rend(); ++whereExpr)
    {
        queryPlan = LogicalPlanBuilder::addSelection(std::move(*whereExpr), queryPlan);
    }

    if (helpers.top().isInAggFunction())
    {
        queryPlan = LogicalPlanBuilder::addWindowAggregation(
            queryPlan, helpers.top().windowType, helpers.top().windowAggs, helpers.top().groupByFields);
    }
    for (const auto& mapExpr : helpers.top().mapBuilder)
    {
        queryPlan = LogicalPlanBuilder::addMap(mapExpr, queryPlan);
    }
    /// We handle projections AFTER map functions, because:
    /// SELECT (id * 3) as new_id FROM ...
    ///     we project on new_id, but new_id is the result of an function, so we need to execute the function before projecting.
    if (not helpers.top().getProjectionFields().empty())
    {
        queryPlan = LogicalPlanBuilder::addProjection(std::move(helpers.top().getProjectionFields()), queryPlan);
    }
    if (helpers.top().windowType != nullptr)
    {
        for (auto havingExpr = helpers.top().getHavingClauses().rbegin(); havingExpr != helpers.top().getHavingClauses().rend();
             ++havingExpr)
        {
            queryPlan = LogicalPlanBuilder::addSelection(*havingExpr, queryPlan);
        }
    }
    auto isSetOperation = helpers.top().isSetOperation;
    helpers.pop();
    if (helpers.empty() or isSetOperation)
    {
        queryPlans.push(queryPlan);
    }
    else
    {
        auto& subQueryHelper = helpers.top();
        subQueryHelper.queryPlans.push_back(queryPlan);
    }
    AntlrSQLBaseListener::exitPrimaryQuery(context);
}
void AntlrSQLQueryPlanCreator::enterWindowClause(AntlrSQLParser::WindowClauseContext* context)
{
    helpers.top().isWindow = true;
    AntlrSQLBaseListener::enterWindowClause(context);
}
void AntlrSQLQueryPlanCreator::exitWindowClause(AntlrSQLParser::WindowClauseContext* context)
{
    helpers.top().isWindow = false;
    AntlrSQLBaseListener::exitWindowClause(context);
}

void AntlrSQLQueryPlanCreator::enterTimeUnit(AntlrSQLParser::TimeUnitContext* context)
{
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
        helpers.top().timeUnitAdvanceBy = timeunit;
    }
    else
    {
        helpers.top().timeUnit = timeunit;
    }
}

void AntlrSQLQueryPlanCreator::exitSizeParameter(AntlrSQLParser::SizeParameterContext* context)
{
    if (context->children.size() < 3)
    {
        throw InvalidQuerySyntax("SizeParameter must have 'size', a number, and a time unit.");
    }
    helpers.top().size = std::stoi(context->children.at(1)->getText());
    AntlrSQLBaseListener::exitSizeParameter(context);
}

void AntlrSQLQueryPlanCreator::exitAdvancebyParameter(AntlrSQLParser::AdvancebyParameterContext* context)
{
    if (context->children.size() < 3)
    {
        throw InvalidQuerySyntax("AdvancebyParameter must have 'ADVANCE BY', a number, and a time unit.");
    }
    helpers.top().advanceBy = std::stoi(context->children.at(2)->getText());
    AntlrSQLBaseListener::exitAdvancebyParameter(context);
}

void AntlrSQLQueryPlanCreator::exitTimestampParameter(AntlrSQLParser::TimestampParameterContext* context)
{
    helpers.top().timestamp = context->getText();
}

/// WINDOWS
void AntlrSQLQueryPlanCreator::exitTumblingWindow(AntlrSQLParser::TumblingWindowContext* context)
{
    const auto timeMeasure = buildTimeMeasure(helpers.top().size, helpers.top().timeUnit);
    /// We use the ingestion time if the query does not have a timestamp fieldname specified
    if (helpers.top().timestamp.empty())
    {
        helpers.top().windowType = std::make_shared<Windowing::TumblingWindow>(API::IngestionTime(), timeMeasure);
    }
    else
    {
        helpers.top().windowType = std::make_shared<Windowing::TumblingWindow>(
            Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction(helpers.top().timestamp)), timeMeasure);
    }
    AntlrSQLBaseListener::exitTumblingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitSlidingWindow(AntlrSQLParser::SlidingWindowContext* context)
{
    const auto timeMeasure = buildTimeMeasure(helpers.top().size, helpers.top().timeUnit);
    const auto slidingLength = buildTimeMeasure(helpers.top().advanceBy, helpers.top().timeUnitAdvanceBy);
    /// We use the ingestion time if the query does not have a timestamp fieldname specified
    if (helpers.top().timestamp.empty())
    {
        helpers.top().windowType = Windowing::SlidingWindow::of(API::IngestionTime(), timeMeasure, slidingLength);
    }
    else
    {
        helpers.top().windowType = Windowing::SlidingWindow::of(
            Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction(helpers.top().timestamp)),
            timeMeasure,
            slidingLength);
    }
    AntlrSQLBaseListener::exitSlidingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    /// If the current functions consist of a single field access, the user simply specified a field/attribute to access
    if (helpers.top().functionBuilder.size() == 1 and helpers.top().functionBuilder.back().tryGet<FieldAccessLogicalFunction>()
        and not helpers.top().hasUnnamedAggregation)
    {
        /// Project onto the specified field and remove the field access from the active functions.
        helpers.top().addProjectionField(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>());
        helpers.top().functionBuilder.pop_back();
    }
    /// The user either specified a '*', in which case the functionBuilder should be empty, or a function on the attribute
    /// (e.g., SELECT id + 2 ...). If the user did not specify a name (... AS THE_NAME), we need to generate a name.
    else if (context->name == nullptr and not helpers.top().functionBuilder.empty() and not helpers.top().hasUnnamedAggregation)
    {
        const auto mapFunction = helpers.top().functionBuilder.back();
        const auto fieldAccessFunctions = mapFunction.getChildren()
            | std::views::transform([](auto& child) { return child.template tryGet<FieldAccessLogicalFunction>(); })
            | std::ranges::to<std::vector>();

        if (!(fieldAccessFunctions.size() == 1 and fieldAccessFunctions.front().has_value()))
        {
            throw InvalidQuerySyntax("A named function must have exactly one valid FieldAccessLogicalFunction child.");
        }
        const auto implicitFieldName
            = fmt::format("{}_{}", fieldAccessFunctions.at(0).value().getFieldName(), helpers.top().implicitMapCountHelper++);
        const auto mapFunctionWithFieldAssignment
            = FieldAssignmentLogicalFunction(FieldAccessLogicalFunction(implicitFieldName), mapFunction);
        helpers.top().mapBuilder.push_back(mapFunctionWithFieldAssignment);
        /// Projections always follow map functions. Thus, we need to project on the field assigned by the map function.
        helpers.top().addProjectionField(mapFunctionWithFieldAssignment.getField());
        helpers.top().functionBuilder.pop_back();
    }
    /// The user did not specify a new name (... AS THE_NAME) for the aggregation function and we need to generate one.
    else if (context->name == nullptr and not helpers.top().functionBuilder.empty() and helpers.top().hasUnnamedAggregation)
    {
        const auto accessFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        const auto fieldAccessNode = accessFunction.get<FieldAccessLogicalFunction>();
        const auto lastAggregation = helpers.top().windowAggs.back();
        const auto newName = fmt::format("{}_{}", fieldAccessNode.getFieldName(), lastAggregation->getName());
        const auto asField = FieldAccessLogicalFunction(newName);
        lastAggregation->asField = asField;
        helpers.top().windowAggs.pop_back();
        helpers.top().windowAggs.push_back(lastAggregation);
        helpers.top().hasUnnamedAggregation = false;
        helpers.top().addProjectionField(asField);
    }
    AntlrSQLBaseListener::exitNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::enterFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    AntlrSQLBaseListener::enterFunctionCall(context);
}

void AntlrSQLQueryPlanCreator::enterHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helpers.top().isWhereOrHaving = true;
    AntlrSQLBaseListener::enterHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helpers.top().isWhereOrHaving = false;
    if (helpers.top().functionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There was more than one function in the functionBuilder in exitHavingClause.");
    }
    helpers.top().addHavingClause(helpers.top().functionBuilder.back());
    helpers.top().functionBuilder.clear();
    AntlrSQLBaseListener::exitHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitComparison(AntlrSQLParser::ComparisonContext* context)
{
    if (helpers.top().isJoinRelation)
    {
        if (helpers.top().joinKeyRelationHelper.size() < 2)
        {
            throw InvalidQuerySyntax(
                "Requires two functions but got {} at {}", helpers.top().joinKeyRelationHelper.size(), context->getText());
        }
        const auto rightFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        const auto leftFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        const auto function = createFunctionFromOpBoolean(leftFunction, rightFunction, helpers.top().opBoolean);
        helpers.top().joinKeyRelationHelper.push_back(function);
        helpers.top().joinFunction = function;
    }
    else
    {
        if (helpers.top().functionBuilder.size() < 2)
        {
            throw InvalidQuerySyntax("Comparison requires two parameters, got {}", context->getText());
        }
        const auto rightFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        const auto leftFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();

        const auto function = createFunctionFromOpBoolean(leftFunction, rightFunction, helpers.top().opBoolean);
        helpers.top().functionBuilder.push_back(function);
    }
    AntlrSQLBaseListener::exitComparison(context);
}

void AntlrSQLQueryPlanCreator::enterJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    helpers.top().joinKeyRelationHelper.clear();
    helpers.top().isJoinRelation = true;
    AntlrSQLBaseListener::enterJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::enterJoinCriteria(AntlrSQLParser::JoinCriteriaContext* context)
{
    INVARIANT(helpers.top().isJoinRelation, "Join criteria must be inside a join relation.");
    AntlrSQLBaseListener::enterJoinCriteria(context);
}

void AntlrSQLQueryPlanCreator::enterJoinType(AntlrSQLParser::JoinTypeContext* context)
{
    if (not helpers.top().isJoinRelation)
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
        helpers.top().joinType = JoinLogicalOperator::JoinType::INNER_JOIN;
    }
    else
    {
        throw InvalidQuerySyntax("Unknown join type: {}, resolved to token type: {}", joinType, tokenType);
    }
    AntlrSQLBaseListener::exitJoinType(context);
}

void AntlrSQLQueryPlanCreator::exitJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    helpers.top().isJoinRelation = false;
    if (helpers.top().joinSources.size() == helpers.top().joinSourceRenames.size() + 1)
    {
        helpers.top().joinSourceRenames.emplace_back("");
    }

    /// we assume that the left query plan is the first element in the queryPlans vector and the right query plan is the second element
    if (helpers.top().queryPlans.size() != 2)
    {
        throw InvalidQuerySyntax(
            "Join relation requires two subqueries, but got {} at {}", helpers.top().queryPlans.size(), context->getText());
    }
    const auto leftQueryPlan = helpers.top().queryPlans[0];
    const auto rightQueryPlan = helpers.top().queryPlans[1];
    helpers.top().queryPlans.clear();

    if (!helpers.top().joinFunction)
    {
        throw InvalidQuerySyntax("joinFunction is required but empty at {}", context->getText());
    }
    if (!helpers.top().windowType)
    {
        throw InvalidQuerySyntax("joinFunction is required but empty at {}", context->getText());
    }
    const auto queryPlan = LogicalPlanBuilder::addJoin(
        leftQueryPlan, rightQueryPlan, helpers.top().joinFunction.value(), helpers.top().windowType, helpers.top().joinType);
    if (not helpers.empty())
    {
        /// we are in a subquery
        helpers.top().queryPlans.push_back(queryPlan);
    }
    else
    {
        /// for now, we will never enter this branch, because we always have a subquery
        /// as we require the join relations to always be a sub-query
        queryPlans.push(queryPlan);
    }
    AntlrSQLBaseListener::exitJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::exitLogicalNot(AntlrSQLParser::LogicalNotContext* context)
{
    if (helpers.empty())
    {
        throw InvalidQuerySyntax("Parser is confused at {}", context->getText());
    }

    if (helpers.top().isJoinRelation)
    {
        if (helpers.top().joinKeyRelationHelper.empty())
        {
            throw InvalidQuerySyntax("Negate requires child op at {}", context->getText());
        }
        const auto innerFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        if (!helpers.top().joinFunction)
        {
            throw InvalidQuerySyntax("Negate requires child op at {}", context->getText());
        }
        auto negatedFunction = NegateLogicalFunction(helpers.top().joinFunction.value());
        helpers.top().joinKeyRelationHelper.emplace_back(negatedFunction);
        helpers.top().joinFunction = negatedFunction;
    }
    else
    {
        if (helpers.top().functionBuilder.empty())
        {
            throw InvalidQuerySyntax("Negate requires child op at {}", context->getText());
        }
        const auto innerFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        helpers.top().functionBuilder.emplace_back(NegateLogicalFunction(innerFunction));
    }
    AntlrSQLBaseListener::exitLogicalNot(context);
}

void AntlrSQLQueryPlanCreator::exitConstantDefault(AntlrSQLParser::ConstantDefaultContext* context)
{
    if (context->children.size() != 1)
    {
        throw InvalidQuerySyntax("When exiting a constant, there must be exactly one children in the context {}", context->getText());
    }
    if (const auto stringLiteralContext = dynamic_cast<AntlrSQLParser::StringLiteralContext*>(context->children.at(0)))
    {
        if (!(stringLiteralContext->getText().size() > 2))
        {
            throw InvalidQuerySyntax(
                "A constant string literal must contain at least two quotes and must not be empty at {}", context->getText());
        }
        helpers.top().constantBuilder.push_back(context->getText().substr(1, stringLiteralContext->getText().size() - 2));
    }
    else
    {
        helpers.top().constantBuilder.push_back(context->getText());
    }
}

void AntlrSQLQueryPlanCreator::exitFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    const auto funcName = Util::toUpperCase(context->children[0]->getText());
    const auto tokenType = context->getStart()->getType();

    helpers.top().hasUnnamedAggregation = true;
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::COUNT:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                CountAggregationLogicalFunction::create(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::AVG:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                AvgAggregationLogicalFunction::create(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::MAX:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                MaxAggregationLogicalFunction::create(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::MIN:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                MinAggregationLogicalFunction::create(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::SUM:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                SumAggregationLogicalFunction::create(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::MEDIAN:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                MedianAggregationLogicalFunction::create(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::ARRAY_AGG:
            helpers.top().windowAggs.push_back(
                ArrayAggregationLogicalFunction::create(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        default:
            /// Check if the function is a constructor for a datatype
            if (const auto dataType = DataTypeProvider::tryProvideDataType(funcName); dataType.has_value())
            {
                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax("Expected constant, got nothing at {}", context->getText());
                }
                helpers.top().hasUnnamedAggregation = false;
                auto value = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                auto constFunctionItem = ConstantValueLogicalFunction(*dataType, std::move(value));
                helpers.top().functionBuilder.emplace_back(constFunctionItem);
            }
            else if (auto logicalFunction = LogicalFunctionProvider::tryProvide(funcName, std::move(helpers.top().functionBuilder)))
            {
                helpers.top().functionBuilder.push_back(*logicalFunction);
            }
            else
            {
                throw InvalidQuerySyntax("Unknown aggregation function: {}, resolved to token type: {}", funcName, tokenType);
            }
    }
}

void AntlrSQLQueryPlanCreator::exitThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext* context)
{
    helpers.top().minimumCount = std::stoi(context->getText());
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
        helpers.top().queryPlans.push_back(queryPlan);
    }
    else
    {
        queryPlans.push(queryPlan);
    }
    AntlrSQLBaseListener::exitSetOperation(context);
}

void AntlrSQLQueryPlanCreator::enterGroupByClause(AntlrSQLParser::GroupByClauseContext* context)
{
    helpers.top().isGroupBy = true;
    AntlrSQLBaseListener::enterGroupByClause(context);
}

void AntlrSQLQueryPlanCreator::exitGroupByClause(AntlrSQLParser::GroupByClauseContext* context)
{
    helpers.top().isGroupBy = false;
    AntlrSQLBaseListener::exitGroupByClause(context);
}
}

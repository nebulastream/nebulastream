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

#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <variant>
#include <vector>

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
#include <Functions/BooleanFunctions/IsNullCheckLogicalFunction.hpp>
#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConcatLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MedianAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MinAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Util/Overloaded.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <CommonParserFunctions.hpp>
#include <ErrorHandling.hpp>
#include <ParserUtil.hpp>

namespace NES::Parsers
{

LogicalPlan AntlrSQLQueryPlanCreator::getQueryPlan() const
{
    if (sinks.empty())
    {
        throw InvalidQuerySyntax("Query does not contain sink");
    }
    if (queryPlans.empty())
    {
        throw InvalidQuerySyntax("Query could not be parsed");
    }
    /// TODO #421: support multiple sinks
    INVARIANT(!sinks.empty(), "Need at least one sink!");
    return std::visit(
        Overloaded{
            [&](const Identifier& sinkName) { return LogicalPlanBuilder::addSink(sinkName, queryPlans.top()); },
            [&](const std::pair<Identifier, ConfigMap>& anonymousSink)
            {
                const auto& [type, configOptions] = anonymousSink;
                const auto sinkConfig = getSinkConfig(configOptions);
                const auto formatConfig = parseOutputFormatterConfig(configOptions);
                const auto schemaOpt = getSinkSchema(configOptions);
                return LogicalPlanBuilder::addAnonymousSink(type, schemaOpt, sinkConfig, formatConfig, queryPlans.top());
            }},
        sinks.front());
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
        default:
            const AntlrSQLLexer lexer(nullptr);
            const std::string tokenName = std::string(lexer.getVocabulary().getSymbolicName(timebase));
            throw InvalidQuerySyntax("Unknown time unit: {}", tokenName);
    }
}

static LogicalFunction createFunctionFromOpBoolean(LogicalFunction leftFunction, LogicalFunction rightFunction, const uint64_t tokenType)
{
    switch (tokenType)
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
    switch (tokenType)
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

namespace
{

bool isInsideDataTypeConstructorArgument(antlr4::ParserRuleContext* context)
{
    for (auto* parent = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parent != nullptr;
         parent = dynamic_cast<antlr4::ParserRuleContext*>(parent->parent))
    {
        if (auto* const functionCall = dynamic_cast<AntlrSQLParser::FunctionCallContext*>(parent))
        {
            return functionCall->typeDefinition() != nullptr;
        }
    }
    return false;
}

LogicalFunction createRawLiteralFunction(std::string literal)
{
    return ConstantValueLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED), std::move(literal));
}

LogicalFunction createNegatedNumericLiteralFunction(const ConstantValueLogicalFunction& constantFunction)
{
    auto constantValue = constantFunction.getConstantValue();
    if (constantValue.starts_with('-'))
    {
        constantValue.erase(0, 1);
    }
    else
    {
        constantValue.insert(0, 1, '-');
    }

    return ConstantValueLogicalFunction(constantFunction.getDataType(), std::move(constantValue));
}

LogicalFunction createBetweenFunction(const LogicalFunction& valueFunction, LogicalFunction lowerFunction, LogicalFunction upperFunction)
{
    return AndLogicalFunction(
        GreaterEqualsLogicalFunction(valueFunction, std::move(lowerFunction)),
        LessEqualsLogicalFunction(valueFunction, std::move(upperFunction)));
}

LogicalFunction createInFunction(const LogicalFunction& valueFunction, std::vector<LogicalFunction> candidateFunctions)
{
    if (candidateFunctions.empty())
    {
        throw InvalidQuerySyntax("IN predicate requires at least one candidate value");
    }

    LogicalFunction function = EqualsLogicalFunction(valueFunction, std::move(candidateFunctions.front()));
    for (auto& candidateFunction : candidateFunctions | std::views::drop(1))
    {
        function = OrLogicalFunction(std::move(function), EqualsLogicalFunction(valueFunction, std::move(candidateFunction)));
    }
    return function;
}

void combineLogicalFunctions(
    std::vector<LogicalFunction>& functions, const size_t operandCount, const uint64_t tokenType, const std::string& expressionText)
{
    if (operandCount < 2)
    {
        return;
    }
    if (functions.size() < operandCount)
    {
        throw InvalidQuerySyntax("Expected {} operands for logical expression, got {}: {}", operandCount, functions.size(), expressionText);
    }

    const auto firstOperandIndex = functions.size() - operandCount;
    auto function = std::move(functions[firstOperandIndex]);
    for (auto operandIndex = firstOperandIndex + 1; operandIndex < functions.size(); ++operandIndex)
    {
        function = createLogicalBinaryFunction(std::move(function), std::move(functions[operandIndex]), tokenType);
    }
    functions.resize(firstOperandIndex);
    functions.emplace_back(std::move(function));
}

void negateTopFunction(std::stack<AntlrSQLHelper>& helpers, const std::string& expressionText)
{
    if (helpers.empty())
    {
        throw InvalidQuerySyntax("Parser is confused at {}", expressionText);
    }

    if (helpers.top().isJoinRelation)
    {
        if (helpers.top().joinKeyRelationHelper.empty())
        {
            throw InvalidQuerySyntax("Negate requires child op at {}", expressionText);
        }
        auto innerFunction = std::move(helpers.top().joinKeyRelationHelper.back());
        helpers.top().joinKeyRelationHelper.pop_back();
        helpers.top().joinKeyRelationHelper.emplace_back(NegateLogicalFunction(std::move(innerFunction)));
    }
    else
    {
        if (helpers.top().functionBuilder.empty())
        {
            throw InvalidQuerySyntax("Negate requires child op at {}", expressionText);
        }
        auto innerFunction = std::move(helpers.top().functionBuilder.back());
        helpers.top().functionBuilder.pop_back();
        helpers.top().functionBuilder.emplace_back(NegateLogicalFunction(std::move(innerFunction)));
    }
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
    {
        throw InvalidQuerySyntax("INTO must be followed by at least one sink-identifier.");
    }
    /// Store all specified sinks.
    for (const auto& sink : context->sink())
    {
        if (sink->identifier() != nullptr)
        {
            sinks.emplace_back(bindIdentifier(sink->identifier()));
        }
        else if (sink->anonymousSink() != nullptr)
        {
            const auto& anonymousSink = sink->anonymousSink();

            const auto type = bindIdentifier(anonymousSink->type);
            const auto configOptions = bindConfigOptions(anonymousSink->parameters->namedConfigExpression());

            sinks.emplace_back(std::make_pair(type, configOptions));
        }
    }
}

void AntlrSQLQueryPlanCreator::exitOrPredicate(AntlrSQLParser::OrPredicateContext* context)
{
    auto& functions = helpers.top().isJoinRelation ? helpers.top().joinKeyRelationHelper : helpers.top().functionBuilder;
    combineLogicalFunctions(functions, context->andPredicate().size(), AntlrSQLLexer::OR, context->getText());
    AntlrSQLBaseListener::exitOrPredicate(context);
}

void AntlrSQLQueryPlanCreator::exitAndPredicate(AntlrSQLParser::AndPredicateContext* context)
{
    auto& functions = helpers.top().isJoinRelation ? helpers.top().joinKeyRelationHelper : helpers.top().functionBuilder;
    combineLogicalFunctions(functions, context->notPredicate().size(), AntlrSQLLexer::AND, context->getText());
    AntlrSQLBaseListener::exitAndPredicate(context);
}

void AntlrSQLQueryPlanCreator::exitBoolComparison(AntlrSQLParser::BoolComparisonContext* context)
{
    auto* comparison = context->booleanComparison();
    if (comparison == nullptr)
    {
        AntlrSQLBaseListener::exitBoolComparison(context);
        return;
    }

    auto& functions = helpers.top().isJoinRelation ? helpers.top().joinKeyRelationHelper : helpers.top().functionBuilder;
    const auto popFunction = [&functions, comparison]()
    {
        if (functions.empty())
        {
            throw InvalidQuerySyntax("Predicate {} is missing an operand", comparison->getText());
        }
        auto function = std::move(functions.back());
        functions.pop_back();
        return function;
    };

    LogicalFunction function;
    if (comparison->BETWEEN() != nullptr)
    {
        if (functions.size() < 3)
        {
            throw InvalidQuerySyntax("BETWEEN predicate requires a value, lower bound, and upper bound: {}", comparison->getText());
        }
        auto upperFunction = popFunction();
        auto lowerFunction = popFunction();
        auto valueFunction = popFunction();
        function = createBetweenFunction(valueFunction, std::move(lowerFunction), std::move(upperFunction));
        if (comparison->NOT() != nullptr)
        {
            function = NegateLogicalFunction(std::move(function));
        }
    }
    else if (comparison->IN() != nullptr)
    {
        if (comparison->query() != nullptr)
        {
            throw UnsupportedQuery("IN subqueries are currently not supported: {}", comparison->getText());
        }
        const auto candidateCount = comparison->expression().size();
        if (candidateCount == 0)
        {
            throw InvalidQuerySyntax("IN predicate requires at least one candidate value: {}", comparison->getText());
        }
        if (functions.size() < candidateCount + 1)
        {
            throw InvalidQuerySyntax("IN predicate {} is missing operands", comparison->getText());
        }
        const auto candidatesBegin = functions.end() - static_cast<std::ptrdiff_t>(candidateCount);
        std::vector<LogicalFunction> candidateFunctions(candidatesBegin, functions.end());
        functions.erase(candidatesBegin, functions.end());
        auto valueFunction = popFunction();
        function = createInFunction(valueFunction, std::move(candidateFunctions));
        if (comparison->NOT() != nullptr)
        {
            function = NegateLogicalFunction(std::move(function));
        }
    }
    else if (comparison->IS() != nullptr && comparison->nullNotnull() != nullptr)
    {
        function = IsNullCheckLogicalFunction(popFunction());
        if (comparison->nullNotnull()->NOT() != nullptr)
        {
            function = NegateLogicalFunction(std::move(function));
        }
    }
    else
    {
        throw UnsupportedQuery("Predicate is currently not supported: {}", comparison->getText());
    }

    functions.push_back(std::move(function));
    AntlrSQLBaseListener::exitBoolComparison(context);
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
    switch (opTokenType)
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
    auto opTokenType = context->op->getType();
    if (context->valueExpression() != nullptr && !helpers.top().constantBuilder.empty())
    {
        auto& constant = helpers.top().constantBuilder.back();
        if (constant == context->valueExpression()->getText())
        {
            switch (opTokenType)
            {
                case AntlrSQLLexer::PLUS:
                    return;
                case AntlrSQLLexer::MINUS:
                    if (!constant.empty() && constant.front() == '-')
                    {
                        constant.erase(0, 1);
                    }
                    else
                    {
                        constant.insert(0, 1, '-');
                    }
                    return;
                default:
                    break;
            }
        }
    }

    auto& functions = helpers.top().isJoinRelation ? helpers.top().joinKeyRelationHelper : helpers.top().functionBuilder;
    if (functions.empty())
    {
        throw InvalidQuerySyntax("Expected unary operator, got nothing: {}", context->getText());
    }
    LogicalFunction function;
    const auto innerFunction = functions.back();
    functions.pop_back();
    switch (opTokenType)
    {
        case AntlrSQLLexer::PLUS:
            function = innerFunction;
            break;
        case AntlrSQLLexer::MINUS:
            if (const auto constantFunction = innerFunction.tryGetAs<ConstantValueLogicalFunction>(); constantFunction.has_value()
                and (constantFunction->get().getDataType().isNumeric()
                     or constantFunction->get().getDataType().isType(DataType::Type::UNDEFINED))
                and constantFunction->get().getConstantValue() == context->valueExpression()->getText())
            {
                function = createNegatedNumericLiteralFunction(constantFunction->get());
                break;
            }
            function = MulLogicalFunction(
                ConstantValueLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::INT64), "-1"), innerFunction);
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    functions.push_back(function);
}

void AntlrSQLQueryPlanCreator::enterUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext* context)
{
    /// Get Index of Parent Rule to check type of parent rule in conditions
    if (helpers.top().isFrom && !helpers.top().isJoinRelation)
    {
        helpers.top().newSourceName = bindIdentifier(context);
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
        helpers.top().groupByFields.emplace_back(bindIdentifier(context));
    }
    else if (
        (helpers.top().isWhereOrHaving || helpers.top().isSelect || helpers.top().isWindow)
        && AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helpers.top().functionBuilder.emplace_back(UnboundFieldAccessLogicalFunction(bindIdentifier(context)));
    }
    else if (
        helpers.top().isFrom and not helpers.top().isJoinRelation and not helpers.top().isModelInference
        and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        /// get main source name
        helpers.top().setSource(bindIdentifier(context));
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
            const auto attribute = std::move(helpers.top().functionBuilder.back());
            helpers.top().functionBuilder.pop_back();
            helpers.top().addProjection(bindIdentifier(context), attribute);
        }
    }
    else if (helpers.top().isInAggFunction() and AntlrSQLParser::RuleNamedExpression == parentRuleIndex)
    {
        const auto expression = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        if (expression.tryGetAs<UnboundFieldAccessLogicalFunction>())
        {
            auto aggFunc = helpers.top().windowAggs.back();
            helpers.top().windowAggs.pop_back();
            aggFunc.second = bindIdentifier(context);
            helpers.top().windowAggs.push_back(aggFunc);
            helpers.top().addProjection(std::nullopt, UnboundFieldAccessLogicalFunction{aggFunc.second.value()});
        }
        else
        {
            helpers.top().addProjection(bindIdentifier(context), expression);
        }
        helpers.top().hasUnnamedAggregation = false;
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helpers.top().joinKeyRelationHelper.emplace_back(UnboundFieldAccessLogicalFunction(bindIdentifier(context)));
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        helpers.top().joinSources.push_back(bindIdentifier(context));
    }
}

void AntlrSQLQueryPlanCreator::enterPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    if (not helpers.empty() and not helpers.top().isFrom and not helpers.top().isSetOperation)
    {
        throw InvalidQuerySyntax("Subqueries are only supported in FROM clauses, but got {}", context->getText());
    }

    const AntlrSQLHelper helper;
    helpers.push(helper);
    AntlrSQLBaseListener::enterPrimaryQuery(context);
}

void AntlrSQLQueryPlanCreator::exitPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    LogicalPlan queryPlan = [&]
    {
        if (not helpers.top().queryPlans.empty())
        {
            return std::move(helpers.top().queryPlans[0]);
        }
        if (!helpers.top().getSource().has_value())
        {
            const auto anonymousSourceConfig = helpers.top().getAnonymousSourceConfig();
            if (!anonymousSourceConfig.has_value())
            {
                throw InvalidQuerySyntax("Neither named source or anonymous source specified");
            }
            const auto [type, configOptions] = anonymousSourceConfig.value();
            const auto parserConfig = parseInputFormatterConfig(configOptions);
            const auto sourceConfig = getSourceConfig(configOptions);
            const auto schema = getSourceSchema(configOptions);
            if (!schema.has_value())
            {
                throw InvalidConfigParameter("Anonymous Source is missing schema definition");
            }

            return LogicalPlanBuilder::createLogicalPlan(type, schema.value(), sourceConfig, parserConfig);
        }
        return LogicalPlanBuilder::createLogicalPlan(helpers.top().getSource().value());
    }();

    for (auto whereExpr = helpers.top().getWhereClauses().rbegin(); whereExpr != helpers.top().getWhereClauses().rend(); ++whereExpr)
    {
        queryPlan = LogicalPlanBuilder::addSelection(std::move(*whereExpr), queryPlan);
    }

    /// Insert pre-aggregation projections for desugared expression arguments (e.g., AVG(i + UINT64(1))).
    if (!helpers.top().preAggregationProjections.empty())
    {
        auto verifiedProjections = helpers.top().preAggregationProjections
            | std::views::transform(
                                       [](const auto& pair)
                                       {
                                           if (!pair.first.has_value())
                                           {
                                               throw InvalidQuerySyntax("Projection must have a name.");
                                           }
                                           return ProjectionLogicalOperator::UnboundProjection{pair.first.value(), pair.second};
                                       });
        queryPlan = LogicalPlanBuilder::addProjection(verifiedProjections | std::ranges::to<std::vector>(), /*asterisk=*/true, queryPlan);
    }

    auto windowTypeOpt = helpers.top().windowType;
    if (windowTypeOpt.has_value() && helpers.top().joinKeyRelationHelper.empty())
    {
        const auto currentWindowTimestampOpt = helpers.top().windowTimestamp;
        if (!currentWindowTimestampOpt.has_value()
            || !std::holds_alternative<Windowing::UnboundTimeCharacteristic>(currentWindowTimestampOpt.value()))
        {
            throw InvalidQuerySyntax(
                "windowed aggregation requires exactly one time characteristic, got {}",
                currentWindowTimestampOpt.has_value() ? "two" : "none");
        }
        auto characteristic = std::get<Windowing::UnboundTimeCharacteristic>(currentWindowTimestampOpt.value());
        auto aggregations = helpers.top().windowAggs
            | std::views::transform(
                                [](auto& agg)
                                {
                                    if (!agg.second.has_value())
                                    {
                                        throw InvalidQuerySyntax("Aggregation function must have a name.");
                                    }
                                    return WindowedAggregationLogicalOperator::ProjectedAggregation{agg.first, agg.second.value()};
                                });
        queryPlan = LogicalPlanBuilder::addWindowAggregation(
            queryPlan,
            windowTypeOpt.value(),
            aggregations | std::ranges::to<std::vector>(),
            helpers.top().groupByFields,
            std::move(characteristic));
    }

    auto projections = helpers.top().getProjections()
        | std::views::transform(
                           [](const auto& pair)
                           {
                               if (!pair.first.has_value())
                               {
                                   return ProjectionLogicalOperator::UnboundProjection{
                                       bindIdentifier(pair.second.explain(ExplainVerbosity::Short)), pair.second};
                               }
                               return ProjectionLogicalOperator::UnboundProjection{pair.first.value(), pair.second};
                           });
    queryPlan = LogicalPlanBuilder::addProjection(projections | std::ranges::to<std::vector>(), helpers.top().asterisk, queryPlan);

    if (windowTypeOpt.has_value())
    {
        for (auto havingExpr = helpers.top().getHavingClauses().rbegin(); havingExpr != helpers.top().getHavingClauses().rend();
             ++havingExpr)
        {
            queryPlan = LogicalPlanBuilder::addSelection(*havingExpr, queryPlan);
        }
    }
    helpers.pop();
    if (helpers.empty())
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
    PRECONDITION(
        context->IDENTIFIER().size() == 1 || context->IDENTIFIER().size() == 2,
        "TimestampParameter must have either one or two identifiers, is the binder in sync with the grammar?");
    if (context->IDENTIFIER().size() == 1)
    {
        helpers.top().windowTimestamp = Windowing::UnboundEventTimeCharacteristic{
            .field = UnboundFieldAccessLogicalFunction{bindIdentifier(context->IDENTIFIER().at(0)->getText())}};
    }
    else if (context->IDENTIFIER().size() == 2)
    {
        helpers.top().windowTimestamp = std::array<Windowing::UnboundTimeCharacteristic, 2>{
            Windowing::UnboundEventTimeCharacteristic{
                .field = UnboundFieldAccessLogicalFunction{bindIdentifier(context->IDENTIFIER().at(0)->getText())}},
            Windowing::UnboundEventTimeCharacteristic{
                .field = UnboundFieldAccessLogicalFunction{bindIdentifier(context->IDENTIFIER().at(1)->getText())}}};
    }
    AntlrSQLBaseListener::exitTimestampParameter(context);
}

/// WINDOWS
void AntlrSQLQueryPlanCreator::exitTumblingWindow(AntlrSQLParser::TumblingWindowContext* context)
{
    const auto timeMeasure = buildTimeMeasure(helpers.top().size, helpers.top().timeUnit);
    helpers.top().windowType.emplace(Windowing::TimeBasedWindowType{Windowing::TumblingWindow{timeMeasure}});
    /// No explicit timestamp parameter was given (e.g. TUMBLING(SIZE 1000 MS) without an ON <field> clause), so default to ingestion time.
    /// A join window needs two time characteristics (one per input); a plain windowed aggregation needs exactly one. The timestampParameter
    /// child is absent here (it would have set windowTimestamp via exitTimestampParameter), so the arity is derived from the join context.
    if (!helpers.top().windowTimestamp.has_value())
    {
        if (helpers.top().isJoinRelation)
        {
            helpers.top().windowTimestamp = std::array<Windowing::UnboundTimeCharacteristic, 2>{
                Windowing::IngestionTimeCharacteristic{}, Windowing::IngestionTimeCharacteristic{}};
        }
        else
        {
            helpers.top().windowTimestamp = Windowing::IngestionTimeCharacteristic{};
        }
    }
    AntlrSQLBaseListener::exitTumblingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitSlidingWindow(AntlrSQLParser::SlidingWindowContext* context)
{
    const auto timeMeasure = buildTimeMeasure(helpers.top().size, helpers.top().timeUnit);
    const auto slidingLength = buildTimeMeasure(helpers.top().advanceBy, helpers.top().timeUnitAdvanceBy);
    /// We use the ingestion time if the query does not have a timestamp fieldname specified
    helpers.top().windowType.emplace(Windowing::TimeBasedWindowType{Windowing::SlidingWindow(timeMeasure, slidingLength)});
    AntlrSQLBaseListener::exitSlidingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    AntlrSQLHelper& helper = helpers.top();
    if (context->name == nullptr and helper.functionBuilder.size() == 1
        and helper.functionBuilder.back().tryGetAs<UnboundFieldAccessLogicalFunction>() and not helpers.top().hasUnnamedAggregation)
    {
        auto fieldName = helpers.top().functionBuilder.back().getAs<UnboundFieldAccessLogicalFunction>()->getFieldName();
        /// Project onto the specified field and remove the field access from the active functions.
        helpers.top().addProjection(std::make_optional(fieldName), std::move(helpers.top().functionBuilder.back()));
        helpers.top().functionBuilder.pop_back();
    }
    else if (helper.isSelect && context->getText() == "*" && helper.functionBuilder.empty())
    {
        helper.asterisk = true;
    }
    /// The user did not specify a new name (... AS THE_NAME) for the aggregation function.
    /// The aggregation's asField is already set in exitFunctionCall. Project the expression directly,
    /// which may be a plain field access (MEDIAN(i8)) or an arithmetic expression (MEDIAN(i8) + UINT64(1)).
    else if (context->name == nullptr and not helpers.top().functionBuilder.empty() and helpers.top().hasUnnamedAggregation)
    {
        const auto expression = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        helpers.top().addProjection(std::nullopt, expression);
        helpers.top().hasUnnamedAggregation = false;
    }
    AntlrSQLBaseListener::exitNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::enterFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    AntlrSQLBaseListener::enterFunctionCall(context);
}

void AntlrSQLQueryPlanCreator::exitCastExpression(AntlrSQLParser::CastExpressionContext* context)
{
    const auto targetDataType = bindDataType(context->targetType, DataType::NULLABLE::NOT_NULLABLE);
    if (helpers.top().functionBuilder.empty())
    {
        throw InvalidQuerySyntax("CAST requires exactly one child expression at {}", context->getText());
    }
    auto child = std::move(helpers.top().functionBuilder.back());
    helpers.top().functionBuilder.pop_back();
    helpers.top().functionBuilder.emplace_back(CastToTypeLogicalFunction{targetDataType, child});
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
    if (context->LEFT() != nullptr)
    {
        helpers.top().joinType = JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN;
    }
    else if (context->RIGHT() != nullptr)
    {
        helpers.top().joinType = JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN;
    }
    else if (context->FULL() != nullptr)
    {
        helpers.top().joinType = JoinLogicalOperator::JoinType::OUTER_FULL_JOIN;
    }
    else
    {
        helpers.top().joinType = JoinLogicalOperator::JoinType::INNER_JOIN;
    }
    AntlrSQLBaseListener::exitJoinType(context);
}

void AntlrSQLQueryPlanCreator::exitJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    helpers.top().isJoinRelation = false;

    /// we assume that the left query plan is the first element in the queryPlans vector and the right query plan is the second element
    if (helpers.top().queryPlans.size() != 2)
    {
        throw InvalidQuerySyntax(
            "Join relation requires two subqueries, but got {} at {}", helpers.top().queryPlans.size(), context->getText());
    }
    const auto leftQueryPlan = helpers.top().queryPlans[0];
    const auto rightQueryPlan = helpers.top().queryPlans[1];
    helpers.top().queryPlans.clear();

    if (helpers.top().joinKeyRelationHelper.size() != 1)
    {
        throw InvalidQuerySyntax("joinFunction is required but empty at {}", context->getText());
    }
    auto windowTypeOpt = helpers.top().windowType;
    if (!windowTypeOpt)
    {
        throw InvalidQuerySyntax("windowType is required but empty at {}", context->getText());
    }

    const auto currentWindowTimestampOpt = helpers.top().windowTimestamp;
    if (!currentWindowTimestampOpt.has_value()
        || !std::holds_alternative<std::array<Windowing::UnboundTimeCharacteristic, 2>>(currentWindowTimestampOpt.value()))
    {
        throw InvalidQuerySyntax("join requires two timestamps, but got {}", currentWindowTimestampOpt.has_value() ? "only one" : "none");
    }

    auto joinTimeCharacteristics = std::get<std::array<Windowing::UnboundTimeCharacteristic, 2>>(currentWindowTimestampOpt.value());
    const auto queryPlan = LogicalPlanBuilder::addJoin(
        leftQueryPlan,
        rightQueryPlan,
        helpers.top().joinKeyRelationHelper.at(0),
        windowTypeOpt.value(),
        helpers.top().joinType,
        joinTimeCharacteristics[0],
        joinTimeCharacteristics[1]);
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
    negateTopFunction(helpers, context->getText());
    AntlrSQLBaseListener::exitLogicalNot(context);
}

void AntlrSQLQueryPlanCreator::exitLogicalNotPredicate(AntlrSQLParser::LogicalNotPredicateContext* context)
{
    negateTopFunction(helpers, context->getText());
    AntlrSQLBaseListener::exitLogicalNotPredicate(context);
}

void AntlrSQLQueryPlanCreator::exitConstantDefault(AntlrSQLParser::ConstantDefaultContext* context)
{
    if (context->children.size() != 1)
    {
        throw InvalidQuerySyntax("When exiting a constant, there must be exactly one children in the context {}", context->getText());
    }

    const auto insideDataTypeConstructorArgument = isInsideDataTypeConstructorArgument(context);
    if (dynamic_cast<AntlrSQLParser::NumericLiteralContext*>(context->constant()) != nullptr and !insideDataTypeConstructorArgument)
    {
        auto& functionBuilder = helpers.top().isJoinRelation ? helpers.top().joinKeyRelationHelper : helpers.top().functionBuilder;
        functionBuilder.emplace_back(createRawLiteralFunction(context->getText()));
        return;
    }

    if (const auto stringLiteralContext = dynamic_cast<AntlrSQLParser::StringLiteralContext*>(context->children.at(0)))
    {
        if (!(stringLiteralContext->getText().size() > 2))
        {
            throw InvalidQuerySyntax(
                "A constant string literal must contain at least two quotes and must not be empty at {}", context->getText());
        }
        if (!insideDataTypeConstructorArgument)
        {
            auto& functionBuilder = helpers.top().isJoinRelation ? helpers.top().joinKeyRelationHelper : helpers.top().functionBuilder;
            functionBuilder.emplace_back(createRawLiteralFunction(context->getText()));
            return;
        }
        helpers.top().constantBuilder.push_back(context->getText().substr(1, stringLiteralContext->getText().size() - 2));
    }
    else if (dynamic_cast<AntlrSQLParser::BooleanLiteralContext*>(context->constant()) != nullptr and !insideDataTypeConstructorArgument)
    {
        auto& functionBuilder = helpers.top().isJoinRelation ? helpers.top().joinKeyRelationHelper : helpers.top().functionBuilder;
        functionBuilder.emplace_back(createRawLiteralFunction(context->getText()));
    }
    else
    {
        helpers.top().constantBuilder.push_back(context->getText());
    }
}

void AntlrSQLQueryPlanCreator::exitFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    const auto funcName = toUpperCase(context->children[0]->getText());
    const auto tokenType = context->getStart()->getType();

    /// Ensures the current aggregation argument is a direct field access.
    /// If the argument is an expression (e.g. i + UINT64(1)), desugars it into a pre-aggregation
    /// projection so that the aggregation operates on a simple field reference.
    const auto ensureFieldAccessArgument = [&]()
    {
        if (helpers.top().functionBuilder.empty())
        {
            throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
        }
        if (!helpers.top().functionBuilder.back().tryGetAs<UnboundFieldAccessLogicalFunction>())
        {
            /// Desugar: pop the expression, create a temp field, store a pre-aggregation projection,
            /// and push a FieldAccess to the temp field back onto functionBuilder.
            auto expression = std::move(helpers.top().functionBuilder.back());
            helpers.top().functionBuilder.pop_back();
            const auto tempName = bindIdentifier(fmt::format("_agg_input_{}", helpers.top().aggExprCounter++));
            helpers.top().preAggregationProjections.emplace_back(tempName, std::move(expression));
            helpers.top().functionBuilder.emplace_back(UnboundFieldAccessLogicalFunction(tempName));
        }
    };

    auto isAggregation = false;
    switch (tokenType)
    {
        case AntlrSQLLexer::COUNT: {
            const auto includeNullValues = (context->starArg != nullptr);
            if (includeNullValues)
            {
                /// COUNT(*) — no field argument needed; use a dummy * field
                helpers.top().functionBuilder.emplace_back(UnboundFieldAccessLogicalFunction(Identifier::parse("*")));
            }
            else
            {
                ensureFieldAccessArgument();
            }
            helpers.top().windowAggs.emplace_back(
                CountAggregationLogicalFunction{
                    helpers.top().functionBuilder.back().getAs<UnboundFieldAccessLogicalFunction>(), includeNullValues},
                std::nullopt);
            isAggregation = true;
            break;
        }
        case AntlrSQLLexer::AVG:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.emplace_back(
                AvgAggregationLogicalFunction{helpers.top().functionBuilder.back().getAs<UnboundFieldAccessLogicalFunction>()},
                std::nullopt);
            isAggregation = true;
            break;
        case AntlrSQLLexer::MAX:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.emplace_back(
                MaxAggregationLogicalFunction{helpers.top().functionBuilder.back().getAs<UnboundFieldAccessLogicalFunction>()},
                std::nullopt);
            isAggregation = true;
            break;
        case AntlrSQLLexer::MIN:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.emplace_back(
                MinAggregationLogicalFunction{helpers.top().functionBuilder.back().getAs<UnboundFieldAccessLogicalFunction>()},
                std::nullopt);
            isAggregation = true;
            break;
        case AntlrSQLLexer::SUM:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.emplace_back(
                SumAggregationLogicalFunction{helpers.top().functionBuilder.back().getAs<UnboundFieldAccessLogicalFunction>()},
                std::nullopt);
            isAggregation = true;
            break;
        case AntlrSQLLexer::MEDIAN:
            ensureFieldAccessArgument();
            helpers.top().windowAggs.emplace_back(
                MedianAggregationLogicalFunction{helpers.top().functionBuilder.back().getAs<UnboundFieldAccessLogicalFunction>()},
                std::nullopt);
            isAggregation = true;
            break;
        default:
            helpers.top().hasUnnamedAggregation = false;
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
            else
            {
                const auto numArgs = context->argument.size();
                if (numArgs > helpers.top().functionBuilder.size())
                {
                    throw InvalidQuerySyntax(
                        "Function '{}' expects {} arguments but only {} are available",
                        funcName,
                        numArgs,
                        helpers.top().functionBuilder.size());
                }
                auto argsBegin = helpers.top().functionBuilder.end() - static_cast<std::ptrdiff_t>(numArgs);
                std::vector<LogicalFunction> funcArgs(argsBegin, helpers.top().functionBuilder.end());
                if (auto logicalFunction = LogicalFunctionProvider::tryProvide(funcName, std::move(funcArgs)))
                {
                    helpers.top().functionBuilder.resize(helpers.top().functionBuilder.size() - numArgs);
                    helpers.top().functionBuilder.push_back(*logicalFunction);
                }
                else
                {
                    throw InvalidQuerySyntax("Unknown (aggregation) function: {}, resolved to token type: {}", funcName, tokenType);
                }
            }
    }

    /// For aggregation functions, generate an auto-name for the result field and replace the raw
    /// field access in functionBuilder with a reference to the aggregation output. This enables
    /// post-aggregation arithmetic like MEDIAN(i8) + UINT64(1).
    if (isAggregation)
    {
        helpers.top().hasUnnamedAggregation = true;
        const auto onField = helpers.top().functionBuilder.back().getAs<UnboundFieldAccessLogicalFunction>().get();
        helpers.top().functionBuilder.pop_back();
        const auto autoName = fmt::format("{}_{}", onField.getFieldName(), funcName);
        const auto asField = bindIdentifier(autoName);
        const auto [aggFunc, asName] = helpers.top().windowAggs.back();
        helpers.top().windowAggs.pop_back();
        helpers.top().windowAggs.emplace_back(aggFunc, std::optional{asField});
        helpers.top().functionBuilder.emplace_back(UnboundFieldAccessLogicalFunction(asField));
    }
}

void AntlrSQLQueryPlanCreator::exitThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext* context)
{
    helpers.top().minimumCount = std::stoi(context->getText());
}

void AntlrSQLQueryPlanCreator::enterAnonymousSource(AntlrSQLParser::AnonymousSourceContext* context)
{
    const auto type = bindIdentifier(context->type);

    const auto parameters = bindConfigOptions(context->parameters->namedConfigExpression());

    helpers.top().setAnonymousSource(type, parameters);
}

void AntlrSQLQueryPlanCreator::enterSetOperation(AntlrSQLParser::SetOperationContext*)
{
    AntlrSQLHelper helper;
    helper.isSetOperation = true;
    helpers.push(helper);
}

void AntlrSQLQueryPlanCreator::exitSetOperation(AntlrSQLParser::SetOperationContext* context)
{
    INVARIANT(!helpers.empty(), "the set operation helper should not disappear before this function call");

    auto& helperPlans = helpers.top().queryPlans;
    if (helperPlans.size() < 2)
    {
        throw InvalidQuerySyntax("Union does not have sufficient amount of QueryPlans for unifying.");
    }

    auto rightQuery = std::move(helperPlans.back());
    helperPlans.pop_back();
    auto leftQuery = std::move(helperPlans.back());
    helperPlans.pop_back();
    helpers.pop();

    auto queryPlan = LogicalPlanBuilder::addUnion(std::move(leftQuery), std::move(rightQuery));
    if (!helpers.empty())
    {
        /// we are in a subquery
        helpers.top().queryPlans.push_back(std::move(queryPlan));
    }
    else
    {
        queryPlans.push(std::move(queryPlan));
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

void AntlrSQLQueryPlanCreator::enterModelInferenceRelation(AntlrSQLParser::ModelInferenceRelationContext* context)
{
    helpers.top().isModelInference = true;
    AntlrSQLBaseListener::enterModelInferenceRelation(context);
}

namespace
{
/// Recursively build a LogicalPlan from a modelInferenceSource context.
/// For subquery inputs, the inner query plan is already on the queryPlans vector (processed by the listener).
LogicalPlan buildModelInferencePlan(AntlrSQLParser::ModelInferenceSourceContext* ctx, std::vector<LogicalPlan>& queryPlans)
{
    auto modelName = bindIdentifier(ctx->modelName);

    auto* input = ctx->modelInferenceInput();
    const LogicalPlan childPlan = [&]() -> LogicalPlan
    {
        if (auto* streamName = dynamic_cast<AntlrSQLParser::ModelInferenceStreamNameContext*>(input))
        {
            std::string name;
            for (auto* part : streamName->multipartIdentifier()->parts)
            {
                if (!name.empty())
                {
                    name += "$";
                }
                name += fmt::format("{}", bindIdentifier(part->identifier()));
            }
            return LogicalPlanBuilder::createLogicalPlan(bindIdentifier(std::move(name)));
        }
        if (auto* nested = dynamic_cast<AntlrSQLParser::ModelInferenceNestedContext*>(input))
        {
            return buildModelInferencePlan(nested->modelInferenceSource(), queryPlans);
        }
        if (dynamic_cast<AntlrSQLParser::ModelInferenceSubqueryContext*>(input))
        {
            /// The subquery has already been processed by the listener; its plan is on the queryPlans vector.
            if (queryPlans.empty())
            {
                throw InvalidQuerySyntax("MODEL_INFERENCE subquery plan not found");
            }
            auto plan = std::move(queryPlans.back());
            queryPlans.pop_back();
            return plan;
        }
        throw InvalidQuerySyntax("MODEL_INFERENCE: unrecognized input type");
    }();

    return LogicalPlanBuilder::addInferModel(std::move(modelName), childPlan);
}
}

void AntlrSQLQueryPlanCreator::exitModelInferenceRelation(AntlrSQLParser::ModelInferenceRelationContext* context)
{
    auto plan = buildModelInferencePlan(context->modelInferenceSource(), helpers.top().queryPlans);
    helpers.top().queryPlans.push_back(std::move(plan));
    helpers.top().isModelInference = false;
    AntlrSQLBaseListener::exitModelInferenceRelation(context);
}
}

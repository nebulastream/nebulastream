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

#include <regex>

#include <AntlrSQLParser/AntlrSQLQueryPlanCreator.hpp>
#include <Functions/LogicalFunctions/NodeFunctionAnd.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Functions/LogicalFunctions/NodeFunctionOr.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/ValueTypes/BasicValue.hpp>

namespace NES::Parsers
{
std::string queryPlanToString(const QueryPlanPtr queryPlan)
{
    std::regex r2("[0-9]");
    std::regex r1("  ");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r1, "");
    queryPlanStr = std::regex_replace(queryPlanStr, r2, "");
    return queryPlanStr;
}

QueryPlanPtr AntlrSQLQueryPlanCreator::getQueryPlan() const
{
    /// Todo #421: support multiple sinks
    return QueryPlanBuilder::addSink(std::move(sinkNames.front()), queryPlans.top());
}
Windowing::TimeMeasure AntlrSQLQueryPlanCreator::buildTimeMeasure(int size, const std::string& timebase)
{
    if (timebase == "MS")
        return Milliseconds(size);
    if (timebase == "SEC")
        return Seconds(size);
    if (timebase == "MIN")
        return Minutes(size);
    if (timebase == "HOUR")
        return Hours(size);

    /// default: return milliseconds
    return Milliseconds(0);
}

void AntlrSQLQueryPlanCreator::poppush(AntlrSQLHelper helper)
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
    std::shared_ptr<NES::NodeFunction> expression;

    auto rightExpression = helper.expressionBuilder.back();
    helper.expressionBuilder.pop_back();
    auto leftExpression = helper.expressionBuilder.back();
    helper.expressionBuilder.pop_back();

    std::string opText = context->op->getText();

    if (opText == "AND")
    {
        expression = NES::NodeFunctionAnd::create(leftExpression, rightExpression);
    }
    else if (opText == "OR")
    {
        expression = NES::NodeFunctionOr::create(leftExpression, rightExpression);
    }
    else
    {
        throw InvalidQuerySyntax("Unknown binary function in SQL query: {}", context->getText());
    }

    helper.expressionBuilder.push_back(expression);
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
    AntlrSQLHelper helper = helpers.top();
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
    auto temp = context->getText();
    poppush(helper);
    AntlrSQLBaseListener::enterComparisonOperator(context);
}
void AntlrSQLQueryPlanCreator::exitArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    NES::NodeFunctionPtr expression;

    auto rightExpression = helper.expressionBuilder.back();
    helper.expressionBuilder.pop_back();
    auto leftExpression = helper.expressionBuilder.back();
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
    NES::NodeFunctionPtr expression;

    auto innerExpression = helper.expressionBuilder.back();
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

    /// Get Index of  Parent Rule to check type of parent rule in conditions
    auto parentContext = static_cast<AntlrSQLParser::IdentifierContext*>(context->parent);
    size_t parentRuleIndex = -1;
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

    /// Get Index of  Parent Rule to check type of parent rule in conditions
    auto parentContext = static_cast<AntlrSQLParser::IdentifierContext*>(context->parent);
    size_t parentRuleIndex = -1;
    if (parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }
    if (helper.isGroupBy)
    {
        NodeFunctionFieldAccessPtr key = Util::as<NodeFunctionFieldAccess>(NodeFunctionFieldAccess::create(context->getText()));
        helper.groupByFields.push_back(key);
    }
    else if ((helper.isWhereOrHaving || helper.isSelect || helper.isWindow) && AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        /// add identifiers in select, window, where and having clauses to the expression builder list
        helper.expressionBuilder.push_back(NES::Attribute(context->getText()));
    }
    else if (helper.isFrom && !helper.isJoinRelation && AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        /// get main source name
        helper.setSource(context->getText());
    }
    else if (AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex && !helper.isFunctionCall && !helper.isJoinRelation)
    {
        /// handle renames of identifiers
        if (helper.isArithmeticBinary)
        {
            throw InvalidQuerySyntax("There must not be a binary arithmetic token at this point: {}", context->getText());
        }
        if ((helper.isWhereOrHaving || helper.isSelect))
        {
            NodeFunctionPtr attr = helper.expressionBuilder.back();
            helper.expressionBuilder.pop_back();
            if (helper.identCountHelper == 1)
            {
                /// rename of a single attribute
                FunctionItem cattr = static_cast<FunctionItem>(attr);
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
    else if (helper.isFunctionCall && AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        auto aggFunc = helper.windowAggs.back();
        helper.windowAggs.pop_back();
        aggFunc = aggFunc->as(Attribute(context->getText()));
        helper.windowAggs.push_back(aggFunc);
    }
    else if (helper.isJoinRelation && AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helper.joinKeyRelationHelper.push_back(Attribute(context->getText()));
    }
    else if (helper.isJoinRelation && AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        helper.joinSources.push_back(context->getText());
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
    auto parentContext = static_cast<AntlrSQLParser::IdentifierContext*>(context->parent);
    size_t parentRuleIndex = -1;
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
    helper.timeUnit = context->getText();
    poppush(helper);
}
void AntlrSQLQueryPlanCreator::exitSizeParameter(AntlrSQLParser::SizeParameterContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.size = std::stoi(context->children[1]->getText());
    poppush(helper);

    AntlrSQLBaseListener::exitSizeParameter(context);
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
    TimeMeasure timeMeasure = buildTimeMeasure(helper.size, helper.timeUnit);
    helper.windowType = Windowing::TumblingWindow::of(EventTime(Attribute(helper.timestamp)), timeMeasure);
    poppush(helper);
    AntlrSQLBaseListener::exitTumblingWindow(context);
}
void AntlrSQLQueryPlanCreator::exitSlidingWindow(AntlrSQLParser::SlidingWindowContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    TimeMeasure timeMeasure = buildTimeMeasure(helper.size, helper.timeUnit);
    TimeMeasure slidingLength = buildTimeMeasure(1, helper.timeUnit);
    helper.windowType = Windowing::SlidingWindow::of(EventTime(Attribute(helper.timestamp)), timeMeasure, slidingLength);
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
    AntlrSQLHelper helper = helpers.top();
    poppush(helper);
    AntlrSQLBaseListener::enterHavingClause(context);
}
void AntlrSQLQueryPlanCreator::exitHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helpers.top().isWhereOrHaving = false;
    AntlrSQLHelper helper = helpers.top();
    if (helper.expressionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There were more than 1 functions in the expressionBuilder in exitWhereClause.");
    }
    helper.addHavingClause(helper.expressionBuilder.back());
    helper.expressionBuilder.clear();
    poppush(helper);
    AntlrSQLBaseListener::exitHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitComparison(AntlrSQLParser::ComparisonContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    NES::NodeFunctionPtr expression;
    if (!helper.isJoinRelation)
    {
        INVARIANT(helper.expressionBuilder.size() >= 2, "Requires two expressions")
        auto rightExpression = helper.expressionBuilder.back();
        helper.expressionBuilder.pop_back();
        auto leftExpression = helper.expressionBuilder.back();
        helper.expressionBuilder.pop_back();
        std::string op = context->children.at(1)->getText();
        if (op == "==" || op == "=")
        {
            expression = NES::NodeFunctionEquals::create(leftExpression, rightExpression);
        }
        else if (op == "!=")
        {
            const auto equalsExpression = NES::NodeFunctionEquals::create(leftExpression, rightExpression);
            expression = NES::NodeFunctionNegate::create(equalsExpression);
        }
        else
        {
            throw InvalidQuerySyntax("Unknown Comparison Operator: {}", op);
        }
        helper.expressionBuilder.push_back(expression);
        poppush(helper);
    }
    AntlrSQLBaseListener::enterComparison(context);
}
void AntlrSQLQueryPlanCreator::enterJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.joinKeyRelationHelper.clear();
    helper.isJoinRelation = true;
    poppush(helper);
    AntlrSQLBaseListener::enterJoinRelation(context);
}
void AntlrSQLQueryPlanCreator::exitLogicalNot(AntlrSQLParser::LogicalNotContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    NES::NodeFunctionPtr expression;

    auto innerExpression = helper.expressionBuilder.back();
    helper.expressionBuilder.pop_back();

    expression = !innerExpression;
    helper.expressionBuilder.push_back(expression);
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
    auto constFunctionItem = FunctionItem(NES::NodeFunctionConstantValue::create(valueType));
    helper.expressionBuilder.push_back(constFunctionItem);
    poppush(helper);
}

void AntlrSQLQueryPlanCreator::exitRealIdent(AntlrSQLParser::RealIdentContext* context)
{
    AntlrSQLBaseListener::exitRealIdent(context);
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
void AntlrSQLQueryPlanCreator::exitJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    AntlrSQLHelper helper = helpers.top();
    helper.isJoinRelation = false;
    if (helper.joinKeyRelationHelper.size() >= 2)
    {
        helper.joinKeys.emplace_back(helper.joinKeyRelationHelper[0], helper.joinKeyRelationHelper[1]);
    }
    else
    {
        throw InvalidQuerySyntax("Join keys must be specified explicitly.");
    }
    if (helper.joinSources.size() == helper.joinSourceRenames.size() + 1)
    {
        helper.joinSourceRenames.emplace_back("");
    }
    poppush(helper);
    AntlrSQLBaseListener::exitJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::exitSetOperation(AntlrSQLParser::SetOperationContext* context)
{
    if (queryPlans.size() < 2)
    {
        throw InvalidQuerySyntax("Union does not have sufficient amount of QueryPlans for unifying.");
    }

    auto rightQuery = queryPlans.top();
    queryPlans.pop();
    auto leftQuery = queryPlans.top();
    queryPlans.pop();
    auto queryPlan = QueryPlanBuilder::addUnion(leftQuery, rightQuery);
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

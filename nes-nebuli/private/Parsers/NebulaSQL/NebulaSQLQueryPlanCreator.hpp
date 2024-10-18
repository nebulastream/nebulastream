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

#ifndef NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLQUERYPLANCREATOR_HPP_
#define NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLQUERYPLANCREATOR_HPP_

#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Functions/LogicalFunctions/NodeFunctionAnd.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreaterEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreater.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLessEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionOr.hpp>
#include <Parsers/NebulaSQL/NebulaSQLHelper.hpp>
#include <NebulaSQLBaseListener.h>
#include <NebulaSQLLexer.h>
#include <Plans/Query/QueryPlan.hpp>


namespace NES::Parsers
{
class NebulaSQLQueryPlanCreator : public NebulaSQLBaseListener
{
private:
    std::stack<NebulaSQLHelper> helpers;
    QueryPlanPtr completeQueryPlan;
    std::string sinkName;
    std::stack<QueryPlanPtr> queryPlans;


public:
    QueryPlanPtr getQueryPlan() const;
    void enterSelectClause(NebulaSQLParser::SelectClauseContext* context) override;
    void enterSinkClause(NebulaSQLParser::SinkClauseContext* context) override;
    void exitLogicalBinary(NebulaSQLParser::LogicalBinaryContext* context) override;
    void enterFromClause(NebulaSQLParser::FromClauseContext* context) override;
    void exitSelectClause(NebulaSQLParser::SelectClauseContext* context) override;
    void enterNamedExpressionSeq(NebulaSQLParser::NamedExpressionSeqContext* context) override;
    void exitFromClause(NebulaSQLParser::FromClauseContext* context) override;
    void enterWhereClause(NebulaSQLParser::WhereClauseContext* context) override;
    void exitWhereClause(NebulaSQLParser::WhereClauseContext* context) override;
    void enterComparisonOperator(NebulaSQLParser::ComparisonOperatorContext* context) override;
    void enterUnquotedIdentifier(NebulaSQLParser::UnquotedIdentifierContext* context) override;
    void enterIdentifier(NebulaSQLParser::IdentifierContext* context) override;
    void enterPrimaryQuery(NebulaSQLParser::PrimaryQueryContext* context) override;
    void exitPrimaryQuery(NebulaSQLParser::PrimaryQueryContext* context) override;
    void enterTimeUnit(NebulaSQLParser::TimeUnitContext* context) override;
    void exitSizeParameter(NebulaSQLParser::SizeParameterContext* context) override;
    void exitTimestampParameter(NebulaSQLParser::TimestampParameterContext* context) override;
    void exitTumblingWindow(NebulaSQLParser::TumblingWindowContext* context) override;
    void exitSlidingWindow(NebulaSQLParser::SlidingWindowContext* context) override;
    void enterNamedExpression(NebulaSQLParser::NamedExpressionContext* context) override;
    void exitNamedExpression(NebulaSQLParser::NamedExpressionContext* context) override;
    void enterFunctionCall(NebulaSQLParser::FunctionCallContext* context) override;
    void exitFunctionCall(NebulaSQLParser::FunctionCallContext* context) override;
    void enterHavingClause(NebulaSQLParser::HavingClauseContext* context) override;
    void exitHavingClause(NebulaSQLParser::HavingClauseContext* context) override;
    void enterJoinRelation(NebulaSQLParser::JoinRelationContext* context) override;
    void poppush(NebulaSQLHelper helper);
    void exitComparison(NebulaSQLParser::ComparisonContext* context) override;
    void exitArithmeticUnary(NebulaSQLParser::ArithmeticUnaryContext* context) override;
    void exitArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext* context) override;
    void exitLogicalNot(NebulaSQLParser::LogicalNotContext*) override;
    void exitConstantDefault(NebulaSQLParser::ConstantDefaultContext* context) override;
    void exitRealIdent(NebulaSQLParser::RealIdentContext* context) override;
    TimeMeasure buildTimeMeasure(int size, const std::string& timebase);
    void exitThresholdBasedWindow(NebulaSQLParser::ThresholdBasedWindowContext* context) override;
    void exitWindowClause(NebulaSQLParser::WindowClauseContext* context) override;
    void enterWindowClause(NebulaSQLParser::WindowClauseContext* context) override;
    void exitThresholdMinSizeParameter(NebulaSQLParser::ThresholdMinSizeParameterContext* context) override;
    void enterValueExpressionDefault(NebulaSQLParser::ValueExpressionDefaultContext* context) override;
    void exitJoinRelation(NebulaSQLParser::JoinRelationContext* context) override;
    void exitSetOperation(NebulaSQLParser::SetOperationContext* context) override;
    void enterAggregationClause(NebulaSQLParser::AggregationClauseContext* context) override;
    void exitAggregationClause(NebulaSQLParser::AggregationClauseContext* context) override;
};


} // namespace NES::Parsers

#endif // NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLQUERYPLANCREATOR_HPP_

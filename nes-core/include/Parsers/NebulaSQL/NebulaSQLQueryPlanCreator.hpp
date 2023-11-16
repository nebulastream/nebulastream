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

#include <Parsers/NebulaSQL/NebulaSQLOperatorNode.hpp>
#include <Parsers/NebulaSQL/gen/NebulaSQLBaseListener.h>
#include <Parsers/NebulaSQL/NebulaSQLHelper.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Operators//Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/OrExpressionNode.hpp>


namespace NES::Parsers {
        class NebulaSQLQueryPlanCreator : public NebulaSQLBaseListener{
          private:
            std::string currentLeftExp;
            std::string currentRightExp;
            std::stack<NebulaSQLHelper> helpers;
            QueryPlanPtr completeQueryPlan;

          public:
            QueryPlanPtr getQueryPlan() const;
            void enterSelectClause(NebulaSQLParser::SelectClauseContext* context) override;
            void enterSinkClause(NebulaSQLParser::SinkClauseContext* context) override;
            void exitLogicalBinary(NebulaSQLParser::LogicalBinaryContext* context) override;
            void enterFromClause(NebulaSQLParser::FromClauseContext* context) override;
            void exitSelectClause(NebulaSQLParser::SelectClauseContext* context) override;
            void enterNamedExpressionSeq(NebulaSQLParser::NamedExpressionSeqContext* context) override;
            void exitNamedExpressionSeq(NebulaSQLParser::NamedExpressionSeqContext* context) override;
            void exitFromClause(NebulaSQLParser::FromClauseContext* context) override;
            void enterWhereClause(NebulaSQLParser::WhereClauseContext* context) override;
            void exitWhereClause(NebulaSQLParser::WhereClauseContext* context) override;
            void enterComparisonOperator(NebulaSQLParser::ComparisonOperatorContext* context) override;
            void enterConstantDefault(NebulaSQLParser::ConstantDefaultContext* context) override;
            void enterArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext* context) override;
            void exitErrorCapturingIdentifier(NebulaSQLParser::ErrorCapturingIdentifierContext* context) override;
            void enterUnquotedIdentifier(NebulaSQLParser::UnquotedIdentifierContext* context) override;
            void enterIdentifier(NebulaSQLParser::IdentifierContext* context) override;
            void enterPrimaryQuery(NebulaSQLParser::PrimaryQueryContext* context) override;
            void exitPrimaryQuery(NebulaSQLParser::PrimaryQueryContext* context) override;
            void enterTimeUnit(NebulaSQLParser::TimeUnitContext* context) override;
            void exitSizeParameter(NebulaSQLParser::SizeParameterContext* context) override;
            void exitTimestampParameter(NebulaSQLParser::TimestampParameterContext* context) override;
            void exitAdvancebyParameter(NebulaSQLParser::AdvancebyParameterContext* context) override;
            void exitWindowedAggregationClause(NebulaSQLParser::WindowedAggregationClauseContext* context) override;
            void exitTumblingWindow(NebulaSQLParser::TumblingWindowContext* context) override;
            void exitSlidingWindow(NebulaSQLParser::SlidingWindowContext* context) override;
            void exitCountBasedWindow(NebulaSQLParser::CountBasedWindowContext* context) override;
            void exitCountBasedTumbling(NebulaSQLParser::CountBasedTumblingContext* context) override;
            void enterNamedExpression(NebulaSQLParser::NamedExpressionContext* context) override;
            void exitNamedExpression(NebulaSQLParser::NamedExpressionContext* context) override;
            void enterFunctionCall(NebulaSQLParser::FunctionCallContext* context) override;
            void exitFunctionCall(NebulaSQLParser::FunctionCallContext* context) override;
            void enterHavingClause(NebulaSQLParser::HavingClauseContext* context) override;
            void exitHavingClause(NebulaSQLParser::HavingClauseContext* context) override;
            void enterComparison(NebulaSQLParser::ComparisonContext* context) override;
            void exitAggregationClause(NebulaSQLParser::AggregationClauseContext* context) override;
            void enterJoinRelation(NebulaSQLParser::JoinRelationContext* context) override;
            void enterColumnReference(NebulaSQLParser::ColumnReferenceContext* context) override;
            void enterDereference(NebulaSQLParser::DereferenceContext* context) override;
            void exitDereference(NebulaSQLParser::DereferenceContext* context) override;
            void exitJoinCriteria(NebulaSQLParser::JoinCriteriaContext* context) override;
            void enterTableName(NebulaSQLParser::TableNameContext* context) override;
            void enterWatermarkClause(NebulaSQLParser::WatermarkClauseContext* context) override;
            void enterWatermarkParameters(NebulaSQLParser::WatermarkParametersContext* context) override;
            void exitWatermarkClause(NebulaSQLParser::WatermarkClauseContext* context) override;
            void exitSingleStatement(NebulaSQLParser::SingleStatementContext* context) override;
            void poppush(NebulaSQLHelper helper);
            void exitComparison(NebulaSQLParser::ComparisonContext* context) override;
            void exitArithmeticUnary(NebulaSQLParser::ArithmeticUnaryContext* context) override;
            void exitArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext* context) override;
            void exitLogicalNot(NebulaSQLParser::LogicalNotContext*) override;
            void exitConstantDefault(NebulaSQLParser::ConstantDefaultContext* context) override;
            void exitRealIdent(NebulaSQLParser::RealIdentContext* context) override;
            void exitIdentifier(NebulaSQLParser::IdentifierContext* context) override;
        };



    }// namespace NES::Parsers

#endif// NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLQUERYPLANCREATOR_HPP_

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
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>


namespace NES::Parsers {
        class NebulaSQLQueryPlanCreator : public NebulaSQLBaseListener{
          private:
            NebulaSQLHelper helper;
            int32_t sourceCounter = 0;
            int32_t lastSeenSourcePtr = -1;
            int32_t nodeId = 0;
            //bool inWhere = false;
            //bool leftFilter = true;
            std::string currentLeftExp;
            std::string currentRightExp;

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
            void exitPredicated(NebulaSQLParser::PredicatedContext* context) override;
            void enterArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext* context) override;
            void exitErrorCapturingIdentifier(NebulaSQLParser::ErrorCapturingIdentifierContext* context) override;
            void enterUnquotedIdentifier(NebulaSQLParser::UnquotedIdentifierContext* context) override;
            void enterIdentifier(NebulaSQLParser::IdentifierContext* context) override;
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
        };



    }// namespace NES::Parsers

#endif// NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLQUERYPLANCREATOR_HPP_

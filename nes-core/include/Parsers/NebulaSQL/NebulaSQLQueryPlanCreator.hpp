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

  public:
    QueryPlanPtr buildQueryPlan();

    void enterSelectClause(NebulaSQLParser::SelectClauseContext* context) override;
    void enterRelation(NebulaSQLParser::RelationContext* context) override;
    void enterSinkClause(NebulaSQLParser::SinkClauseContext* context) override;
    void exitLogicalBinary(NebulaSQLParser::LogicalBinaryContext* context) override;
    void exitLogicalNot(NebulaSQLParser::LogicalNotContext* context) override;
    void exitPredicated(NebulaSQLParser::PredicatedContext* context) override;


};



}// namespace NES::Parsers

#endif// NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLQUERYPLANCREATOR_HPP_

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


namespace NES::Parsers {
class NebulaSQLQueryPlanCreator : public NebulaSQLBaseListener{
  public:

    void enterSelectClause(NebulaSQLParser::SelectClauseContext *context) override;

    void enterFromClause(NebulaSQLParser::FromClauseContext *context) override;

    void enterRelation(NebulaSQLParser::RelationContext *context) override;

    void enterSinkClause(NebulaSQLParser::SinkClauseContext *context) override;



  private:
    NebulaSQLHelper helper;

    int32_t sourceCounter = 0;
    int32_t lastSeenSourcePtr = -1;
    int32_t nodeId = 0;
    bool inWhere = false;
    bool leftFilter = true;
    std::string currentLeftExp;
    std::string currentRightExp;
};



}// namespace NES::Parsers

#endif// NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLQUERYPLANCREATOR_HPP_

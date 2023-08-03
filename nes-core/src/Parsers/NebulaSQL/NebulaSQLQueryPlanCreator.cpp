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

#include <Parsers/NebulaSQL/NebulaSQLQueryPlanCreator.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>


namespace NES::Parsers {

void NebulaSQLQueryPlanCreator::enterSelectClause(NebulaSQLParser::SelectClauseContext *context) {
    for (auto namedExprContext : context->namedExpressionSeq()->namedExpression()) {
        auto expressionText = namedExprContext->expression()->getText();
        auto attribute = NES::Attribute(expressionText).getExpressionNode();
        helper.addProjectionField(attribute);
        // catach den alias
        if (namedExprContext->name) {
            std::string alias = namedExprContext->name->getText();
            //Stream Name ändern?
        }
    }
}


}


}// namespace NES::Parsers
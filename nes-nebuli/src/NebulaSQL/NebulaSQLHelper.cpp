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

#include <Parsers/NebulaSQL/NebulaSQLHelper.hpp>

namespace NES::Parsers
{

/// Getter and Setter for the map/list entries of each clause
const std::string NebulaSQLHelper::getSource() const
{
    return this->source;
}
const std::list<std::shared_ptr<NES::NodeFunction>>& NebulaSQLHelper::getWhereClauses() const
{
    return this->whereClauses;
}
const std::list<std::shared_ptr<NES::NodeFunction>>& NebulaSQLHelper::getHavingClauses() const
{
    return this->havingClauses;
}

const std::vector<std::shared_ptr<NES::NodeFunction>>& NebulaSQLHelper::getProjectionFields() const
{
    return this->projectionFields;
}

/// methods to update the clauses maps/lists
void NebulaSQLHelper::addSource(std::string sourceName)
{
    this->source = sourceName;
}
void NebulaSQLHelper::addWhereClause(std::shared_ptr<NES::NodeFunction> expressionNode)
{
    auto pos = this->whereClauses.begin();
    this->whereClauses.insert(pos, expressionNode);
}
void NebulaSQLHelper::addHavingClause(std::shared_ptr<NES::NodeFunction> expressionNode)
{
    auto pos = this->havingClauses.begin();
    this->havingClauses.insert(pos, expressionNode);
}
void NebulaSQLHelper::addProjectionField(std::shared_ptr<NES::NodeFunction> expressionNode)
{
    this->projectionFields.push_back(expressionNode);
}

const NES::Windowing::WindowTypePtr NebulaSQLHelper::getWindowType() const
{
    return NES::Windowing::WindowTypePtr();
}

void NebulaSQLHelper::addMapExpression(std::shared_ptr<NES::NodeFunctionFieldAssignment> expressionNode)
{
    auto pos = this->mapBuilder.begin();
    this->mapBuilder.insert(pos, expressionNode);
}
std::vector<std::shared_ptr<NES::NodeFunctionFieldAssignment>> NebulaSQLHelper::getMapExpressions() const
{
    return this->mapBuilder;
}
void NebulaSQLHelper::setMapExpressions(const std::vector<std::shared_ptr<NES::NodeFunctionFieldAssignment>> expressions)
{
    this->mapBuilder = expressions;
}

}

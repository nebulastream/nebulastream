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

#include <utility>
#include <AntlrSQLParser/AntlrSQLHelper.hpp>

namespace NES::Parsers
{

/// Getter and Setter for the map/list entries of each clause
const std::string AntlrSQLHelper::getSource() const
{
    return this->source;
}
std::vector<NES::Logical::Function>& AntlrSQLHelper::getWhereClauses()
{
    return whereClauses;
}
std::vector<Logical::Function>& AntlrSQLHelper::getHavingClauses()
{
    return havingClauses;
}
std::vector<Logical::Function>& AntlrSQLHelper::getProjectionFields()
{
    return projectionFields;
}

/// methods to update the clauses maps/lists
void AntlrSQLHelper::setSource(std::string sourceName)
{
    this->source = sourceName;
}
void AntlrSQLHelper::addWhereClause(Logical::Function expressionNode)
{
    this->whereClauses.emplace_back(std::move(expressionNode));
}
void AntlrSQLHelper::addHavingClause(Logical::Function expressionNode)
{
    this->havingClauses.emplace_back(std::move(expressionNode));
}
void AntlrSQLHelper::addProjectionField(Logical::FieldAccessFunction expressionNode)
{
    this->projectionFields.push_back(std::move(expressionNode));
}

std::shared_ptr<Windowing::WindowType> AntlrSQLHelper::getWindowType()
{
    return {};
}

void AntlrSQLHelper::addMapExpression(Logical::FieldAssignmentFunction expressionNode)
{
    auto pos = this->mapBuilder.begin();
    this->mapBuilder.insert(pos, expressionNode);
}
std::vector<Logical::FieldAssignmentFunction>& AntlrSQLHelper::getMapExpressions()
{
    return mapBuilder;
}

void AntlrSQLHelper::setMapExpressions(std::vector<Logical::FieldAssignmentFunction> expressions)
{
    this->mapBuilder = std::move(expressions);
}

}

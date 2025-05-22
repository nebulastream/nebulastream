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
#include <Types/WindowType.hpp>

namespace NES::Parsers
{

/// Getter and Setter for the map/list entries of each clause
const std::string AntlrSQLHelper::getSource() const
{
    return this->source;
}

const std::vector<std::shared_ptr<NES::NodeFunction>>& AntlrSQLHelper::getWhereClauses() const
{
    return this->whereClauses;
}

const std::vector<std::shared_ptr<NES::NodeFunction>>& AntlrSQLHelper::getHavingClauses() const
{
    return this->havingClauses;
}

const std::vector<std::shared_ptr<NES::NodeFunction>>& AntlrSQLHelper::getProjectionFields() const
{
    return this->projectionFields;
}

/// methods to update the clauses maps/lists
void AntlrSQLHelper::setSource(std::string sourceName)
{
    this->source = sourceName;
}

void AntlrSQLHelper::addWhereClause(const std::shared_ptr<NES::NodeFunction>& expressionNode)
{
    this->whereClauses.emplace_back(expressionNode);
}

void AntlrSQLHelper::addHavingClause(const std::shared_ptr<NES::NodeFunction>& expressionNode)
{
    this->havingClauses.emplace_back(expressionNode);
}

void AntlrSQLHelper::addProjectionField(const std::shared_ptr<NES::NodeFunctionFieldAccess>& expressionNode)
{
    this->projectionFields.push_back(expressionNode);
}

std::shared_ptr<Windowing::WindowType> AntlrSQLHelper::getWindowType()
{
    return {};
}

void AntlrSQLHelper::addMapExpression(std::shared_ptr<NES::NodeFunctionFieldAssignment> expressionNode)
{
    auto pos = this->mapBuilder.begin();
    this->mapBuilder.insert(pos, std::move(expressionNode));
}

std::vector<std::shared_ptr<NES::NodeFunctionFieldAssignment>> AntlrSQLHelper::getMapExpressions() const
{
    return this->mapBuilder;
}

void AntlrSQLHelper::setMapExpressions(const std::vector<std::shared_ptr<NES::NodeFunctionFieldAssignment>> expressions)
{
    this->mapBuilder = expressions;
}

}

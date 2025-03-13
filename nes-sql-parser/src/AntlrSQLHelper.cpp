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
#include <WindowTypes/Types/WindowType.hpp>

namespace NES::Parsers
{

/// Getter and Setter for the map/list entries of each clause
const std::string AntlrSQLHelper::getSource() const
{
    return this->source;
}
std::vector<std::unique_ptr<NES::LogicalFunction>>& AntlrSQLHelper::getWhereClauses()
{
    return whereClauses;
}
std::vector<std::unique_ptr<NES::LogicalFunction>>& AntlrSQLHelper::getHavingClauses()
{
    return this->havingClauses;
}
std::vector<std::unique_ptr<NES::LogicalFunction>>& AntlrSQLHelper::getProjectionFields()
{
    return this->projectionFields;
}

/// methods to update the clauses maps/lists
void AntlrSQLHelper::setSource(std::string sourceName)
{
    this->source = sourceName;
}
void AntlrSQLHelper::addWhereClause(std::unique_ptr<NES::LogicalFunction> expressionNode)
{
    this->whereClauses.emplace_back(std::move(expressionNode));
}
void AntlrSQLHelper::addHavingClause(std::unique_ptr<NES::LogicalFunction> expressionNode)
{
    this->havingClauses.emplace_back(std::move(expressionNode));
}
void AntlrSQLHelper::addProjectionField(std::unique_ptr<NES::LogicalFunction> expressionNode)
{
    this->projectionFields.push_back(std::move(expressionNode));
}

const std::unique_ptr<NES::Windowing::WindowType> AntlrSQLHelper::getWindowType() const
{
    return std::unique_ptr<NES::Windowing::WindowType>();
}

void AntlrSQLHelper::addMapExpression(std::unique_ptr<NES::FieldAssignmentLogicalFunction> expressionNode)
{
    auto pos = this->mapBuilder.begin();
    this->mapBuilder.insert(pos, std::move(expressionNode));
}

void AntlrSQLHelper::setMapExpressions(std::vector<std::unique_ptr<NES::FieldAssignmentLogicalFunction>> expressions)
{
    this->mapBuilder = std::move(expressions);
}

}

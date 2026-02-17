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

#include <AntlrSQLParser/AntlrSQLHelper.hpp>

#include <optional>
#include <utility>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <Operators/DeltaLogicalOperator.hpp>
#include <CommonParserFunctions.hpp>

namespace NES::Parsers
{

/// Getter and Setter for the map/list entries of each clause
const std::string AntlrSQLHelper::getSource() const
{
    return this->source;
}

std::vector<LogicalFunction>& AntlrSQLHelper::getWhereClauses()
{
    return whereClauses;
}

std::vector<LogicalFunction>& AntlrSQLHelper::getHavingClauses()
{
    return havingClauses;
}

/// methods to update the clauses maps/lists
void AntlrSQLHelper::setSource(std::string sourceName)
{
    this->source = sourceName;
}

void AntlrSQLHelper::setInlineSource(const std::string& type, const ConfigMap& parameters)
{
    this->inlineSourceConfig = std::make_pair(type, parameters);
}

std::pair<std::string, ConfigMap> AntlrSQLHelper::getInlineSourceConfig()
{
    return this->inlineSourceConfig;
}

void AntlrSQLHelper::addWhereClause(LogicalFunction expressionNode)
{
    this->whereClauses.emplace_back(std::move(expressionNode));
}

void AntlrSQLHelper::addHavingClause(LogicalFunction expressionNode)
{
    this->havingClauses.emplace_back(std::move(expressionNode));
}

void AntlrSQLHelper::addProjection(std::optional<FieldIdentifier> identifier, LogicalFunction logicalFunction)
{
    this->projectionBuilder.emplace_back(std::move(identifier), std::move(logicalFunction));
}

std::vector<AntlrSQLHelper::Projection>& AntlrSQLHelper::getProjections()
{
    return projectionBuilder;
}

std::vector<DeltaLogicalOperator::DeltaExpression>& AntlrSQLHelper::getDeltaExpressions()
{
    return deltaExpressionBuilder;
}

}

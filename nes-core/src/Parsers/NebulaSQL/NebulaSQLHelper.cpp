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

#include <Parsers/NebulaSQL/NebulaSQLOperatorNode.hpp>
#include <Parsers/NebulaSQL/NebulaSQLHelper.hpp>

namespace NES::Parsers {

//Getter and Setter for the map/list entries of each clause
const std::string NebulaSQLHelper::getSource() const { return this->source; }
const std::list<ExpressionNodePtr>& NebulaSQLHelper::getExpressions() const { return this->expressionList; }
const std::vector<ExpressionNodePtr>& NebulaSQLHelper::getProjectionFields() const { return this->projectionFields; }

// methods to update the clauses maps/lists
void NebulaSQLHelper::addSource(std::string sourceName) { this->source=sourceName; }
void NebulaSQLHelper::addExpression(ExpressionNodePtr expressionNode) {
    auto pos = this->expressionList.begin();
    this->expressionList.insert(pos, expressionNode);
}
void NebulaSQLHelper::addProjectionField(ExpressionNodePtr expressionNode) { this->projectionFields.push_back(expressionNode); }

const NES::Windowing::WindowTypePtr NebulaSQLHelper::getWindowType() const { return NES::Windowing::WindowTypePtr(); }

void NebulaSQLHelper::addMapExpression(FieldAssignmentExpressionNodePtr expressionNode) {
    auto pos = this->mapBuilder.begin();
    this->mapBuilder.insert(pos, expressionNode);
}
std::vector<FieldAssignmentExpressionNodePtr> NebulaSQLHelper::getMapExpressions() const { return this->mapBuilder; }
void NebulaSQLHelper::setMapExpressions(const std::vector<FieldAssignmentExpressionNodePtr> expressions) { this->mapBuilder = expressions; }

}// namespace NES::Parsers

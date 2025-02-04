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

#include <Parsers/NebulaPSL/NebulaPSLOperator.hpp>
#include <Parsers/NebulaPSL/NebulaPSLPattern.hpp>

namespace NES::Parsers {

//Getter and Setter for the map/list entries of each clause
const std::map<int32_t, std::string>& NebulaPSLPattern::getSources() const { return this->sourceList; }
const std::map<std::string, std::string>& NebulaPSLPattern::getAliasList() const { return this->aliasList; }
const std::map<int32_t, NebulaPSLOperator>& NebulaPSLPattern::getOperatorList() const { return this->operatorList; }
const std::list<ExpressionNodePtr>& NebulaPSLPattern::getExpressions() const { return this->expressionList; }
const std::vector<ExpressionNodePtr>& NebulaPSLPattern::getProjectionFields() const { return this->projectionFields; }
const std::list<SinkDescriptorPtr>& NebulaPSLPattern::getSinks() const { return this->sinkList; }
Window NebulaPSLPattern::getWindow() const { return this->window; }
void NebulaPSLPattern::setWindow(const Window window) { this->window = window; }
// methods to update the clauses maps/lists
void NebulaPSLPattern::addSource(std::pair<int32_t, std::basic_string<char>> sourcePair) { this->sourceList.insert(sourcePair); }
void NebulaPSLPattern::updateSource(const int32_t key, std::string sourceName) { this->sourceList[key] = sourceName; }
void NebulaPSLPattern::updateAliasList(std::string aliasName, std::string sourceName) {
    this->aliasList.insert({aliasName, sourceName});
}

void NebulaPSLPattern::addExpression(ExpressionNodePtr expressionNode) {
    auto pos = this->expressionList.begin();
    this->expressionList.insert(pos, expressionNode);
}
void NebulaPSLPattern::addSink(SinkDescriptorPtr sinkDescriptor) { this->sinkList.push_back(sinkDescriptor); }
void NebulaPSLPattern::addProjectionField(ExpressionNodePtr expressionNode) { this->projectionFields.push_back(expressionNode); }
void NebulaPSLPattern::addOperator(NebulaPSLOperator operatorNode) {
    this->operatorList.insert(std::pair<uint32_t, NebulaPSLOperator>(operatorNode.getId(), operatorNode));
}
}// namespace NES::Parsers

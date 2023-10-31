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
const std::map<int32_t, std::string>& NebulaSQLHelper::getSources() const { return this->sourceList; }
void NebulaSQLHelper::setSources(const std::map<int32_t, std::string>& sources) { this->sourceList = sources; }
const std::map<int32_t, NebulaSQLOperatorNode>& NebulaSQLHelper::getOperatorList() const { return this->operatorList; }
void NebulaSQLHelper::setOperatorList(const std::map<int32_t, NebulaSQLOperatorNode>& operatorList) {
    this->operatorList = operatorList;
}
const std::list<ExpressionNodePtr>& NebulaSQLHelper::getExpressions() const { return this->expressionList; }
void NebulaSQLHelper::setExpressions(const std::list<ExpressionNodePtr>& expressions) { this->expressionList = expressions; }
const std::vector<ExpressionNodePtr>& NebulaSQLHelper::getProjectionFields() const { return this->projectionFields; }
void NebulaSQLHelper::setProjectionFields(const std::vector<ExpressionNodePtr>& projectionFields) {
    this->projectionFields = projectionFields;
}
const std::list<SinkDescriptorPtr>& NebulaSQLHelper::getSinks() const { return this->sinkList; }
void NebulaSQLHelper::setSinks(const std::list<SinkDescriptorPtr>& sinks) { this->sinkList = sinks; }
const std::pair<std::string, int32_t>& NebulaSQLHelper::getWindow() const { return this->window; }
void NebulaSQLHelper::setWindow(const std::pair<std::string, int32_t>& window) { this->window = window; }
// methods to update the clauses maps/lists
void NebulaSQLHelper::addSource(std::pair<int32_t, std::basic_string<char>> sourcePair) { this->sourceList.insert(sourcePair); }
void NebulaSQLHelper::updateSource(const int32_t key, std::string sourceName) { this->sourceList[key] = sourceName; }
void NebulaSQLHelper::addExpression(ExpressionNodePtr expressionNode) {
    auto pos = this->expressionList.begin();
    this->expressionList.insert(pos, expressionNode);
}
void NebulaSQLHelper::addSink(SinkDescriptorPtr sinkDescriptor) { this->sinkList.push_back(sinkDescriptor); }
void NebulaSQLHelper::addProjectionField(ExpressionNodePtr expressionNode) { this->projectionFields.push_back(expressionNode); }
void NebulaSQLHelper::addOperatorNode(NebulaSQLOperatorNode operatorNode) {
    this->operatorList.insert(std::pair<uint32_t, NebulaSQLOperatorNode>(operatorNode.getId(), operatorNode));
}
uint64_t NebulaSQLHelper::getLimit() const { return 0; }
//const std::string& NebulaSQLHelper::getNewName() const { return <#initializer #>; }
//const FieldAssignmentExpressionNodePtr& NebulaSQLHelper::getMapExpression() const { return <#initializer #>; }
//const WatermarkStrategyDescriptorPtr& NebulaSQLHelper::getWatermarkStrategieDescriptor() const {return <#initializer #>;}
const NES::Windowing::WindowTypePtr NebulaSQLHelper::getWindowType() const { return NES::Windowing::WindowTypePtr(); }

}// namespace NES::Parsers

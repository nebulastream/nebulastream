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

#include <Parsers/NebulaPSL/NebulaPSLOperatorNode.hpp>
#include <Parsers/NebulaPSL/NebulaPSLPattern.hpp>

namespace NES::Parsers {

//Getter and Setter for the map/list entries of each clause
const std::map<int32_t, std::string>& NebulaPSLPattern::getSources() const { return this->sources; }
void NebulaPSLPattern::setSources(const std::map<int32_t, std::string>& sources) { this->sources = sources; }
const std::map<int32_t, NebulaPSLOperatorNode>& NebulaPSLPattern::getOperatorList() const { return this->operatorList; }
void NebulaPSLPattern::setOperatorList(const std::map<int32_t, NebulaPSLOperatorNode>& operatorList) {
    this->operatorList = operatorList;
}
const std::list<ExpressionNodePtr>& NebulaPSLPattern::getExpressions() const { return this->expressions; }
void NebulaPSLPattern::setExpressions(const std::list<ExpressionNodePtr>& expressions) { this->expressions = expressions; }
const std::vector<ExpressionNodePtr>& NebulaPSLPattern::getProjectionFields() const { return this->projectionFields; }
void NebulaPSLPattern::setProjectionFields(const std::vector<ExpressionNodePtr>& projectionFields) {
    this->projectionFields = projectionFields;
}
const std::list<SinkDescriptorPtr>& NebulaPSLPattern::getSinks() const { return this->sinks; }
void NebulaPSLPattern::setSinks(const std::list<SinkDescriptorPtr>& sinks) { this->sinks = sinks; }
const std::pair<std::string, int32_t>& NebulaPSLPattern::getWindow() const { return this->window; }
void NebulaPSLPattern::setWindow(const std::pair<std::string, int32_t>& window) { this->window = window; }
// methods to update the clauses maps/lists
void NebulaPSLPattern::addSource(std::pair<int32_t, std::basic_string<char>> sourcePair) { this->sources.insert(sourcePair); }
void NebulaPSLPattern::updateSource(const int32_t key, std::string streamName) { this->sources[key] = streamName; }
void NebulaPSLPattern::addExpression(ExpressionNodePtr expNode) {
    auto pos = this->expressions.begin();
    this->expressions.insert(pos, expNode);
}
void NebulaPSLPattern::addSink(SinkDescriptorPtr sink) { this->sinks.push_back(sink); }
void NebulaPSLPattern::addProjectionField(ExpressionNodePtr expressionItem) { this->projectionFields.push_back(expressionItem); }
void NebulaPSLPattern::addOperatorNode(NebulaPSLOperatorNode node) {
    this->operatorList.insert(std::pair<uint32_t, NebulaPSLOperatorNode>(node.getId(), node));
}
}// namespace NES::Parsers

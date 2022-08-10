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

#include <Parsers/NebulaPSL/NebulaPSLOperatorNode.h>
#include <Parsers/NebulaPSL/NebulaPSLPattern.h>

namespace NES {

const std::map<int, std::string>& NebulaPSLPattern::getSources() const { return sources; }
void NebulaPSLPattern::setSources(const std::map<int, std::string>& sources) { NebulaPSLPattern::sources = sources; }
const std::map<int, NebulaPSLOperatorNode*>& NebulaPSLPattern::getOperatorList() const { return operatorList; }
void NebulaPSLPattern::setOperatorList(const std::map<int, NebulaPSLOperatorNode*>& operator_list) {
    operatorList = operator_list;
}
const std::list<ExpressionNodePtr>& NebulaPSLPattern::getExpressions() const { return expressions; }
void NebulaPSLPattern::setExpressions(const std::list<ExpressionNodePtr>& expressions) {
    NebulaPSLPattern::expressions = expressions;
}
const std::vector<ExpressionNodePtr>& NebulaPSLPattern::getProjectionFields() const { return projectionFields; }
void NebulaPSLPattern::setProjectionFields(const std::vector<ExpressionNodePtr>& projection_fields) {
    projectionFields = projection_fields;
}
const std::list<SinkDescriptorPtr>& NebulaPSLPattern::getSinks() const { return sinks; }
void NebulaPSLPattern::setSinks(const std::list<SinkDescriptorPtr>& sinks) { NebulaPSLPattern::sinks = sinks; }
const std::pair<std::string, int>& NebulaPSLPattern::getWindow() const { return window; }
void NebulaPSLPattern::setWindow(const std::pair<std::string, int>& window) { NebulaPSLPattern::window = window; }
void NebulaPSLPattern::addSource(std::pair<int, std::basic_string<char>> sourcePair) { sources.insert(sourcePair); }
void NebulaPSLPattern::updateSource(const int key, std::string streamName) { sources[key] = streamName; }
void NebulaPSLPattern::addExpression(ExpressionNodePtr expNode) {
    auto pos = expressions.begin();
    expressions.insert(pos, expNode);
}
void NebulaPSLPattern::addSink(SinkDescriptorPtr sink) { sinks.push_back(sink); }
void NebulaPSLPattern::addProjectionField(ExpressionNodePtr expression_item) { projectionFields.push_back(expression_item); }
void NebulaPSLPattern::addOperatorNode(NebulaPSLOperatorNode* node) { operatorList[node->getId()] = node; }
}// namespace NES

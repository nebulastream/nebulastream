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

#include <API/Query.hpp>
#include <Parsers/NebulaPSL/NePSLPatternEventNode.h>
#include <Parsers/NebulaPSL/NesCEPQueryPlanCreator.h>
#include <Parsers/NebulaPSL/gen/NesCEPLexer.h>
#include <iostream>

namespace NES {

NePSLPatternEventNode::NePSLPatternEventNode(int id) {
    setId(id);
    setNegated(false);
    setIteration(false);
}
void NePSLPatternEventNode::printPattern() {
    std::cout << "subPattern of id: " << this->getId() << std::endl;
    std::cout << "parent: " << this->getParentNodeId() << std::endl;

    try {
        std::cout << "op: " << this->eventName << std::endl;

    } catch (std::exception exception) {
        std::cout << "op: null" << std::endl;
    }
    std::cout << "negated: " << this->negated << std::endl;
    std::cout << "iteration: " << this->iteration << std::endl;
}

int NePSLPatternEventNode::getId() const { return id; }
void NePSLPatternEventNode::setId(int id) { NePSLPatternEventNode::id = id; }
const std::string& NePSLPatternEventNode::getEventName() const { return eventName; }
void NePSLPatternEventNode::setEventName(const std::string& eventName) { NePSLPatternEventNode::eventName = eventName; }
int NePSLPatternEventNode::getParentNodeId() const { return parentNodeId; }
void NePSLPatternEventNode::setParentNodeId(int parent) { NePSLPatternEventNode::parentNodeId = parent; }
bool NePSLPatternEventNode::isNegated() const { return negated; }
void NePSLPatternEventNode::setNegated(bool negated) { NePSLPatternEventNode::negated = negated; }
bool NePSLPatternEventNode::isIteration() const { return iteration; }
void NePSLPatternEventNode::setIteration(bool iteration) { NePSLPatternEventNode::iteration = iteration; }
uint64_t NePSLPatternEventNode::getIterMin() const { return iterMin; }
void NePSLPatternEventNode::setIterMin(uint64_t iterMin) { NePSLPatternEventNode::iterMin = iterMin; }
uint64_t NePSLPatternEventNode::getIterMax() const { return iterMax; }
void NePSLPatternEventNode::setIterMax(uint64_t iterMax) { NePSLPatternEventNode::iterMax = iterMax; }
const NES::Query& NePSLPatternEventNode::getQuery() const { return query; }
void NePSLPatternEventNode::setQuery(const NES::Query& query) { NePSLPatternEventNode::query = query; }
int NePSLPatternEventNode::getRightChildId() const { return rightChildId; }
void NePSLPatternEventNode::setRightChildId(int right_child_id) { rightChildId = right_child_id; }
int NePSLPatternEventNode::getLeftChildId() const { return leftChildId; }
void NePSLPatternEventNode::setLeftChildId(int left_child_id) { leftChildId = left_child_id; }

}
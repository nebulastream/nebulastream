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
#include <Parsers/NebulaPSL/NebulaPSLOperatorNode.h>
#include <Parsers/NebulaPSL/NesCEPQueryPlanCreator.h>
#include <Parsers/NebulaPSL/gen/NesCEPLexer.h>
#include <iostream>

namespace NES {

NebulaPSLOperatorNode::NebulaPSLOperatorNode(int id) { setId(id); }

int NebulaPSLOperatorNode::getId() const { return id; }
void NebulaPSLOperatorNode::setId(int id) { NebulaPSLOperatorNode::id = id; }
const std::string& NebulaPSLOperatorNode::getEventName() const { return eventName; }
void NebulaPSLOperatorNode::setEventName(const std::string& event_name) { eventName = event_name; }
int NebulaPSLOperatorNode::getRightChildId() const { return rightChildId; }
void NebulaPSLOperatorNode::setRightChildId(int right_child_id) { rightChildId = right_child_id; }
int NebulaPSLOperatorNode::getLeftChildId() const { return leftChildId; }
void NebulaPSLOperatorNode::setLeftChildId(int left_child_id) { leftChildId = left_child_id; }
const std::pair<int, int>& NebulaPSLOperatorNode::getMinMax() const { return minMax; }
void NebulaPSLOperatorNode::setMinMax(const std::pair<int, int>& min_max) { minMax = min_max; }
int NebulaPSLOperatorNode::getParentNodeId() const { return parentNodeId; }
void NebulaPSLOperatorNode::setParentNodeId(int parent_node_id) { parentNodeId = parent_node_id; }
const Query& NebulaPSLOperatorNode::getQuery() const { return query; }
void NebulaPSLOperatorNode::setQuery(const Query& query) { NebulaPSLOperatorNode::query = query; }

}// namespace NES
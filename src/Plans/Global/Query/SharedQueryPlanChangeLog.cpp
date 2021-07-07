/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Plans/Global/Query/SharedQueryPlanChangeLog.hpp>

namespace NES {

SharedQueryPlanChangeLogPtr SharedQueryPlanChangeLog::create() {
    return std::make_unique<SharedQueryPlanChangeLog>(SharedQueryPlanChangeLog());
}

void SharedQueryPlanChangeLog::addAddition(const OperatorNodePtr& upstreamOperator, uint64_t addedOperatorId) {
    //Check if an entry of the upstream operator already exists.
    if (addition.find(upstreamOperator) != addition.end()) {
        //Add the new operator id to the existing list
        auto addedOperatorIds = addition[upstreamOperator];
        addedOperatorIds.emplace_back(addedOperatorId);
        addition[upstreamOperator] = addedOperatorIds;
    } else {
        //Create a new entry for the upstream operator and added operator id in the change log
        addition[upstreamOperator] = {addedOperatorId};
    }
}

void SharedQueryPlanChangeLog::addRemoval(const OperatorNodePtr& upstreamOperator, uint64_t removedOperatorId) {
    //Check if an entry of the upstream operator already exists.
    if (removal.find(upstreamOperator) != removal.end()) {
        //Add the new operator id to the existing list
        auto removedOperatorIds = removal[upstreamOperator];
        removedOperatorIds.emplace_back(removedOperatorId);
        removal[upstreamOperator] = removedOperatorIds;
    } else {
        //Create a new entry for the upstream operator and added operator id in the change log
        removal[upstreamOperator] = {removedOperatorId};
    }
}

void SharedQueryPlanChangeLog::clearAdditionLog() {
    addition.clear();
    addedSinks.clear();
}

void SharedQueryPlanChangeLog::clearRemovalLog() {
    removal.clear();
    removedSinks.clear();
}

const std::map<OperatorNodePtr, std::vector<uint64_t>>& SharedQueryPlanChangeLog::getAddition() const { return addition; }

const std::map<OperatorNodePtr, std::vector<uint64_t>>& SharedQueryPlanChangeLog::getRemoval() const { return removal; }

const std::vector<uint64_t>& SharedQueryPlanChangeLog::getAddedSinks() const { return addedSinks; }

const std::vector<uint64_t>& SharedQueryPlanChangeLog::getRemovedSinks() const { return removedSinks; }

void SharedQueryPlanChangeLog::registerNewlyAddedSink(uint64_t sinkOperatorId) { addedSinks.emplace_back(sinkOperatorId); }

void SharedQueryPlanChangeLog::registerRemovedSink(uint64_t sinkOperatorId) { removedSinks.emplace_back(sinkOperatorId); }

void SharedQueryPlanChangeLog::reset() {
    clearAdditionLog();
    clearRemovalLog();
}
}// namespace NES

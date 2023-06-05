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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer::Experimental {

ChangeLogPtr ChangeLog::create() { return std::make_unique<ChangeLog>(ChangeLog()); }

void ChangeLog::addChangeLogEntry(uint64_t timestamp, ChangeLogEntryPtr&& changeLogEntry) {
    changeLogEntries[timestamp] = changeLogEntry;
}

void ChangeLog::performChangeLogCompaction(uint64_t timestamp) {

    std::vector<ChangeLogEntryPtr> changeLogEntriesToCompact;
    //Find the range of keys to be fetched
    auto firstElement = changeLogEntries.lower_bound(0);
    auto lastElement = changeLogEntries.lower_bound(timestamp);

    //Fetch the change log entries to be compacted
    auto iterator = firstElement;
    while (iterator != lastElement) {
        changeLogEntriesToCompact.emplace_back(iterator->second);
        iterator++;
    }

    for (uint32_t i = 0; i < changeLogEntriesToCompact.size(); i++) {
        auto sourceChangeLog = changeLogEntriesToCompact.at(i);
        for (uint32_t j = i + 1; j < changeLogEntriesToCompact.size(); j++) {

            changeLogEntriesToCompact.erase(changeLogEntriesToCompact.begin() + j);
            break;
        }
        changeLogEntriesToCompact.erase(changeLogEntriesToCompact.begin() + i);
    }
}

std::vector<ChangeLogEntryPtr> ChangeLog::getChangeLogEntriesBefore(uint64_t timestamp) {

    //Perform compaction before fetching change log
    performChangeLogCompaction(timestamp);

    std::vector<ChangeLogEntryPtr> changeLogEntriesToReturn;
    //Find the range of keys to be fetched
    auto firstElement = changeLogEntries.lower_bound(0);
    auto lastElement = changeLogEntries.lower_bound(timestamp);

    auto iterator = firstElement;
    while (iterator != lastElement) {
        changeLogEntriesToReturn.emplace_back(iterator->second);
        iterator++;
    }
    return changeLogEntriesToReturn;
}

void ChangeLog::updateProcessedChangeLogTimestamp(uint64_t timestamp) {
    this->lastProcessedChangeLogTimestamp = timestamp;
    cleanup(lastProcessedChangeLogTimestamp);
}

void ChangeLog::cleanup(uint64_t timestamp) {

    //Find the range of keys to be removed
    auto firstElement = changeLogEntries.lower_bound(0);
    auto lastElement = changeLogEntries.lower_bound(timestamp);

    //Remove the keys from the change log entry
    changeLogEntries.erase(firstElement, lastElement);
}

ChangeLogEntryPtr ChangeLog::mergeChangeLogs(std::vector<ChangeLogEntryPtr>& changeLogEntries) {

    ChangeLogEntryPtr firstChangeLogEntry = changeLogEntries.at(0);
    std::set<OperatorNodePtr> upstreamOperators;
    upstreamOperators.insert(firstChangeLogEntry->upstreamOperators.begin(), firstChangeLogEntry->upstreamOperators.end());
    std::set<OperatorNodePtr> downstreamOperators;
    downstreamOperators.insert(firstChangeLogEntry->downstreamOperators.begin(), firstChangeLogEntry->downstreamOperators.end());

    for (uint32_t index = 1; index < changeLogEntries.size(); index++) {

        // check if the upstream operators in the temp is also the upstream operator of the change log entry under consideration
        // push the most upstream operator into the new upstream Operator set

        std::set<OperatorNodePtr> tempUpstreamOperators;

        for (const auto& upstreamOperator1 : upstreamOperators) {
            for (const auto& upstreamOperator2 : changeLogEntries[index]->upstreamOperators) {
                if (upstreamOperator1->getId() == upstreamOperator2->getId()) {
                    tempUpstreamOperators.insert(upstreamOperator1);
                } else if (upstreamOperator1->containAsGrandParent(upstreamOperator2)) {
                    tempUpstreamOperators.insert(upstreamOperator1);
                } else if (upstreamOperator2->containAsGrandParent(upstreamOperator1)) {
                    tempUpstreamOperators.insert(upstreamOperator2);
                }
            }
        }

        // check if the downstream operators in the temp is also the downstream operator of the change log entry under consideration
        // push the most downstream operator into the new downstream Operator set
    }

    return ChangeLogEntry::create(upstreamOperators, downstreamOperators);
}

}// namespace NES::Optimizer::Experimental

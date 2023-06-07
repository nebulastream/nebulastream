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

    //Get all change log entries to compact
    std::vector<std::pair<uint64_t, ChangeLogEntryPtr>> changeLogEntriesToCompact = getChangeLogEntriesBefore(timestamp);

    // Repeat this process till all change log entries within the timestamp are non-overlapping or are compacted.
    bool repeat = true;
    while (repeat) {
        repeat = false;//set to false after entering the loop to prevent infinite loop.

        //Iterate over the fetched change log entries to be compacted and find the changelogs that can be merged together.
        for (uint32_t i = 0; i < changeLogEntriesToCompact.size(); i++) {

            //state with a change log entry and find all following overlapping change log entries.
            auto candidateChangeLog = changeLogEntriesToCompact.at(i);

            //Fetch the poset of the candidate change log
            std::set<OperatorId> sourceChangeLogPoSet = candidateChangeLog.second->poSetOfSubQueryPlan;

            // temp container to store all overlapping change log entries
            std::vector<std::pair<uint64_t, ChangeLogEntryPtr>> changeLogEntriesToMerge;

            //Iterate over the remaining change logs entries. Find all change logs that are overlapping with the first change log by
            // comparing their respective poset.
            for (uint32_t j = i + 1; j < changeLogEntriesToCompact.size(); j++) {
                auto destinationChangeLog = changeLogEntriesToCompact.at(j);
                std::set<uint32_t> diff;
                //compute difference among the poset of two change log entries
                std::set_difference(sourceChangeLogPoSet.begin(),
                                    sourceChangeLogPoSet.end(),
                                    destinationChangeLog.second->poSetOfSubQueryPlan.begin(),
                                    destinationChangeLog.second->poSetOfSubQueryPlan.end(),
                                    std::inserter(diff, diff.begin()));

                //If diff is not empty then the change log entries are overlapping
                if (!diff.empty()) {
                    //add the change log entry into the temp container
                    changeLogEntriesToMerge.emplace_back(changeLogEntriesToCompact.at(j));
                }
            }

            //If found overlapping change logs then merge them together
            if (!changeLogEntriesToMerge.empty()) {
                //inset the candidate change log entry into the temp container storing all overlapping change log entries.
                changeLogEntriesToMerge.emplace_back(candidateChangeLog);

                //Union all the change log entries and proceed further
                auto mergedChangeLog = mergeChangeLogs(changeLogEntriesToMerge);

                // store the union change log entry into the btree with key as the timestamp of the candidate change log entry.
                changeLogEntriesToCompact.emplace_back(candidateChangeLog.first, mergedChangeLog);

                //remove merged change log entries from the compacted change log entries.
                for (const auto& mergedChangeLogEntry : changeLogEntriesToMerge) {
                    changeLogEntriesToCompact.erase(
                        std::remove(changeLogEntriesToCompact.begin(), changeLogEntriesToCompact.end(), mergedChangeLogEntry),
                        changeLogEntriesToCompact.end());
                }
                repeat = true;
                break;
            }
        }
    }

    //clear all un-compacted change log entries before the timestamp
    removeChangeLogsBefore(timestamp);

    //Insert compacted changelog entries
    for (auto& compactedChangeLogEntry : changeLogEntriesToCompact){
        addChangeLogEntry(compactedChangeLogEntry.first, std::move(compactedChangeLogEntry.second));
    }
}

std::vector<std::pair<uint64_t, ChangeLogEntryPtr>> ChangeLog::getChangeLogEntriesBefore(uint64_t timestamp) {

    std::vector<std::pair<uint64_t, ChangeLogEntryPtr>> changeLogEntriesToReturn;
    //Find the range of keys to be fetched
    auto firstElement = changeLogEntries.lower_bound(0);
    auto lastElement = changeLogEntries.lower_bound(timestamp);

    auto iterator = firstElement;
    while (iterator != lastElement) {
        changeLogEntriesToReturn.emplace_back(iterator->first, iterator->second);
        iterator++;
    }
    return changeLogEntriesToReturn;
}

void ChangeLog::updateProcessedChangeLogTimestamp(uint64_t timestamp) {
    this->lastProcessedChangeLogTimestamp = timestamp;
    removeChangeLogsBefore(lastProcessedChangeLogTimestamp);
}

void ChangeLog::removeChangeLogsBefore(uint64_t timestamp) {

    //Find the range of keys to be removed
    auto firstElement = changeLogEntries.lower_bound(0);
    auto lastElement = changeLogEntries.lower_bound(timestamp);

    //Remove the keys from the change log entry
    changeLogEntries.erase(firstElement, lastElement);
}

ChangeLogEntryPtr ChangeLog::mergeChangeLogs(std::vector<std::pair<uint64_t, ChangeLogEntryPtr>>& changeLogEntriesToMerge) {

    ChangeLogEntryPtr firstChangeLogEntry = changeLogEntriesToMerge.at(0).second;
    std::set<OperatorNodePtr> upstreamOperators = firstChangeLogEntry->upstreamOperators;
    std::set<OperatorNodePtr> downstreamOperators = firstChangeLogEntry->downstreamOperators;

    for (uint32_t index = 1; index < changeLogEntriesToMerge.size(); index++) {

        // check if the upstream operators in the temp is also the upstream operator of the change log entry under consideration
        // push the most upstream operator into the new upstream Operator set

        std::set<OperatorNodePtr> tempUpstreamOperators;
        std::set<OperatorNodePtr> upstreamOperatorsToCheck = changeLogEntriesToMerge[index].second->upstreamOperators;
        for (const auto& upstreamOperator1 : upstreamOperators) {
            for (const auto& upstreamOperator2 : upstreamOperatorsToCheck) {
                if (upstreamOperator1->getId() == upstreamOperator2->getId()) {
                    tempUpstreamOperators.insert(upstreamOperator1);
                    upstreamOperators.erase(upstreamOperator1);
                    upstreamOperatorsToCheck.erase(upstreamOperator2);
                    break;
                } else if (upstreamOperator1->containAsGrandParent(upstreamOperator2)) {
                    tempUpstreamOperators.insert(upstreamOperator1);
                    upstreamOperatorsToCheck.erase(upstreamOperator2);
                } else if (upstreamOperator2->containAsGrandParent(upstreamOperator1)) {
                    tempUpstreamOperators.insert(upstreamOperator2);
                    upstreamOperators.erase(upstreamOperator1);
                    break;
                }
            }
        }

        if (!upstreamOperators.empty()) {
            tempUpstreamOperators.insert(upstreamOperators.begin(), upstreamOperators.end());
        }

        if (!upstreamOperatorsToCheck.empty()) {
            tempUpstreamOperators.insert(upstreamOperatorsToCheck.begin(), upstreamOperatorsToCheck.end());
        }

        upstreamOperators = tempUpstreamOperators;
        tempUpstreamOperators.clear();

        // check if the downstream operators in the temp is also the downstream operator of the change log entry under consideration
        // push the most downstream operator into the new downstream Operator set

        std::set<OperatorNodePtr> tempDownstreamOperators;
        std::set<OperatorNodePtr> downstreamOperatorsToCheck = changeLogEntriesToMerge[index].second->downstreamOperators;
        for (const auto& downstreamOperator1 : downstreamOperators) {
            for (const auto& downstreamOperator2 : downstreamOperatorsToCheck) {
                if (downstreamOperator1->getId() == downstreamOperator2->getId()) {
                    tempUpstreamOperators.insert(downstreamOperator1);
                    downstreamOperators.erase(downstreamOperator1);
                    downstreamOperatorsToCheck.erase(downstreamOperator2);
                    break;
                } else if (downstreamOperator1->containAsGrandParent(downstreamOperator2)) {
                    tempDownstreamOperators.insert(downstreamOperator2);
                    downstreamOperators.erase(downstreamOperator1);
                    break;
                } else if (downstreamOperator2->containAsGrandParent(downstreamOperator1)) {
                    tempDownstreamOperators.insert(downstreamOperator1);
                    downstreamOperatorsToCheck.erase(downstreamOperator2);
                }
            }
        }

        if (!upstreamOperators.empty()) {
            tempUpstreamOperators.insert(upstreamOperators.begin(), upstreamOperators.end());
        }

        if (!downstreamOperatorsToCheck.empty()) {
            tempDownstreamOperators.insert(downstreamOperatorsToCheck.begin(), downstreamOperatorsToCheck.end());
        }

        downstreamOperators = tempDownstreamOperators;
        tempDownstreamOperators.clear();
    }

    return ChangeLogEntry::create(upstreamOperators, downstreamOperators);
}

}// namespace NES::Optimizer::Experimental

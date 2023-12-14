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

#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer::Experimental {

ChangeLogPtr ChangeLog::create() { return std::make_unique<ChangeLog>(ChangeLog()); }

void ChangeLog::addChangeLogEntry(Timestamp timestamp, ChangeLogEntryPtr&& changeLogEntry) {
    changeLogEntries[timestamp] = changeLogEntry;
}

std::vector<std::pair<Timestamp, ChangeLogEntryPtr>> ChangeLog::getCompactChangeLogEntriesBeforeTimestamp(Timestamp timestamp) {
    performChangeLogCompactionTillTimestamp(timestamp);
    return getChangeLogEntriesBeforeTimestamp(timestamp);
}

void ChangeLog::updateProcessedChangeLogTimestamp(Timestamp timestamp) {
    this->lastProcessedChangeLogTimestamp = timestamp;
    removeChangeLogsBefore(lastProcessedChangeLogTimestamp);
}

void ChangeLog::performChangeLogCompactionTillTimestamp(uint64_t timestamp) {

    //Get all change log entries to compact
    std::vector<std::pair<Timestamp, ChangeLogEntryPtr>> changeLogEntriesToCompact =
        getChangeLogEntriesBeforeTimestamp(timestamp);

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
            std::vector<std::pair<uint64_t, ChangeLogEntryPtr>> overlappingChangeLogEntries;

            //Iterate over the remaining change logs entries. Find all change logs that are overlapping with the first change log by
            // comparing their respective poset.
            for (uint32_t j = i + 1; j < changeLogEntriesToCompact.size(); j++) {
                auto destinationChangeLog = changeLogEntriesToCompact.at(j);
                std::set<uint32_t> diff;
                //compute intersection among the poset of two change log entries
                std::set_intersection(sourceChangeLogPoSet.begin(),
                                      sourceChangeLogPoSet.end(),
                                      destinationChangeLog.second->poSetOfSubQueryPlan.begin(),
                                      destinationChangeLog.second->poSetOfSubQueryPlan.end(),
                                      std::inserter(diff, diff.begin()));

                //If diff is not empty then the change log entries are overlapping
                if (!diff.empty()) {
                    //add the change log entry into the temp container
                    overlappingChangeLogEntries.emplace_back(changeLogEntriesToCompact.at(j));
                }
            }

            //If found overlapping change logs then merge them together
            if (!overlappingChangeLogEntries.empty()) {
                //inset the candidate change log entry into the temp container storing all overlapping change log entries.
                overlappingChangeLogEntries.emplace_back(candidateChangeLog);

                ChangeLogEntryPtr compactedChangeLogEntry;
                if (overlappingChangeLogEntries.size() > 1) {
                    //Union all the change log entries and proceed further
                    compactedChangeLogEntry = compactChangeLogEntries(overlappingChangeLogEntries);
                } else {
                    compactedChangeLogEntry = overlappingChangeLogEntries.at(0).second;
                }

                // store the union change log entry into the btree with key as the timestamp of the candidate change log entry.
                changeLogEntriesToCompact.emplace_back(candidateChangeLog.first, compactedChangeLogEntry);

                //remove merged change log entries from the compacted change log entries.
                for (const auto& mergedChangeLogEntry : overlappingChangeLogEntries) {
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
    for (auto& compactedChangeLogEntry : changeLogEntriesToCompact) {
        addChangeLogEntry(compactedChangeLogEntry.first, std::move(compactedChangeLogEntry.second));
    }
}

std::vector<std::pair<Timestamp, ChangeLogEntryPtr>> ChangeLog::getChangeLogEntriesBeforeTimestamp(Timestamp timestamp) {

    std::vector<std::pair<Timestamp, ChangeLogEntryPtr>> changeLogEntriesToReturn;
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

void ChangeLog::removeChangeLogsBefore(Timestamp timestamp) {

    //Find the range of keys to be removed
    auto firstElement = changeLogEntries.lower_bound(0);
    auto lastElement = changeLogEntries.lower_bound(timestamp);

    //Remove the keys from the change log entry
    changeLogEntries.erase(firstElement, lastElement);
}

ChangeLogEntryPtr
ChangeLog::compactChangeLogEntries(std::vector<std::pair<Timestamp, ChangeLogEntryPtr>>& changeLogEntriesToCompact) {

    ChangeLogEntryPtr firstChangeLogEntry = changeLogEntriesToCompact.at(0).second;
    std::set<LogicalOperatorNodePtr> firstUpstreamOperators = firstChangeLogEntry->upstreamOperators;
    std::set<LogicalOperatorNodePtr> firstDownstreamOperators = firstChangeLogEntry->downstreamOperators;

    //iterate from the first entry as the 0th entry is assigned as the first change log entry above.
    for (uint32_t index = 1; index < changeLogEntriesToCompact.size(); index++) {
        // check if the upstream operators in the temp is also the upstream operator of the change log entry under consideration
        // push the most upstream operator into the new upstream Operator set
        std::set<LogicalOperatorNodePtr> tempUpstreamOperators;
        std::set<LogicalOperatorNodePtr> nextUpstreamOperators = changeLogEntriesToCompact[index].second->upstreamOperators;

        for (auto firstItr = firstUpstreamOperators.begin(); firstItr != firstUpstreamOperators.end();) {
            bool incFirstItr = true;
            for (auto nextItr = nextUpstreamOperators.begin(); nextItr != nextUpstreamOperators.end();) {
                if ((*firstItr)->getId() == (*nextItr)->getId()) {
                    // Insert item in the temp upstream operator list
                    tempUpstreamOperators.insert((*firstItr));
                    // It is okay to erase these operators as there won't be any other operators with same id in the change logs entries.
                    firstItr =
                        firstUpstreamOperators.erase(firstItr);// Please note that we are assigning the iterator to the next item
                    nextUpstreamOperators.erase(nextItr);
                    incFirstItr = false;
                    break;
                } else if ((*firstItr)->as<OperatorNode>()->containAsGrandParent((*nextItr))) {
                    // Insert item in the temp upstream operator list
                    tempUpstreamOperators.insert((*firstItr));
                    // It is okay to erase this next operator as there won't be any other operator in the first upstream operator list
                    // that can be this operator's upstream operator
                    nextItr =
                        nextUpstreamOperators.erase(nextItr);// Please note that we are assigning the iterator to the next item
                } else if ((*nextItr)->as<OperatorNode>()->containAsGrandParent((*firstItr))) {
                    tempUpstreamOperators.insert((*nextItr));
                    //It is okay to erase this first operator as no other upstream operator can be its upstream operator
                    firstItr =
                        firstUpstreamOperators.erase(firstItr);// Please note that we are assigning the iterator to the next item
                    incFirstItr = false;
                    break;
                } else {
                    nextItr++;// move to the next item
                }
            }

            // Increment the first iterator
            if (incFirstItr) {
                firstItr++;
            }
        }

        //Add remaining upstream operators to the temp upstream operator set.
        if (!firstUpstreamOperators.empty()) {
            tempUpstreamOperators.insert(firstUpstreamOperators.begin(), firstUpstreamOperators.end());
        }
        if (!nextUpstreamOperators.empty()) {
            tempUpstreamOperators.insert(nextUpstreamOperators.begin(), nextUpstreamOperators.end());
        }

        //Reinitialize first Upstream Operators to the temp and clear the temp upstream operator set
        firstUpstreamOperators = tempUpstreamOperators;
        tempUpstreamOperators.clear();

        // check if the downstream operators in the temp is also the downstream operator of the change log entry under consideration
        // push the most downstream operator into the new downstream Operator set
        std::set<LogicalOperatorNodePtr> tempDownstreamOperators;
        std::set<LogicalOperatorNodePtr> nextDownstreamOperators = changeLogEntriesToCompact[index].second->downstreamOperators;

        for (auto firstItr = firstDownstreamOperators.begin(); firstItr != firstDownstreamOperators.end();) {
            bool incFirstItr = true;
            for (auto nextItr = nextDownstreamOperators.begin(); nextItr != nextDownstreamOperators.end();) {
                if ((*firstItr)->getId() == (*nextItr)->getId()) {
                    // Insert item in the temp downstream operator list
                    tempDownstreamOperators.insert((*firstItr));
                    // It is okay to erase these operators as there won't be any other operators with same id in the change logs entries.
                    firstItr = firstDownstreamOperators.erase(
                        firstItr);// Please note that we are assigning the iterator to the next item
                    nextDownstreamOperators.erase(nextItr);
                    incFirstItr = false;
                    break;
                } else if ((*firstItr)->as<OperatorNode>()->containAsGrandParent((*nextItr))) {
                    tempDownstreamOperators.insert((*nextItr));
                    // It is okay to erase first operators as there won't be any other operators in the next change log entry that can also be its downstream operator.
                    firstItr = firstDownstreamOperators.erase(
                        firstItr);// Please note that we are assigning the iterator to the next item
                    incFirstItr = false;
                    break;
                } else if ((*nextItr)->as<OperatorNode>()->containAsGrandParent((*firstItr))) {
                    // It is okay to erase next operators as there won't be any other operators in the first change log entry that can also be its downstream operator.
                    tempDownstreamOperators.insert((*firstItr));
                    nextItr = nextDownstreamOperators.erase(nextItr);
                } else {
                    nextItr++;// move to the next item
                }
            }

            // Increment the first iterator only if it was not incremented before
            if (incFirstItr) {
                firstItr++;
            }
        }

        //Add remaining downstream operators to the temp upstream operator set.
        if (!firstDownstreamOperators.empty()) {
            tempDownstreamOperators.insert(firstDownstreamOperators.begin(), firstDownstreamOperators.end());
        }
        if (!nextDownstreamOperators.empty()) {
            tempDownstreamOperators.insert(nextDownstreamOperators.begin(), nextDownstreamOperators.end());
        }

        //Reinitialize first downstream Operators to the temp and clear the temp downstream operator set
        firstDownstreamOperators = tempDownstreamOperators;
        tempDownstreamOperators.clear();
    }

    return ChangeLogEntry::create(firstUpstreamOperators, firstDownstreamOperators);
}

}// namespace NES::Optimizer::Experimental

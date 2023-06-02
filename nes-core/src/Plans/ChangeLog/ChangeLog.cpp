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

#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer::Experimental {

ChangeLogPtr ChangeLog::create() { return std::make_unique<ChangeLog>(ChangeLog()); }

void ChangeLog::addChangeLogEntry(uint64_t timestamp, ChangeLogEntryPtr&& changeLogEntry) {
    changeLogEntries[timestamp] = changeLogEntry;
}

//FIXME: implement as part of the issue #3797
void ChangeLog::performChangeLogCompaction(uint64_t) { NES_NOT_IMPLEMENTED(); }

std::vector<ChangeLogEntryPtr> ChangeLog::getChangeLogEntriesBefore(uint64_t timestamp) {

    //TODO: enable as part of #3797
    //performChangeLogCompaction(timestamp);

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

}// namespace NES::Optimizer::Experimental

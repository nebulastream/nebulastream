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

#ifndef NES_CORE_INCLUDE_PLANS_CHANGELOG_CHANGELOG_HPP_
#define NES_CORE_INCLUDE_PLANS_CHANGELOG_CHANGELOG_HPP_

#include <Plans/ChangeLog/ChangeLogEntry.hpp>
#include <absl/container/btree_map.h>
#include <memory>
#include <vector>

namespace NES::Optimizer::Experimental {

class ChangeLog;
using ChangeLogPtr = std::unique_ptr<ChangeLog>;

using Timestamp = uint64_t;

/**
 * @brief: ChangeLog records changes occurring to a shared query plan due to dynamism in the underlying topology, or due to incoming and outgoing stream queries.
 * For each external event we create a @see ChangeLogEntry with a timestamp when the change occurred. We store this information in a B-Tree where key is the timestamp when he change log entry was created.
 */
class ChangeLog {

  public:
    static ChangeLogPtr create();

    /**
     * Add change log entry into the change log
     * @param timestamp: the timestamp of the change log entry
     * @param changeLogEntry: the change log entry
     */
    void addChangeLogEntry(Timestamp timestamp, ChangeLogEntryPtr&& changeLogEntry);

    /**
     * @brief Compact change log entries before the time stamp and return all compacted change log entries
     * @param timestamp: the timestamp before which the changelog entries need to be compacted and returned
     * @return vector of non-overlapping and compacted change log entries
     */
    std::vector<std::pair<Timestamp, ChangeLogEntryPtr>> getCompactChangeLogEntriesBeforeTimestamp(Timestamp timestamp);

    /**
     * @brief: Update the timestamp till which the change log entries are processed
     * @param timestamp: the new timestamp till which the change log entries are processed
     */
    void updateProcessedChangeLogTimestamp(Timestamp timestamp);

  protected:
    /**
     * @brief Get all non-overlapping change log entries created after the indicated timestamp
     * @param timestamp : the timestamp after which the change log entries need to be retrieved
     * @return a vector of change log entries
     */
    std::vector<std::pair<Timestamp, ChangeLogEntryPtr>> getChangeLogEntriesBeforeTimestamp(Timestamp timestamp);

  private:
    ChangeLog() = default;

    /**
     * @brief: Compact change log by combining overlapping change log entries into a single changelog entry.
     * @note: Overlapping change log entry means that the sub-query plans represented by the change log entries are overlapping
     * with each other.
     * @param timestamp: the timestamp till which the log is to be compacted
     */
    void performChangeLogCompactionTillTimestamp(uint64_t timestamp);

    /**
     * @brief Merge all change log entries together
     * @param changeLogEntriesToCompact: entries to be merged
     * @return merged change log entry
     */
    ChangeLogEntryPtr compactChangeLogEntries(std::vector<std::pair<Timestamp, ChangeLogEntryPtr>>& changeLogEntriesToCompact);

    /**
     * @brief Clean up the change log entries created before the provided timestamp
     * @param timestamp : the timestamp before which the change log entries need to be removed
     */
    void removeChangeLogsBefore(Timestamp timestamp);

    // time stamp till which all change log entries are processed
    Timestamp lastProcessedChangeLogTimestamp;
    absl::btree_map<Timestamp, ChangeLogEntryPtr> changeLogEntries;
};
}// namespace NES::Optimizer::Experimental
#endif// NES_CORE_INCLUDE_PLANS_CHANGELOG_CHANGELOG_HPP_

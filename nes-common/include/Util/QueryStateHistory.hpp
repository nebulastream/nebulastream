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

#ifndef QUERYSTATEHISTORY_HPP
#define QUERYSTATEHISTORY_HPP

#include <Util/QueryState.hpp>
#include <nlohmann/json_fwd.hpp>
#include <vector>

namespace NES {

class QueryStateHistoryEntry;

/** @brief Track a timestamped history of query status changes. */
using QueryStateHistory = std::vector<QueryStateHistoryEntry>;

/**
 * @brief Associate a timestamp (in microseconds) with a query status change.
 *
 * Also includes code to transform to JSON.
 */
class QueryStateHistoryEntry {
public:
    QueryStateHistoryEntry(const uint64_t timestampInUs, const QueryState queryState)
    : timestampInUs(timestampInUs), queryState(queryState) {}
    // We cannot use the JSON helper macros, because we want to return a string representation of the QueryState enum.
    /** @brief Transfor this query status history entry to JSON.
     *  (There is no corresponding method from_json method.)
     */
    friend void to_json(nlohmann::json&, const QueryStateHistoryEntry&);
private:
    uint64_t timestampInUs;
    QueryState queryState;
};



}

#endif //QUERYSTATEHISTORY_HPP

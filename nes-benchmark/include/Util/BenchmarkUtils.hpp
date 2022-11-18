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

#ifndef NES_BENCHMARKUTILS_HPP
#define NES_BENCHMARKUTILS_HPP

#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Common/Identifiers.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>

#include <chrono>

namespace NES::Benchmark::Util {

static constexpr auto defaultStartQueryTimeout = std::chrono::seconds(180);
static constexpr auto sleepDuration = std::chrono::milliseconds(250);

/**
     * @brief This method is used for waiting till the query gets into running status or a timeout occurs
     * @param queryId : the query id to check for
     * @param queryCatalogService: the catalog to look into for status change
     * @param timeoutInSec: time to wait before stop checking
     * @return true if query gets into running status else false
     */
[[nodiscard]] bool waitForQueryToStart(QueryId queryId,
                                       const QueryCatalogServicePtr& queryCatalogService,
                                       std::chrono::seconds timeoutInSec = std::chrono::seconds(defaultStartQueryTimeout)) {
    NES_TRACE("TestUtils: wait till the query " << queryId << " gets into Running status.");
    auto start_timestamp = std::chrono::system_clock::now();

    NES_TRACE("TestUtils: Keep checking the status of query " << queryId << " until a fixed time out");
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        auto queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
        if (!queryCatalogEntry) {
            NES_ERROR("TestUtils: unable to find the entry for query " << queryId << " in the query catalog.");
            return false;
        }
        NES_TRACE("TestUtils: Query " << queryId << " is now in status " << queryCatalogEntry->getQueryStatusAsString());
        QueryStatus::Value status = queryCatalogEntry->getQueryStatus();

        switch (queryCatalogEntry->getQueryStatus()) {
            case QueryStatus::MarkedForHardStop:
            case QueryStatus::MarkedForSoftStop:
            case QueryStatus::SoftStopCompleted:
            case QueryStatus::SoftStopTriggered:
            case QueryStatus::Stopped:
            case QueryStatus::Running: {
                return true;
            }
            case QueryStatus::Failed: {
                NES_ERROR("Query failed to start. Expected: Running or Optimizing but found " + QueryStatus::toString(status));
                return false;
            }
            default: {
                NES_WARNING("Expected: Running or Scheduling but found " + QueryStatus::toString(status));
                break;
            }
        }

        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkCompleteOrTimeout: waitForStart expected results are not reached after timeout");
    return false;
}

/**
    * @brief creates a vector with a range of [start, stop) and step size
    */
template<typename T>
static void createRangeVector(std::vector<T>& vector, T start, T stop, T stepSize) {
    for (T i = start; i < stop; i += stepSize) {
        vector.push_back(i);
    }
}

/**
    * @brief creates a vector with a range of [start, stop). This will increase by a power of two. So e.g. 2kb, 4kb, 8kb
    */
template<typename T>
static void createRangeVectorPowerOfTwo(std::vector<T>& vector, T start, T stop) {
    for (T i = start; i < stop; i = i << 1) {
        vector.push_back(i);
    }
}

}// namespace NES::Benchmark::Util

#endif//NES_BENCHMARKUTILS_HPP

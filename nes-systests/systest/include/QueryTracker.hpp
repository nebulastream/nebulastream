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

#pragma once

#include <cstdint>
#include <deque>
#include <optional>
#include <vector>

#include <SystestState.hpp>

namespace NES::Systest
{

class QueryTracker
{
    std::vector<PlannedQuery> pendingQueries;
    std::deque<SubmittedQuery> submittedQueries;
    std::vector<FailedQuery> failedQueries;
    std::vector<FinishedQuery> finishedQueries;

    const uint64_t numConcurrentQueries;

public:
    QueryTracker(const std::vector<PlannedQuery>& queries, uint64_t concurrency);
    QueryTracker() = delete;

    [[nodiscard]] std::vector<FailedQuery> getFailedQueries() const;
    size_t getTotalQueries() const;
    bool done() const;

    std::optional<PlannedQuery> nextPending();
    std::optional<SubmittedQuery> nextSubmitted();

    void moveToFailed(FailedQuery&& failed);
    void moveToSubmitted(SubmittedQuery&& submitted);
    void moveToFinished(FinishedQuery&& finished);
};

}
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

#include <QueryTracker.hpp>

#include <utility>

namespace NES::Systest
{

QueryTracker::QueryTracker(const std::vector<PlannedQuery>& queries, const uint64_t concurrency)
    : pendingQueries{queries}, numConcurrentQueries{concurrency}
{
}

std::vector<FailedQuery> QueryTracker::getFailedQueries() const
{
    return failedQueries;
}

size_t QueryTracker::getTotalQueries() const
{
    return pendingQueries.size() + submittedQueries.size() + failedQueries.size() + finishedQueries.size();
}

bool QueryTracker::done() const
{
    return pendingQueries.empty() && submittedQueries.empty();
}

std::optional<PlannedQuery> QueryTracker::nextPending()
{
    if (submittedQueries.size() >= numConcurrentQueries || pendingQueries.empty())
    {
        return std::nullopt;
    }

    auto query = std::move(pendingQueries.back());
    pendingQueries.pop_back();
    return {query};
}

std::optional<SubmittedQuery> QueryTracker::nextSubmitted()
{
    if (submittedQueries.empty())
    {
        return std::nullopt;
    }
    auto query = std::move(submittedQueries.front());
    submittedQueries.pop_front();
    return {query};
}

void QueryTracker::moveToFailed(FailedQuery&& failed)
{
    failedQueries.push_back(std::move(failed));
}

void QueryTracker::moveToSubmitted(SubmittedQuery&& submitted)
{
    submittedQueries.push_back(std::move(submitted));
}

void QueryTracker::moveToFinished(FinishedQuery&& finished)
{
    finishedQueries.push_back(std::move(finished));
}

}
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

#include <cstddef>
#include <string>
#include <string_view>

#include <ErrorHandling.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

/// Pad size constants for console output formatting
static constexpr auto padSizeSuccess = 120;
static constexpr auto padSizeQueryNumber = 2;
static constexpr auto padSizeQueryCounter = 3;

class QueryResultReporter
{
    size_t finishedCount = 0;
    size_t totalQueries = 0;

public:
    explicit QueryResultReporter(size_t totalQueries);

    template <typename ErrorCallable>
    bool reportResult(FailedQuery& failedQuery, ErrorCallable&& errorBuilder)
    {
        const std::string msg = errorBuilder();
        const bool passed = msg.empty();
        printQueryResultToStdOut(failedQuery.ctx, msg, finishedCount++, totalQueries, "", false);
        return passed;
    }

    void reportSuccess(const SystestQueryContext& ctx, const std::string_view performanceMessage = "");
    void reportFailure(const SystestQueryContext& ctx, const std::string& errorMessage, const std::string_view performanceMessage = "");
    void reportExpectedError(const SystestQueryContext& ctx, const ExpectedError& expectedError, const Exception& actualException);
    void reportUnexpectedSuccess(const SystestQueryContext& ctx, const ExpectedError& expectedError);

    size_t getFinishedCount() const;

private:
    void printQueryResultToStdOut(
        const SystestQueryContext& ctx,
        const std::string& errorMessage,
        size_t queryCounter,
        size_t totalQueries,
        std::string_view queryPerformanceMessage,
        bool passed);
};

}
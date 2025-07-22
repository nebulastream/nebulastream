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

#include <QueryResultReporter.hpp>

#include <iostream>
#include <string>

#include <fmt/color.h>
#include <fmt/format.h>

namespace NES::Systest
{

QueryResultReporter::QueryResultReporter(const size_t totalQueries) : totalQueries(totalQueries) { }

void QueryResultReporter::reportSuccess(const SystestQueryContext& ctx, const std::string_view performanceMessage)
{
    printQueryResultToStdOut(ctx, "", finishedCount++, totalQueries, performanceMessage, true);
}

void QueryResultReporter::reportFailure(const SystestQueryContext& ctx, const std::string& errorMessage, const std::string_view performanceMessage)
{
    printQueryResultToStdOut(ctx, errorMessage, finishedCount++, totalQueries, performanceMessage, false);
}

void QueryResultReporter::reportExpectedError(const SystestQueryContext& ctx, const ExpectedError& expectedError, const Exception& actualException)
{
    const std::string errorMsg = fmt::format("expected error {} but got {}", expectedError.code, actualException.code());
    reportFailure(ctx, errorMsg);
}

void QueryResultReporter::reportUnexpectedSuccess(const SystestQueryContext& ctx, const ExpectedError& expectedError)
{
    const std::string errorMsg = fmt::format("expected error {} but query succeeded", expectedError.code);
    reportFailure(ctx, errorMsg);
}

size_t QueryResultReporter::getFinishedCount() const
{
    return finishedCount;
}

void QueryResultReporter::printQueryResultToStdOut(
    const SystestQueryContext& ctx,
    const std::string& errorMessage,
    const size_t queryCounter,
    const size_t totalQueries,
    const std::string_view queryPerformanceMessage,
    const bool passed)
{
    const auto queryNameLength = ctx.testName.size();
    const auto queryNumberAsString = ctx.queryIdInFile.toString();
    const auto queryNumberLength = queryNumberAsString.size();
    const auto queryCounterAsString = std::to_string(queryCounter + 1);

    /// spd logger cannot handle multiline prints with proper color and pattern.
    /// And as this is only for test runs we use stdout here.
    std::cout << std::string(padSizeQueryCounter - queryCounterAsString.size(), ' ');
    std::cout << queryCounterAsString << "/" << totalQueries << " ";
    std::cout << ctx.testName << ":" << std::string(padSizeQueryNumber - queryNumberLength, '0') << queryNumberAsString;
    std::cout << std::string(padSizeSuccess - (queryNameLength + padSizeQueryNumber), '.');

    if (passed)
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::green), "PASSED {}\n", queryPerformanceMessage);
    }
    else
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "FAILED {}\n", queryPerformanceMessage);
        std::cout << "===================================================================" << '\n';
        std::cout << ctx.queryDefinition << '\n';
        std::cout << "===================================================================" << '\n';
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "Error: {}\n", errorMessage);
        std::cout << "===================================================================" << '\n';
    }
}

}
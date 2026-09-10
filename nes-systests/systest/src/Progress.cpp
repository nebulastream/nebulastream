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

#include <Progress.hpp>

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <string>
#include <string_view>
#include <variant>

#include <unistd.h>
#include <fmt/color.h>
#include <fmt/format.h>

#include <Model/RunnableTest.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Util/Overloaded.hpp>

namespace NES
{
namespace
{

/// The column at which every outcome starts, so outcomes line up and a failure is found by scanning.
constexpr size_t outcomeColumn = 100;

/// The width of the rules printed around a failing query.
constexpr size_t ruleWidth = 67;

/// Colour goes only to a terminal, because redirected output would collect escape sequences.
bool colourSupported()
{
    static const bool supported = isatty(fileno(stdout)) != 0;
    return supported;
}

void printEmphasised(const std::string_view text, const fmt::color colour)
{
    if (colourSupported())
    {
        fmt::print(fmt::emphasis::bold | fg(colour), "{}", text);
        return;
    }
    fmt::print("{}", text);
}

}

Progress::Progress(const size_t totalCases) : total{totalCases}
{
}

void Progress::beginRun(const size_t files) const
{
    fmt::print("Running {} queries from {} test files\n", total, files);
}

void Progress::report(
    const TestCaseId& id, const RunnableCase& testCase, const Verdict& verdict, const std::chrono::steady_clock::duration elapsed)
{
    ++done;
    const auto percent = total > 0 ? (static_cast<double>(done) * 100.0) / static_cast<double>(total) : 0.0;
    const auto prefix = fmt::format("{:>4}/{} ({:5.1f}%) {} ", done, total, percent, id);
    /// At least one dot, so the outcome never abuts a location too long to pad.
    const auto dots = prefix.size() < outcomeColumn ? outcomeColumn - prefix.size() : 1;
    fmt::print("{}{} ", prefix, std::string(dots, '.'));

    const auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    if (verdict)
    {
        printEmphasised("PASSED", fmt::color::green);
        fmt::print(" ({} ms)\n", milliseconds);
    }
    else
    {
        printEmphasised("FAILED", fmt::color::red);
        fmt::print(" ({} ms)\n", milliseconds);
        /// The final report lists this failure too, but printing the statements here saves the reader scrolling back to them.
        const auto sql = std::visit(
            Overloaded{
                [](const RunnableQuery& query) { return query.sql; },
                [](const RunnableDifferential& differential)
                { return fmt::format("{}\n====\n{}", differential.firstSql, differential.secondSql); }},
            testCase.action);
        const auto rule = std::string(ruleWidth, '=');
        fmt::print("{}\n{}\n{}\n", rule, sql, rule);
        printEmphasised(fmt::format("Error: {}\n", verdict.error().detail), fmt::color::red);
        fmt::print("{}\n", rule);
    }
    /// Redirected output is block buffered, so an unflushed line would appear long after the query it reports.
    std::fflush(stdout);
}

}

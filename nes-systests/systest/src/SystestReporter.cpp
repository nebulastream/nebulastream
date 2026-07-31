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

#include <SystestReporter.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <ranges>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/color.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rfl/json/write.hpp>
#include <SystestResolver.hpp>
#include <SystestRun.hpp>

namespace NES::Systest
{
namespace
{

std::string_view primarySql(const CaseAction& action)
{
    if (const auto* query = std::get_if<QueryAction>(&action))
    {
        return query->sql;
    }
    return std::get<DifferentialAction>(action).leftSql;
}

std::string formatThroughput(const ExecutionMetrics& metrics)
{
    double bytesPerSecond = NAN;
    double tuplesPerSecond = NAN;
    const auto elapsed = metrics.elapsed().count();
    if (elapsed > 0 && metrics.bytesProcessed > 0 && metrics.tuplesProcessed > 0)
    {
        bytesPerSecond = static_cast<double>(metrics.bytesProcessed) / elapsed;
        tuplesPerSecond = static_cast<double>(metrics.tuplesProcessed) / elapsed;
    }
    const auto formatUnits = [](double throughput)
    {
        static constexpr std::array<std::string_view, 5> Units{"", "k", "M", "G", "T"};
        size_t unit = 0;
        while (throughput >= 1000 && unit < Units.size() - 1)
        {
            throughput /= 1000;
            ++unit;
        }
        return fmt::format("{:.3f} {}", throughput, Units[unit]);
    };
    return fmt::format("{}B/s / {}Tup/s", formatUnits(bytesPerSecond), formatUnits(tuplesPerSecond));
}

}

bool TestSelection::contains(const TestCaseId& id) const
{
    return includeAll || std::ranges::find(cases, id) != cases.end();
}

std::optional<std::string_view> TestSelection::skipReason(const TestCaseId& id) const
{
    const auto skip = std::ranges::find(intentionalSkips, id, &IntentionalCaseSkip::id);
    return skip == intentionalSkips.end() ? std::nullopt : std::optional<std::string_view>{skip->reason};
}

ConsoleRunReporter::ConsoleRunReporter(const ResolvedRun& run, const bool showPerformance) : run(run), showPerformance(showPerformance)
{
}

std::expected<void, ReportingDiagnostic> ConsoleRunReporter::publish(const RunEvent& event)
{
    if (const auto* started = std::get_if<RunStarted>(&event))
    {
        completed = 0;
        unbounded = std::holds_alternative<UntilCancelled>(started->plan.repetition);
        total = started->plan.selection.includeAll ? run.cases.size() : started->plan.selection.cases.size();
        if (const auto* repetitions = std::get_if<FixedRepetitions>(&started->plan.repetition))
        {
            total *= repetitions->count;
        }
        return {};
    }
    if (const auto* finishedCase = std::get_if<CaseFinished>(&event))
    {
        ++completed;
        if (unbounded)
        {
            total = completed;
        }
        const auto& result = finishedCase->result;
        const auto& testCase = run.testCase(result.id);
        const auto testName = result.id.source.relativeTestFile.generic_string();
        const auto queryNumber = result.id.source.queryNumber.toString();
        const auto counter = std::to_string(completed);
        const auto progress
            = total == 0 ? 100.0 : std::clamp(static_cast<double>(completed) / static_cast<double>(total) * 100.0, 0.0, 100.0);

        auto overrides = run.environment(testCase.environment).configuration.values;
        std::ranges::sort(overrides);
        std::string overrideText;
        if (!overrides.empty())
        {
            overrideText = fmt::format(
                " [{}]",
                fmt::join(
                    overrides | std::views::transform([](const auto& entry) { return fmt::format("{}={}", entry.first, entry.second); }),
                    ", "));
        }

        static constexpr size_t CounterWidth = 3;
        static constexpr size_t QueryNumberWidth = 2;
        static constexpr size_t StatusColumn = 120;
        std::cout << std::string(CounterWidth > counter.size() ? CounterWidth - counter.size() : 0, ' ');
        std::cout << counter << "/" << total << fmt::format(" ({:5.1f}%) ", progress);
        std::cout << testName << ":";
        std::cout << std::string(QueryNumberWidth > queryNumber.size() ? QueryNumberWidth - queryNumber.size() : 0, '0');
        std::cout << queryNumber << overrideText;
        const auto used = testName.size() + QueryNumberWidth + overrideText.size();
        std::cout << std::string(used < StatusColumn ? StatusColumn - used : 0, '.');

        std::string performance;
        if (showPerformance && result.metrics.started && result.metrics.finished)
        {
            performance = fmt::format(" in {} ({})", result.metrics.elapsed(), formatThroughput(result.metrics));
        }
        switch (result.verdict)
        {
            case Verdict::Passed:
                fmt::print(fmt::emphasis::bold | fg(fmt::color::green), "PASSED {}\n", performance);
                break;
            case Verdict::Failed:
                fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "FAILED {}\n", performance);
                std::cout << "===================================================================\n";
                std::cout << primarySql(testCase.action) << '\n';
                std::cout << "===================================================================\n";
                fmt::print(
                    fmt::emphasis::bold | fg(fmt::color::red),
                    "Error: {}\n",
                    fmt::join(result.diagnostics | std::views::transform(&Diagnostic::message), "\n"));
                std::cout << "===================================================================\n";
                break;
            case Verdict::Skipped:
                fmt::print(
                    fmt::emphasis::bold | fg(fmt::color::yellow),
                    "SKIPPED{}\n",
                    result.diagnostics.empty()
                        ? std::string{}
                        : fmt::format(" ({})", fmt::join(result.diagnostics | std::views::transform(&Diagnostic::message), "; ")));
                break;
        }
        return {};
    }
    return {};
}

BenchmarkRunReporter::BenchmarkRunReporter(const ResolvedRun& run, std::filesystem::path outputFile)
    : run(run), outputFile(std::move(outputFile))
{
}

std::expected<void, ReportingDiagnostic> BenchmarkRunReporter::publish(const RunEvent& event)
{
    if (std::holds_alternative<RunStarted>(event))
    {
        results.clear();
        return {};
    }
    if (const auto* finishedCase = std::get_if<CaseFinished>(&event))
    {
        if (finishedCase->result.verdict == Verdict::Passed)
        {
            results.push_back(finishedCase->result);
        }
        return {};
    }
    if (!std::holds_alternative<RunFinished>(event))
    {
        return {};
    }

    std::vector<BenchmarkResult> benchmarkResults;
    benchmarkResults.reserve(results.size());
    for (const auto& result : results)
    {
        const auto elapsed = result.metrics.elapsed().count();
        const auto& testCase = run.testCase(result.id);
        auto configuration = run.environment(testCase.environment).configuration.values;
        std::ranges::sort(configuration);
        const auto configurationLabel = configuration.empty()
            ? std::string{}
            : fmt::format(
                  " [{}]",
                  fmt::join(
                      configuration
                          | std::views::transform([](const auto& entry) { return fmt::format("{}={}", entry.first, entry.second); }),
                      ", "));
        benchmarkResults.push_back(BenchmarkResult{
            .queryName = fmt::format(
                "{}:{}:variant={}{}",
                result.id.source.relativeTestFile.generic_string(),
                result.id.source.queryNumber,
                result.id.configurationVariant,
                configurationLabel),
            .time = elapsed,
            .bytesPerSecond = elapsed > 0 ? static_cast<double>(result.metrics.bytesProcessed) / elapsed : NAN,
            .tuplesPerSecond = elapsed > 0 ? static_cast<double>(result.metrics.tuplesProcessed) / elapsed : NAN});
    }
    const auto serialized = rfl::json::write(benchmarkResults, rfl::json::pretty);
    std::cout << serialized;
    std::ofstream output(outputFile);
    if (!output)
    {
        return std::unexpected(ReportingDiagnostic{.message = fmt::format("Failed to open benchmark output {}", outputFile)});
    }
    output << serialized;
    output.flush();
    const auto writeFailed = !output;
    output.close();
    if (writeFailed)
    {
        return std::unexpected(ReportingDiagnostic{.message = fmt::format("Failed to write benchmark output {}", outputFile)});
    }
    if (!output)
    {
        return std::unexpected(ReportingDiagnostic{.message = fmt::format("Failed to close benchmark output {}", outputFile)});
    }
    return {};
}

CompositeRunReporter::CompositeRunReporter(std::vector<std::reference_wrapper<RunReporter>> reporters) : reporters(std::move(reporters))
{
}

std::expected<void, ReportingDiagnostic> CompositeRunReporter::publish(const RunEvent& event)
{
    for (auto& reporter : reporters)
    {
        if (auto published = reporter.get().publish(event); !published)
        {
            return published;
        }
    }
    return {};
}

}

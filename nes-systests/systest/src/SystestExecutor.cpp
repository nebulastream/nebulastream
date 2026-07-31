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

#include <SystestExecutor.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <stop_token>
#include <string>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>
#include <unistd.h>
#include <Identifiers/NESStrongTypeYaml.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestConfiguration.hpp>
#include <SystestCoordinator.hpp>
#include <SystestExecutionBackend.hpp>
#include <SystestParser.hpp>
#include <SystestPreparation.hpp>
#include <SystestReporter.hpp>
#include <SystestResolver.hpp>
#include <SystestRun.hpp>
#include <SystestState.hpp>
#include <SystestValidation.hpp>

extern void enable_memcom();

namespace NES
{
namespace
{

std::filesystem::path relativeTestFile(const std::filesystem::path& testFile, const std::filesystem::path& discoveryRoot)
{
    std::error_code error;
    auto relative = std::filesystem::relative(testFile, discoveryRoot, error);
    if (error || relative.empty())
    {
        return testFile.filename();
    }
    return relative.lexically_normal();
}

struct ParsedFiles
{
    std::vector<Systest::ParsedTestFile> files;
    size_t loaded = 0;
};

ParsedFiles
parseTestFiles(const Systest::TestFileMap& discoveredTestFiles, const SystestConfiguration& config, const std::stop_token stopToken)
{
    std::vector<std::reference_wrapper<const Systest::TestFile>> orderedFiles;
    orderedFiles.reserve(discoveredTestFiles.size());
    for (const auto& testFile : discoveredTestFiles | std::views::values)
    {
        orderedFiles.emplace_back(testFile);
    }
    std::ranges::sort(orderedFiles, {}, [](const auto& testFile) { return testFile.get().file; });

    ParsedFiles result;
    for (const auto& reference : orderedFiles)
    {
        if (stopToken.stop_requested())
        {
            break;
        }
        const auto& testFile = reference.get();
        std::cout << "Loading queries from test file: file://" << testFile.getLogFilePath() << '\n' << std::flush;
        try
        {
            Systest::SystestParser parser;
            parser.registerSubstitutionRule(Systest::SystestParser::SubstitutionRule{
                .keyword = "TESTDATA", .ruleFunction = [&](std::string& substitute) { substitute = config.testDataDir.getValue(); }});
            parser.registerSubstitutionRule(Systest::SystestParser::SubstitutionRule{
                .keyword = "CONFIG/",
                .ruleFunction = [&](std::string& substitute)
                {
                    substitute = config.configDir.getValue();
                    if (!substitute.empty() && substitute.back() != '/')
                    {
                        substitute.push_back('/');
                    }
                }});
            if (!parser.loadFile(testFile.file, relativeTestFile(testFile.file, config.testsDiscoverDir.getValue())))
            {
                throw TestException("Could not successfully load test file://{}", testFile.file.string());
            }
            auto parsed = parser.parse();
            std::unordered_set<Systest::SystestQueryId> found;
            for (const auto& testCase : parsed.cases)
            {
                found.insert(testCase.key.queryNumber);
            }
            if (testFile.hasQueryNumberSelection())
            {
                std::erase_if(
                    parsed.cases,
                    [&](const Systest::ParsedCase& testCase) { return !testFile.isQueryNumberSelected(testCase.key.queryNumber); });
                for (auto& testCase : parsed.cases)
                {
                    if (testCase.runAfter && !testFile.isQueryNumberSelected(testCase.runAfter->queryNumber))
                    {
                        testCase.runAfter.reset();
                    }
                }
            }
            for (const auto selected : testFile.onlyEnableQueriesWithTestQueryNumber)
            {
                if (!found.contains(selected))
                {
                    std::cerr << fmt::format(
                        "Warning: Query number {} specified via command line argument but not found in file://{}",
                        selected,
                        testFile.file.string());
                }
            }
            for (const auto& range : testFile.queryNumberRanges)
            {
                if (std::ranges::none_of(found, [&](const Systest::SystestQueryId queryNumber) { return range.contains(queryNumber); }))
                {
                    std::cerr << fmt::format(
                        "Warning: Query range {}-{} specified via command line argument but not found in file://{}",
                        range.first,
                        range.last,
                        testFile.file.string());
                }
            }
            result.files.push_back(std::move(parsed));
            ++result.loaded;
        }
        catch (const Exception& exception)
        {
            tryLogCurrentException();
            std::cerr << fmt::format(
                "Loading test file://{} failed: {}Could not successfully parse and bind test file://{}\n",
                testFile.getLogFilePath(),
                exception.what(),
                testFile.file.string());
        }
    }
    return result;
}

void createSymlink(const std::filesystem::path& absoluteLogPath, const std::filesystem::path& symlinkPath)
{
    std::error_code errorCode;
    const auto relativeLogPath = relative(absoluteLogPath, symlinkPath.parent_path(), errorCode);
    if (errorCode)
    {
        std::cerr << "Error calculating relative path during logger setup: " << errorCode.message() << "\n";
        return;
    }

    if (exists(symlinkPath, errorCode) || is_symlink(symlinkPath, errorCode))
    {
        std::filesystem::remove(symlinkPath, errorCode);
        if (errorCode)
        {
            std::cerr << "Error removing existing symlink during logger setup:  " << errorCode.message() << "\n";
        }
    }

    try
    {
        create_symlink(relativeLogPath, symlinkPath);
    }
    catch (const std::filesystem::filesystem_error& exception)
    {
        std::cerr << "Error creating symlink during logger setup: " << exception.what() << '\n';
    }
}

void setupLogging(const SystestConfiguration& config)
{
    std::filesystem::path absoluteLogPath;
    const std::filesystem::path logDir = std::filesystem::path(PATH_TO_BINARY_DIR) / "nes-systests";

    if (config.logFilePath.getValue().empty())
    {
        std::error_code errorCode;
        create_directories(logDir, errorCode);
        if (errorCode)
        {
            std::cerr << "Error creating log directory during logger setup: " << errorCode.message() << "\n";
            return;
        }

        const auto now = std::chrono::system_clock::now();
        const auto pid = ::getpid();
        absoluteLogPath = logDir / fmt::format("SystemTest_{:%Y-%m-%d_%H-%M-%S}_{:d}.log", now, pid);
    }
    else
    {
        absoluteLogPath = config.logFilePath.getValue();
        const std::filesystem::path parentDir = absoluteLogPath.parent_path();
        if (!exists(parentDir) || !is_directory(parentDir))
        {
            fmt::println(std::cerr, "Error creating log file during logger setup: directory does not exist: file://{}", parentDir.string());
            std::exit(1);
        }
    }

    fmt::println(std::cout, "Find the log at: file://{}", absoluteLogPath.string());
    Logger::setupLogging(absoluteLogPath.string(), LogLevel::LOG_DEBUG, false);
    createSymlink(absoluteLogPath, logDir / "latest.log");
}

}

SystestExecutor::SystestExecutor(SystestConfiguration config) : config(std::move(config))
{
}

SystestExecutorResult SystestExecutor::executeSystests(const std::stop_token stopToken)
{
    const auto cancelledResult = []
    {
        return SystestExecutorResult{
            .returnType = SystestExecutorResult::ReturnType::FAILED,
            .outputMessage = "The systest run was cancelled before completion.",
            .errorCode = ErrorCode::QueryStatusFailed};
    };

    CPPTRACE_TRY
    {
        if (stopToken.stop_requested())
        {
            return cancelledResult();
        }
        setupLogging(config);
        std::filesystem::remove_all(config.workingDir.getValue());
        std::filesystem::create_directory(config.workingDir.getValue());

        const auto discoveredTestFiles = Systest::loadTestFileMap(config);
        auto parsed = parseTestFiles(discoveredTestFiles, config, stopToken);
        if (stopToken.stop_requested())
        {
            return cancelledResult();
        }
        if (parsed.loaded != discoveredTestFiles.size())
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = "Could not load all test files. Terminating.",
                .errorCode = ErrorCode::TestException};
        }

        auto resolved = Systest::resolveSystestFiles(std::move(parsed.files), config.clusterConfig);
        if (stopToken.stop_requested())
        {
            return cancelledResult();
        }
        if (!resolved)
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = resolved.error().what(),
                .errorCode = resolved.error().code()};
        }
        std::cout << fmt::format(
            "Loaded {}/{} test files containing a total of {} queries\n", parsed.loaded, discoveredTestFiles.size(), resolved->cases.size())
                  << std::flush;
        if (config.benchmark.getValue() && config.endlessMode.getValue())
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = "Benchmark and endless execution modes cannot be combined.",
                .errorCode = ErrorCode::InvalidConfigParameter};
        }

        auto prepared = Systest::prepareSystestRun(
            std::move(*resolved),
            config.workingDir.getValue(),
            config.testDataDir.getValue(),
            config.queryOptimizerConfig.value_or(QueryOptimizerConfiguration{}));
        if (stopToken.stop_requested())
        {
            return cancelledResult();
        }
        if (!prepared)
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = prepared.error().what(),
                .errorCode = prepared.error().code()};
        }
        if (prepared->resolved.cases.empty())
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = "No queries were run.",
                .errorCode = ErrorCode::TestException};
        }

        if (!config.remoteWorker.getValue())
        {
            enable_memcom();
        }
        auto singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value_or(SingleNodeWorkerConfiguration{});
        if (!config.workerConfig.getValue().empty())
        {
            singleNodeWorkerConfiguration.workerConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig);
        }

        std::unique_ptr<Systest::ExecutionBackend> backend;
        if (config.remoteWorker.getValue())
        {
            backend = std::make_unique<Systest::RemoteExecutionBackend>(prepared->executions);
        }
        else
        {
            backend = std::make_unique<Systest::EmbeddedExecutionBackend>(prepared->executions, singleNodeWorkerConfiguration);
        }

        Systest::RunSetup runSetup{
            .selection = Systest::TestSelection{.includeAll = false, .cases = prepared->executions->ids(), .intentionalSkips = {}},
            .ordering = Systest::
                OrderingPolicy{.kind = config.randomQueryOrder.getValue() ? Systest::OrderingKind::Shuffled : Systest::OrderingKind::SourceOrder, .seed = std::nullopt},
            .concurrency = Systest::ConcurrencyPolicy{.maximumActiveCases = static_cast<size_t>(config.numberConcurrentQueries.getValue())},
            .repetition = config.endlessMode.getValue() ? Systest::RepetitionPolicy{Systest::UntilCancelled{}}
                                                        : Systest::RepetitionPolicy{Systest::Once{}},
            .failurePolicy
            = config.endlessMode.getValue() ? Systest::IndependentFailurePolicy::FailFast : Systest::IndependentFailurePolicy::Continue,
            .deadlines = {},
            .validation = {},
            .metrics = Systest::MetricsPolicy{
                .collect = config.benchmark.getValue() || config.showQueryPerformance.getValue(),
                .report = config.benchmark.getValue() || config.showQueryPerformance.getValue()}};

        if (config.endlessMode.getValue())
        {
            std::cout << fmt::format(
                "Running endlessly over a total of {} queries (across all configuration overrides).\n", prepared->executions->ids().size());
        }
        if (config.benchmark.getValue())
        {
            runSetup.concurrency.maximumActiveCases = 1;
            for (const auto& testCase : prepared->resolved.cases)
            {
                std::optional<std::string> reason;
                if (const auto* action = std::get_if<Systest::QueryAction>(&testCase->action);
                    action && action->kind == Systest::QueryKind::Explain)
                {
                    reason = "Skipped because EXPLAIN cases are not benchmarkable";
                }
                else if (std::holds_alternative<Systest::DifferentialAction>(testCase->action))
                {
                    reason = "Skipped because differential cases are not benchmarkable";
                }
                else if (std::holds_alternative<Systest::ErrorExpectation>(testCase->expectation))
                {
                    reason = "Skipped because cases expecting an error are not benchmarkable";
                }
                if (reason)
                {
                    runSetup.selection.intentionalSkips.push_back(
                        Systest::IntentionalCaseSkip{.id = testCase->id, .reason = std::move(*reason)});
                }
            }
        }

        Systest::FileResultDecoder decoder;
        Systest::ResultComparator resultComparator;
        Systest::TextComparator textComparator;
        Systest::CaseValidator validator(decoder, resultComparator, textComparator, *prepared->executions);
        Systest::ConsoleRunReporter consoleReporter(prepared->resolved, runSetup.metrics.report);
        std::vector<std::reference_wrapper<Systest::RunReporter>> reporters{consoleReporter};
        std::unique_ptr<Systest::BenchmarkRunReporter> benchmarkReporter;
        if (config.benchmark.getValue())
        {
            benchmarkReporter = std::make_unique<Systest::BenchmarkRunReporter>(
                prepared->resolved, std::filesystem::path(config.workingDir.getValue()) / "BenchmarkResults.json");
            reporters.emplace_back(*benchmarkReporter);
        }
        Systest::CompositeRunReporter reporter(std::move(reporters));
        const auto summary = Systest::RunCoordinator{}.run(*prepared, std::move(runSetup), *backend, validator, reporter, stopToken);

        const auto fatalDiagnostic = std::ranges::find_if(
            summary.diagnostics,
            [](const Systest::Diagnostic& diagnostic)
            {
                return diagnostic.kind == Systest::DiagnosticKind::Execution || diagnostic.kind == Systest::DiagnosticKind::Reporting
                    || diagnostic.kind == Systest::DiagnosticKind::Scheduling;
            });
        if (summary.failed != 0 || summary.cancelled || fatalDiagnostic != summary.diagnostics.end())
        {
            std::vector<std::string> failedQueries;
            for (const auto& result : summary.results)
            {
                if (result.verdict != Systest::Verdict::Failed)
                {
                    continue;
                }
                const auto& testCase = prepared->resolved.testCase(result.id);
                auto displayName = result.id.source.relativeTestFile;
                displayName.replace_extension();
                failedQueries.push_back(fmt::format(
                    "[{}, systest -t {}:{}]\n  {}",
                    displayName.generic_string(),
                    testCase.source.file,
                    result.id.source.queryNumber,
                    fmt::join(result.diagnostics | std::views::transform(&Systest::Diagnostic::message), "\n  ")));
            }
            std::string outputMessage;
            if (!failedQueries.empty())
            {
                outputMessage = fmt::format("The following queries failed:\n[Name, Command]\n- {}", fmt::join(failedQueries, "\n- "));
            }
            else if (fatalDiagnostic != summary.diagnostics.end())
            {
                outputMessage = fmt::format("The systest run failed: {}", fatalDiagnostic->message);
            }
            else
            {
                outputMessage = "The systest run was cancelled before completion.";
            }
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = std::move(outputMessage),
                .errorCode = ErrorCode::QueryStatusFailed};
        }
        if (summary.passed + summary.failed == 0)
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = config.benchmark.getValue() ? "No benchmarkable queries were run." : "No queries were run.",
                .errorCode = ErrorCode::QueryStatusFailed};
        }
        return {.returnType = SystestExecutorResult::ReturnType::SUCCESS, .outputMessage = "\nAll queries passed."};
    }
    CPPTRACE_CATCH(const std::exception& exception)
    {
        tryLogCurrentException();
        const auto* nesException = dynamic_cast<const Exception*>(&exception);
        const auto errorCode = nesException == nullptr ? ErrorCode::TestException : nesException->code();
        return {
            .returnType = SystestExecutorResult::ReturnType::FAILED,
            .outputMessage = fmt::format("Failed with exception: {}, {}", errorCode, exception.what()),
            .errorCode = errorCode};
    }
    return {
        .returnType = SystestExecutorResult::ReturnType::FAILED,
        .outputMessage = "Fatal error, should never reach this point.",
        .errorCode = ErrorCode::UnknownException};
}

}

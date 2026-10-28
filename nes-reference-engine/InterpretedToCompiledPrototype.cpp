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

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include <Configuration/WorkerConfiguration.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <CompiledQueryPlan.hpp>
#include <CompositeStatisticListener.hpp>
#include <ExecutableQueryPlan.hpp>
#include <ModelCatalog.hpp>
#include <QueryCompiler.hpp>
#include <QueryId.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <ReferenceNodeEngine.hpp>
#include "PrototypeQuery.hpp"

/// Runs a windowed query over a long-running source (a large CSV file, or an unbounded Generator) and alternates it between the
/// Nautilus interpreter and the JIT with live adaptations that carry over every pipeline's state, including open windows. It starts
/// interpreted, so the first result does not wait for the JIT. A sampler records the input throughput over time into a CSV for
/// plotting. The output must be the exact prefix of an uninterrupted run.
using namespace NES;
using namespace NES::PrototypeQuery;

namespace
{
using Clock = std::chrono::steady_clock;

enum class Version : uint8_t
{
    Interpreted,
    Compiled
};

std::string_view name(Version version)
{
    return version == Version::Interpreted ? "interpreted" : "compiled";
}

/// All versions come from the same SQL and optimizer, so their pipelines correspond by position.
std::vector<std::pair<PipelineId, PipelineId>> transfersBetween(const std::vector<PipelineId>& donor, const CompiledQueryPlan& target)
{
    if (donor.size() != target.pipelines.size())
    {
        throw std::runtime_error("Replacement plan has a different pipeline topology");
    }
    std::vector<std::pair<PipelineId, PipelineId>> transfers;
    for (size_t index = 0; index < donor.size(); ++index)
    {
        transfers.emplace_back(donor[index], target.pipelines[index]->id);
    }
    return transfers;
}

std::vector<PipelineId> pipelineIds(const CompiledQueryPlan& plan)
{
    std::vector<PipelineId> ids;
    for (const auto& pipeline : plan.pipelines)
    {
        ids.push_back(pipeline->id);
    }
    return ids;
}

/// Shares the source across all versions, so a switch keeps reading where the previous version stopped.
ExecutableQueryPlan::SharingIds sharedSource(const CompiledQueryPlan& plan)
{
    if (plan.sources.size() != 1)
    {
        throw std::runtime_error("Expected exactly one source");
    }
    ExecutableQueryPlan::SharingIds sharing;
    sharing.sources.emplace(plan.sources.front().originId, "prototype:interpreted-to-compiled:source");
    return sharing;
}

double seconds(Clock::duration duration)
{
    return std::chrono::duration<double>(duration).count();
}

/// Counts result lines appended to the sink's file. Every result stands for ROWS_PER_RESULT input rows, and windows close as the
/// watermark passes them, so the result rate times ROWS_PER_RESULT is the query's input throughput.
class ResultCounter
{
public:
    explicit ResultCounter(std::filesystem::path output) : output(std::move(output)) { }

    uint64_t count()
    {
        std::ifstream file(output, std::ios::binary);
        file.seekg(static_cast<std::streamoff>(offset));
        std::string chunk(1 << 20, '\0');
        while (file.read(chunk.data(), static_cast<std::streamsize>(chunk.size())) || file.gcount() > 0)
        {
            for (const auto character : std::string_view(chunk.data(), static_cast<size_t>(file.gcount())))
            {
                if (lineStart)
                {
                    resultLine = std::isdigit(static_cast<unsigned char>(character)) != 0;
                }
                lineStart = character == '\n';
                results += static_cast<uint64_t>(lineStart && resultLine);
            }
            offset += static_cast<uint64_t>(file.gcount());
        }
        return results;
    }

private:
    std::filesystem::path output;
    uint64_t offset = 0;
    uint64_t results = 0;
    bool lineStart = true;
    bool resultLine = false;
};

struct Sample
{
    double time;
    double rowsPerSecond;
    /// The version that processed the input during the sample. During a switch that is still the previous version.
    Version running;
    bool switching;
    size_t phase;
};

/// What the main thread is doing, read by the sampler.
struct RunState
{
    std::atomic<Version> running{Version::Interpreted};
    std::atomic<bool> switching{false};
    std::atomic<size_t> phase{0};
};

constexpr auto SAMPLE_INTERVAL = std::chrono::milliseconds(250);

/// Samples the input throughput every SAMPLE_INTERVAL until stopped. Only the sampler thread touches `samples` until it is joined.
void sample(const std::stop_token& stop, ResultCounter& counter, const RunState& state, Clock::time_point started, std::vector<Sample>& samples)
{
    auto previous = counter.count();
    auto previousTime = Clock::now();
    for (auto tick = previousTime + SAMPLE_INTERVAL; not stop.stop_requested(); tick += SAMPLE_INTERVAL)
    {
        const auto running = state.running.load();
        const auto switching = state.switching.load();
        const auto phase = state.phase.load();
        std::this_thread::sleep_until(tick);
        const auto now = Clock::now();
        const auto results = counter.count();
        samples.push_back(
            {.time = seconds(now - started),
             .rowsPerSecond = static_cast<double>(results - previous) * ROWS_PER_RESULT / seconds(now - previousTime),
             .running = running,
             .switching = switching,
             .phase = phase});
        previous = results;
        previousTime = now;
    }
}

/// Median throughput of one phase, excluding switches. The median ignores the first samples after a switch to a faster version, in
/// which it drains the input that queued up while the slower one ran.
double phaseMedian(const std::vector<Sample>& samples, size_t phase)
{
    std::vector<double> rates;
    for (const auto& sample : samples)
    {
        if (sample.phase == phase && not sample.switching)
        {
            rates.push_back(sample.rowsPerSecond);
        }
    }
    if (rates.empty())
    {
        return 0;
    }
    std::ranges::sort(rates);
    return rates[rates.size() / 2];
}
}

int main(int argc, char** argv)
{
    try
    {
        Logger::setupLogging("nes-reference-interpreted-to-compiled-prototype.log", LogLevel::LOG_ERROR);
        loadBuiltinPlugins();
        const std::string usage = "Usage: nes-reference-interpreted-to-compiled-prototype WORK_DIRECTORY [SECONDS_PER_PHASE] [SWITCHES] "
                                  "[file [ROWS] | generator]\n";
        if (argc < 2 || argc > 6)
        {
            std::cerr << usage;
            return 2;
        }
        const std::filesystem::path directory(argv[1]);
        const auto phaseDuration = std::chrono::seconds(argc >= 3 ? std::stoul(argv[2]) : 10);
        const size_t switches = argc >= 4 ? std::stoul(argv[3]) : 4;
        const std::string sourceKind = argc >= 5 ? argv[4] : "file";
        if (sourceKind != "file" && sourceKind != "generator")
        {
            std::cerr << usage;
            return 2;
        }
        std::filesystem::create_directories(directory);
        const auto output = directory / "interpreted-to-compiled-output.csv";
        const auto timeline = directory / "interpreted-to-compiled-throughput.csv";
        std::filesystem::remove(output);
        std::string physicalSource = unboundedGeneratorSource();
        uint64_t fileRows = 0;
        if (sourceKind == "file")
        {
            /// The file must outlast every phase; at the default size the compiled Debug build needs about a minute to read it.
            fileRows = argc == 6 ? std::stoull(argv[5]) : 30'000'000;
            const auto input = directory / "interpreted-to-compiled-input.csv";
            writeInput(input, fileRows);
            physicalSource = fileSource(input);
        }

        auto sources = std::make_shared<SourceCatalog>();
        auto sinks = std::make_shared<SinkCatalog>();
        registerCatalog(sources, sinks, physicalSource, output);
        const RuleBasedOptimizer optimizer(QueryOptimizerConfiguration{}, sources, sinks, std::make_shared<ModelCatalog>());
        WorkerConfiguration configuration;
        configuration.defaultQueryExecution.executionMode.setValue(ExecutionMode::INTERPRETER);
        QueryCompilation::QueryCompiler interpreter(configuration.defaultQueryExecution);
        configuration.defaultQueryExecution.executionMode.setValue(ExecutionMode::COMPILER);
        QueryCompilation::QueryCompiler jit(configuration.defaultQueryExecution);
        const auto queryId = QueryId::createLocal(LocalQueryId(generateUUID()));
        std::optional<LogicalPlan> donorPlan;
        const auto compileVersion = [&](Version version)
        { return compile(WINDOW_QUERY, queryId, optimizer, version == Version::Interpreted ? interpreter : jit, donorPlan); };

        RunState state;
        std::vector<Sample> samples;
        std::vector<double> switchSeconds;
        {
            ReferenceNodeEngine engine(configuration, std::make_shared<CompositeStatisticListener>(), Host{"localhost"});
            const auto submitted = Clock::now();
            auto plan = compileVersion(Version::Interpreted);
            auto current = pipelineIds(*plan);
            auto sharing = sharedSource(*plan);
            engine.startQuery(queryId, std::move(plan), std::move(sharing));
            waitForOutput(output, std::chrono::seconds(30), [](const Windows& windows) { return not windows.empty(); });
            std::cout << std::format("Source: {}\nFirst result after {:.2f} s (interpreted)\n", sourceKind, seconds(Clock::now() - submitted));

            ResultCounter counter(output);
            const auto started = Clock::now();
            std::jthread sampler([&](const std::stop_token& stop) { sample(stop, counter, state, started, samples); });
            for (size_t phase = 0; phase <= switches; ++phase)
            {
                std::this_thread::sleep_for(phaseDuration);
                if (phase == switches)
                {
                    break;
                }
                /// Starting the next version's stages runs the JIT; the running version keeps processing until the commit replaces it.
                const auto next = state.running.load() == Version::Interpreted ? Version::Compiled : Version::Interpreted;
                auto replacement = compileVersion(next);
                const auto transfers = transfersBetween(current, *replacement);
                current = pipelineIds(*replacement);
                auto replacementSharing = sharedSource(*replacement);
                state.switching = true;
                const auto switchStarted = Clock::now();
                if (not engine.adaptQuery(std::move(replacement), transfers, std::move(replacementSharing)))
                {
                    throw std::runtime_error(std::format("Reference query manager rejected the switch to the {} query", name(next)));
                }
                switchSeconds.push_back(seconds(Clock::now() - switchStarted));
                state.running = next;
                state.phase = phase + 1;
                state.switching = false;
                std::cout << std::format("Switched to {} in {:.1f} s\n", name(next), switchSeconds.back());
            }
            sampler.request_stop();
            sampler.join();
            engine.stopQuery(queryId);
        }

        std::ofstream csv(timeline);
        csv << "time_s,rows_per_s,running,switching\n";
        for (const auto& sample : samples)
        {
            csv << std::format("{:.3f},{:.0f},{},{}\n", sample.time, sample.rowsPerSecond, name(sample.running), sample.switching ? 1 : 0);
        }
        std::cout << "Median throughput per phase:\n";
        auto version = Version::Interpreted;
        for (size_t phase = 0; phase <= switches; ++phase)
        {
            std::cout << std::format("  {} {:<11} {:>12.0f} rows/s\n", phase, name(version), phaseMedian(samples, phase));
            version = version == Version::Interpreted ? Version::Compiled : Version::Interpreted;
        }
        std::cout << "Throughput timeline: " << timeline.string() << '\n';

        const auto windows = readWindows(output);
        verifyWindowPrefix(windows);
        if (fileRows != 0 && windows.size() * ROWS_PER_RESULT + WINDOW_SIZE >= fileRows)
        {
            std::cout << "Warning: the input file ran out during the run; increase ROWS for a steady final phase\n";
        }
        std::cout << "Verified " << windows.size() << " results (" << windows.size() * ROWS_PER_RESULT
                  << " rows) as the exact prefix of an uninterrupted run\n";
        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "Interpreted-to-compiled prototype failed: " << error.what() << '\n';
        return 1;
    }
}

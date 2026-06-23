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

/// nes-bench-runner -- a second, profile-clean way to measure end-to-end query throughput.
///
/// Motivation: `systest -b` runs queries strictly one at a time and, between runs, does heavy
/// per-query work on the main thread (file_size + a full newline scan of the input + result
/// checking). That harness work dominates `perf` profiles and steals cores, and it makes
/// running multiple queries concurrently impossible. This driver reuses the exact same engine
/// stack as `systest` (the embedded QueryManager over a SingleNodeWorker, fed by the systest
/// binder), but replaces the run loop with: register all queries up front (paying the compile
/// cost before timing), arm a wall-clock timer, start them all concurrently, then drain to
/// completion. During the timed window the only main-thread work is a sleep-bound poll loop,
/// so the profile is dominated by engine/formatter/source code, not the harness.
///
/// It reports two numbers: (1) per-query throughput from the engine's own running->stop metrics
/// (identical to what `systest -b` reports), and (2) aggregate wall-clock throughput across the
/// whole concurrent set -- the measurement `-b` cannot produce.

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <unistd.h>

#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeYaml.hpp> ///NOLINT(misc-include-cleaner)
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Signal.hpp>
#include <argparse/argparse.hpp>
#include <fmt/format.h>
#include <nlohmann/json.hpp> ///NOLINT(misc-include-cleaner)
#include <yaml-cpp/yaml.h> ///NOLINT(misc-include-cleaner)
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QuerySubmitter.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestBinder.hpp>
#include <SystestConfiguration.hpp>
#include <SystestResultCheck.hpp>
#include <SystestState.hpp>
#include <Thread.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

/// Rust FFI: route worker-to-worker channels through shared memory instead of real sockets so the
/// embedded single-node worker binds no ports. Declared the same way the systest executor does.
extern void enable_memcom();

using namespace std::literals;

namespace
{
using argparse::ArgumentParser;

/// Cached size + line-count of a single input file. Computed ONCE per distinct path, always
/// outside the timed window.
struct FileStat
{
    uint64_t bytes = 0;
    uint64_t tuples = 0;
};

FileStat statFile(const std::filesystem::path& path, std::unordered_map<std::string, FileStat>& cache)
{
    const auto key = path.string();
    if (const auto it = cache.find(key); it != cache.end())
    {
        return it->second;
    }
    FileStat stat;
    std::error_code errorCode;
    stat.bytes = std::filesystem::exists(path, errorCode) ? std::filesystem::file_size(path, errorCode) : 0;
    if (stat.bytes > 0)
    {
        std::ifstream input(path, std::ios::binary);
        stat.tuples = static_cast<uint64_t>(std::count(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>(), '\n'));
    }
    cache.emplace(key, stat);
    return stat;
}

/// Total input bytes/tuples a single query reads, summing every source file times its number of
/// occurrences in the plan. Mirrors the accounting in SystestRunner::runQueriesAndBenchmark, but
/// kept strictly out of the timed window.
FileStat accountQuery(const NES::Systest::SystestQuery& query, std::unordered_map<std::string, FileStat>& cache)
{
    FileStat total;
    if (not query.planInfoOrException.has_value())
    {
        return total;
    }
    for (const auto& [sourceFile, occurrences] : query.planInfoOrException.value().sourcesToFilePathsAndCounts | std::views::values)
    {
        const auto stat = statFile(sourceFile.getRawValue(), cache);
        total.bytes += stat.bytes * occurrences;
        total.tuples += stat.tuples * occurrences;
    }
    return total;
}

/// A throughput string like "4.62 GB/s / 143.1 MTup/s".
std::string formatRate(double perSecond, std::string_view unit)
{
    static constexpr std::array prefixes = {""sv, "k"sv, "M"sv, "G"sv, "T"sv};
    size_t prefix = 0;
    while (perSecond >= 1000.0 and prefix + 1 < prefixes.size())
    {
        perSecond /= 1000.0;
        ++prefix;
    }
    return fmt::format("{:.2f} {}{}/s", perSecond, prefixes.at(prefix), unit);
}

/// Loads a cluster topology YAML into a SystestClusterConfiguration. Replicates the worker-parsing
/// loop in SystestStarter::applyExecutionOptions so the same topology files work unchanged.
NES::SystestClusterConfiguration loadClusterConfig(const std::string& path)
{
    auto yaml = YAML::LoadFile(path);
    NES::SystestClusterConfiguration clusterConfig;
    clusterConfig.allowSinkPlacement = yaml["allow_sink_placement"].as<std::vector<NES::Host>>();
    clusterConfig.allowSourcePlacement = yaml["allow_source_placement"].as<std::vector<NES::Host>>();
    for (const auto& worker : yaml["workers"])
    {
        NES::SingleNodeWorkerConfiguration workerConfig;
        if (worker["config"].IsDefined() and not worker["config"].IsNull())
        {
            workerConfig.overwriteConfigWithYAMLNode(worker["config"]);
        }
        clusterConfig.workers.push_back(NES::WorkerConfig{
            .host = worker["host"].as<NES::Host>(),
            .dataAddress = worker["data_address"].as<std::string>(),
            .maxOperators = worker["max_operators"].IsDefined()
                ? NES::Capacity(NES::CapacityKind::Limited{worker["max_operators"].as<size_t>()})
                : NES::Capacity(NES::CapacityKind::Unlimited{}),
            .downstream = worker["downstream"].IsDefined() ? worker["downstream"].as<std::vector<NES::Host>>() : std::vector<NES::Host>{},
            .config = workerConfig,
        });
    }
    return clusterConfig;
}

struct BenchArgs
{
    NES::SystestConfiguration systestConfig;
    NES::SingleNodeWorkerConfiguration workerConfig;
    uint64_t replication = 1;
    uint64_t repeat = 1;
    bool pauseBeforeStart = false;
    uint64_t pauseBatch = 0; ///< which 0-based --repeat batch to pause on (NES_BENCH_PAUSE_BATCH); use a late batch to profile a HOT run (pool already faulted)
    bool verify = false;
    std::string jsonOutput;
};

void configureArgumentParser(ArgumentParser& program)
{
    program.add_argument("-t", "--testLocation")
        .help("test file or directory to run, e.g. FormatterInMemory.test:2 (':' selects query numbers)");
    program.add_argument("--data").help("path to the directory where input data files are stored");
    program.add_argument("--workingDir").help("working directory for source/result files (it is wiped at startup)");
    program.add_argument("-c", "--clusterConfig").nargs(1).help("cluster topology YAML (default: single-node)");
    program.add_argument("--optimizer")
        .default_value<std::vector<std::string>>({})
        .append()
        .help("override optimizer defaults, e.g. join_strategy=HASH_JOIN");
    program.add_argument("--replication")
        .help("run this many concurrent copies of EACH selected query (default: 1)")
        .default_value(1)
        .scan<'i', int>();
    program.add_argument("--repeat").help("repeat the whole timed batch this many times (default: 1)").default_value(1).scan<'i', int>();
    program.add_argument("--pause-before-start")
        .flag()
        .help("after registering+compiling, print the PID and wait for Enter so a profiler can attach to just the run window");
    program.add_argument("--verify").flag().help("run a sequential correctness pass (result check) BEFORE the timed run");
    program.add_argument("--json").help("write machine-readable results to this path (default: <workingDir>/BenchRunnerResults.json)");
    program.add_argument("-d", "--debug").flag().help("enable debug logging (noisy; not for clean profiling runs)");
    program.add_argument("--")
        .help("arguments forwarded to the worker config, e.g. -- --worker.query_engine.number_of_worker_threads=8")
        .default_value(std::vector<std::string>{})
        .remaining();
}

/// Mirrors the (small) subset of SystestStarter we need: resolve -t into directlySpecifiedTestFiles
/// or testsDiscoverDir, and parse ':n,m-k' query-number selectors.
void applyTestLocation(const ArgumentParser& program, NES::SystestConfiguration& config)
{
    if (not program.is_used("--testLocation"))
    {
        return;
    }
    auto raw = program.get<std::string>("--testLocation");
    std::filesystem::path testFilePath = raw;
    if (const auto delimiter = raw.find(':'); delimiter != std::string::npos)
    {
        testFilePath = raw.substr(0, delimiter);
        std::string numbers = raw.substr(delimiter + 1);
        std::string item;
        std::stringstream numberStream(numbers);
        while (std::getline(numberStream, item, ','))
        {
            if (const auto dash = item.find('-'); dash != std::string::npos)
            {
                const int start = std::stoi(item.substr(0, dash));
                const int end = std::stoi(item.substr(dash + 1));
                for (int i = start; i <= end; ++i)
                {
                    config.testQueryNumbers.add(i);
                }
                continue;
            }
            config.testQueryNumbers.add(std::stoi(item));
        }
    }

    if (std::filesystem::is_directory(testFilePath))
    {
        config.testsDiscoverDir = testFilePath.string();
    }
    else if (std::filesystem::exists(testFilePath))
    {
        config.directlySpecifiedTestFiles = std::filesystem::canonical(testFilePath).string();
    }
    else
    {
        std::cerr << "test file not found: " << testFilePath << '\n';
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }
}

BenchArgs parseArguments(int argc, const char** argv)
{
    ArgumentParser program("nes-bench-runner");
    configureArgumentParser(program);
    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err)
    {
        std::cerr << "Error parsing arguments: " << err.what() << '\n' << program << '\n';
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }

    BenchArgs args;
    NES::SystestConfiguration& config = args.systestConfig;

    if (program.is_used("--data"))
    {
        config.testDataDir = program.get<std::string>("--data");
    }
    if (program.is_used("--workingDir"))
    {
        config.workingDir = program.get<std::string>("--workingDir");
    }
    applyTestLocation(program, config);

    const auto clusterConfigPath = program.is_used("--clusterConfig")
        ? program.get<std::string>("--clusterConfig")
        : std::string{TEST_CONFIGURATION_DIR} + "/topologies/single-node.yaml";
    try
    {
        config.clusterConfig = loadClusterConfig(clusterConfigPath);
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error loading cluster config '" << clusterConfigPath << "': " << e.what() << '\n';
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }

    if (const auto optimizerOverrides = program.get<std::vector<std::string>>("--optimizer"); not optimizerOverrides.empty())
    {
        std::unordered_map<std::string, std::string> raw;
        for (const auto& entry : optimizerOverrides)
        {
            if (const auto pos = entry.find('='); pos != std::string::npos)
            {
                raw[entry.substr(0, pos)] = entry.substr(pos + 1);
            }
        }
        NES::QueryOptimizerConfiguration optimizerConfig;
        optimizerConfig.overwriteConfigWithCommandLineInput(raw);
        config.queryOptimizerConfig = optimizerConfig;
    }

    /// Worker knobs after `--` are parsed exactly as systest does, so the same recipe applies
    /// (worker threads, io threads, operator buffer size, global buffers, invoke-mode YAML).
    if (const auto passthrough = program.get<std::vector<std::string>>("--"); not passthrough.empty())
    {
        std::vector<const char*> workerArgv;
        workerArgv.reserve(passthrough.size() + 1);
        workerArgv.push_back("nes-bench-runner");
        for (const auto& arg : passthrough)
        {
            workerArgv.push_back(arg.c_str());
        }
        if (auto loaded
            = NES::loadConfiguration<NES::SingleNodeWorkerConfiguration>(static_cast<int>(workerArgv.size()), workerArgv.data()))
        {
            args.workerConfig = std::move(*loaded);
        }
    }

    args.replication = std::max(1, program.get<int>("--replication"));
    args.repeat = std::max(1, program.get<int>("--repeat"));
    args.pauseBeforeStart = program.get<bool>("--pause-before-start") or std::getenv("NES_BENCH_PAUSE_BEFORE_START") != nullptr;
    if (const char* const pauseBatchEnv = std::getenv("NES_BENCH_PAUSE_BATCH"))
    {
        args.pauseBatch = std::strtoull(pauseBatchEnv, nullptr, 10);
    }
    args.verify = program.get<bool>("--verify");
    args.jsonOutput = program.is_used("--json") ? program.get<std::string>("--json") : std::string{};

    NES::Logger::setupLogging("bench-runner.log", program.get<bool>("--debug") ? NES::LogLevel::LOG_DEBUG : NES::LogLevel::LOG_ERROR);
    return args;
}

/// Selects the queries we can actually benchmark: a bound plan, not differential, not error-expecting.
std::vector<NES::Systest::SystestQuery> selectRunnableQueries(std::vector<NES::Systest::SystestQuery> queries)
{
    std::vector<NES::Systest::SystestQuery> runnable;
    for (auto& query : queries)
    {
        if (not query.planInfoOrException.has_value())
        {
            std::cout << "Skipping query that failed to bind: " << query.testName << ":" << query.queryIdInFile.toString() << '\n';
            continue;
        }
        if (query.differentialQueryPlan.has_value()
            or std::holds_alternative<NES::Systest::ExpectedError>(query.expectedResultsOrExpectedError))
        {
            std::cout << "Skipping differential/error query: " << query.testName << ":" << query.queryIdInFile.toString() << '\n';
            continue;
        }
        runnable.push_back(std::move(query));
    }
    return runnable;
}

/// Sequential correctness pass: run each query once and compare its result, BEFORE the timed run.
/// Returns false if any query failed or produced a wrong result.
bool runVerify(const std::vector<NES::Systest::SystestQuery>& queries, NES::Systest::QuerySubmitter& submitter)
{
    bool allPassed = true;
    for (const auto& query : queries)
    {
        auto registered = submitter.registerQuery(query.planInfoOrException.value().queryPlan);
        if (not registered.has_value())
        {
            std::cout << "[verify] FAIL (register) " << query.testName << ":" << query.queryIdInFile.toString() << ": "
                      << registered.error().what() << '\n';
            allPassed = false;
            continue;
        }
        submitter.startQuery(*registered);
        const auto status = submitter.waitForQueryTermination(*registered);
        NES::Systest::RunningQuery runningQuery{
            .systestQuery = query,
            .queryId = *registered,
            .differentialQueryPair = std::nullopt,
            .queryStatus = status,
            .exception = std::nullopt};
        if (status.getGlobalQueryStatus() == NES::DistributedQueryStatus::Failed)
        {
            std::cout << "[verify] FAIL (engine) " << query.testName << ":" << query.queryIdInFile.toString() << '\n';
            allPassed = false;
            continue;
        }
        if (const auto error = NES::checkResult(runningQuery))
        {
            std::cout << "[verify] FAIL (result) " << query.testName << ":" << query.queryIdInFile.toString() << ": " << *error << '\n';
            allPassed = false;
            continue;
        }
        std::cout << "[verify] pass " << query.testName << ":" << query.queryIdInFile.toString() << '\n';
    }
    return allPassed;
}

/// One registered, started, and (after the drain) finished replica.
struct RunRecord
{
    std::string label;
    FileStat input;
    NES::DistributedQueryId queryId{NES::DistributedQueryId::INVALID};
};

void reportBatch(
    const std::vector<RunRecord>& runset,
    const std::unordered_map<NES::DistributedQueryId, NES::DistributedQueryStatusSnapshot>& finished,
    double wallSeconds,
    uint64_t totalBytes,
    uint64_t totalTuples,
    uint64_t replication,
    nlohmann::json& batchJson)
{
    /// Union of all per-query running->stop windows. Aggregate engine throughput over this window
    /// excludes process/engine spin-up (buffer-pool fault-in etc.), giving the true concurrent rate.
    std::optional<std::chrono::system_clock::time_point> earliestRunning;
    std::optional<std::chrono::system_clock::time_point> latestStop;

    std::cout << "\nPer-query (engine running->stop metrics):\n";
    for (const auto& record : runset)
    {
        const auto it = finished.find(record.queryId);
        if (it == finished.end())
        {
            continue;
        }
        const auto metrics = it->second.coalesceQueryMetrics();
        nlohmann::json queryJson{{"query", record.label}, {"bytes", record.input.bytes}, {"tuples", record.input.tuples}};
        if (it->second.getGlobalQueryStatus() == NES::DistributedQueryStatus::Failed)
        {
            std::cout << fmt::format("  {:<28} FAILED\n", record.label);
            queryJson["status"] = "failed";
        }
        else if (metrics.running and metrics.stop)
        {
            const double seconds = std::chrono::duration<double>(*metrics.stop - *metrics.running).count();
            const double bytesPerSecond = seconds > 0 ? static_cast<double>(record.input.bytes) / seconds : 0.0;
            const double tuplesPerSecond = seconds > 0 ? static_cast<double>(record.input.tuples) / seconds : 0.0;
            std::cout << fmt::format(
                "  {:<28} {:>8.4f}s  {}  {}\n", record.label, seconds, formatRate(bytesPerSecond, "B"), formatRate(tuplesPerSecond, "Tup"));
            queryJson["time"] = seconds;
            queryJson["bytesPerSecond"] = bytesPerSecond;
            queryJson["tuplesPerSecond"] = tuplesPerSecond;
            queryJson["status"] = "stopped";
            earliestRunning = earliestRunning ? std::min(*earliestRunning, *metrics.running) : *metrics.running;
            latestStop = latestStop ? std::max(*latestStop, *metrics.stop) : *metrics.stop;
        }
        batchJson["queries"].push_back(queryJson);
    }

    const double engineWindowSeconds
        = (earliestRunning and latestStop) ? std::chrono::duration<double>(*latestStop - *earliestRunning).count() : 0.0;
    const double engineBytesPerSecond = engineWindowSeconds > 0 ? static_cast<double>(totalBytes) / engineWindowSeconds : 0.0;
    const double engineTuplesPerSecond = engineWindowSeconds > 0 ? static_cast<double>(totalTuples) / engineWindowSeconds : 0.0;
    const double wallBytesPerSecond = wallSeconds > 0 ? static_cast<double>(totalBytes) / wallSeconds : 0.0;
    const double wallTuplesPerSecond = wallSeconds > 0 ? static_cast<double>(totalTuples) / wallSeconds : 0.0;
    std::cout << fmt::format(
        "\nAggregate ({} concurrent queries, replication {}, over {:.2f} GB):\n"
        "  engine window (min running -> max stop): {:>8.4f}s  {}  {}\n"
        "  wall clock   (start -> all stopped)    : {:>8.4f}s  {}  {}\n",
        runset.size(),
        replication,
        static_cast<double>(totalBytes) / 1e9,
        engineWindowSeconds,
        formatRate(engineBytesPerSecond, "B"),
        formatRate(engineTuplesPerSecond, "Tup"),
        wallSeconds,
        formatRate(wallBytesPerSecond, "B"),
        formatRate(wallTuplesPerSecond, "Tup"));

    batchJson["concurrency"] = runset.size();
    batchJson["replication"] = replication;
    batchJson["engineWindowSeconds"] = engineWindowSeconds;
    batchJson["engineBytesPerSecond"] = engineBytesPerSecond;
    batchJson["engineTuplesPerSecond"] = engineTuplesPerSecond;
    batchJson["wallSeconds"] = wallSeconds;
    batchJson["wallBytesPerSecond"] = wallBytesPerSecond;
    batchJson["wallTuplesPerSecond"] = wallTuplesPerSecond;
    batchJson["totalBytes"] = totalBytes;
}

/// Registers, starts, drains, and reports one timed batch. Each registration resets the plan id to
/// INVALID so the QueryManager mints a fresh DistributedQueryId -- this lets us register the same
/// plan many times (replication) and across repeats without QueryAlreadyRegistered.
void runBatch(
    const std::vector<NES::Systest::SystestQuery>& queries,
    const std::vector<FileStat>& perQueryStats,
    NES::Systest::QuerySubmitter& submitter,
    const BenchArgs& args,
    bool pauseThisBatch,
    nlohmann::json& batchJson)
{
    std::vector<RunRecord> runset;
    uint64_t totalBytes = 0;
    uint64_t totalTuples = 0;
    for (size_t q = 0; q < queries.size(); ++q)
    {
        for (uint64_t replica = 0; replica < args.replication; ++replica)
        {
            auto plan = queries[q].planInfoOrException.value().queryPlan;
            plan.setQueryId(NES::DistributedQueryId(NES::DistributedQueryId::INVALID));
            auto registered = submitter.registerQuery(plan);
            if (not registered.has_value())
            {
                std::cout << "Skipping query that failed to register: " << queries[q].testName << ": " << registered.error().what() << '\n';
                continue;
            }
            const auto label = fmt::format("{}:{}#{}", queries[q].testName, queries[q].queryIdInFile.toString(), replica);
            runset.push_back(RunRecord{.label = label, .input = perQueryStats[q], .queryId = *registered});
            totalBytes += perQueryStats[q].bytes;
            totalTuples += perQueryStats[q].tuples;
        }
    }

    if (runset.empty())
    {
        std::cout << "No queries registered; nothing to run.\n";
        return;
    }

    if (pauseThisBatch)
    {
        std::cout << fmt::format(
            "\n[paused] PID {} -- {} queries registered + compiled, none running yet.\n"
            "Attach your profiler now (e.g. perf record -p {}), then press Enter to start...\n",
            ::getpid(),
            runset.size(),
            ::getpid());
        std::string ignored;
        std::getline(std::cin, ignored);
    }

    const auto wallStart = std::chrono::steady_clock::now();
    for (const auto& record : runset)
    {
        try
        {
            submitter.startQuery(record.queryId);
        }
        catch (const std::exception& e)
        {
            std::cout << "Failed to start " << record.label << ": " << e.what() << '\n';
        }
    }

    std::unordered_map<NES::DistributedQueryId, NES::DistributedQueryStatusSnapshot> finished;
    while (finished.size() < runset.size())
    {
        for (auto& snapshot : submitter.finishedQueries())
        {
            finished.emplace(snapshot.queryId, std::move(snapshot));
        }
    }
    const auto wallStop = std::chrono::steady_clock::now();

    reportBatch(
        runset,
        finished,
        std::chrono::duration<double>(wallStop - wallStart).count(),
        totalBytes,
        totalTuples,
        args.replication,
        batchJson);
}
}

int main(int argc, const char** argv)
{
    NES::setupSignalHandlers();
    NES::Thread::initializeThread(NES::Host("bench-runner"), "main");

    try
    {
        auto args = parseArguments(argc, argv);
        NES::SystestConfiguration& config = args.systestConfig;

        std::filesystem::remove_all(config.workingDir.getValue());
        std::filesystem::create_directory(config.workingDir.getValue());

        /// Bind the queries through the systest binder -- this yields fully bound plans (InMemory
        /// source, SIMDCSV formatter, fnattr, parser overrides) straight from the .test files.
        auto discovered = NES::Systest::loadTestFileMap(config);
        NES::Systest::SystestBinder binder{
            config.workingDir.getValue(),
            config.testDataDir.getValue(),
            config.configDir.getValue(),
            config.queryOptimizerConfig.value_or(NES::QueryOptimizerConfiguration{}),
            config.clusterConfig};
        auto [boundQueries, loadedFiles] = binder.loadOptimizeQueries(discovered);
        auto queries = selectRunnableQueries(std::move(boundQueries));
        if (queries.empty())
        {
            std::cout << "No runnable queries found.\n";
            return 1;
        }

        enable_memcom();

        /// Input accounting up front -- a 5 GB file is scanned at most once, never inside timing.
        std::unordered_map<std::string, FileStat> fileStatCache;
        std::vector<FileStat> perQueryStats;
        perQueryStats.reserve(queries.size());
        uint64_t distinctInputBytes = 0;
        for (const auto& query : queries)
        {
            perQueryStats.push_back(accountQuery(query, fileStatCache));
        }
        for (const auto& [path, stat] : fileStatCache)
        {
            distinctInputBytes += stat.bytes;
        }
        const auto estimatedPeakRamBytes = distinctInputBytes * args.replication;
        const auto physBytes = static_cast<uint64_t>(::sysconf(_SC_PHYS_PAGES)) * static_cast<uint64_t>(::sysconf(_SC_PAGE_SIZE));
        std::cout << fmt::format(
            "Selected {} quer{} x replication {} = {} concurrent.\n"
            "Estimated peak in-memory source footprint (InMemory sources): {:.2f} GB of {:.2f} GB physical RAM.\n",
            queries.size(),
            queries.size() == 1 ? "y" : "ies",
            args.replication,
            queries.size() * args.replication,
            static_cast<double>(estimatedPeakRamBytes) / 1e9,
            static_cast<double>(physBytes) / 1e9);
        if (physBytes > 0 and estimatedPeakRamBytes > physBytes / 2)
        {
            std::cout << "WARNING: estimated source footprint exceeds half of physical RAM. Use file slices per replica "
                         "(scripts/benchmarking/slice_csv.py) to bound it.\n";
        }

        /// Verify runs on its own short-lived worker so its started query ids never leak into the
        /// benchmark submitter's tracking set (QuerySubmitter::startQuery records every id, and
        /// waitForQueryTermination does not clear it).
        if (args.verify)
        {
            std::cout << "\n== verify pass (sequential, untimed) ==\n";
            auto verifyCatalog = std::make_shared<NES::WorkerCatalog>(config.clusterConfig.workers);
            NES::Systest::QuerySubmitter verifySubmitter(
                std::make_unique<NES::QueryManager>(std::move(verifyCatalog), NES::createEmbeddedBackend(args.workerConfig)));
            if (not runVerify(queries, verifySubmitter))
            {
                std::cout << "Verification failed; aborting before benchmark.\n";
                return 1;
            }
        }

        auto catalog = std::make_shared<NES::WorkerCatalog>(config.clusterConfig.workers);
        NES::Systest::QuerySubmitter submitter(
            std::make_unique<NES::QueryManager>(std::move(catalog), NES::createEmbeddedBackend(args.workerConfig)));

        nlohmann::json resultJson;
        resultJson["batches"] = nlohmann::json::array();
        for (uint64_t repeat = 0; repeat < args.repeat; ++repeat)
        {
            std::cout << fmt::format("\n== batch {}/{} ==", repeat + 1, args.repeat) << '\n';
            nlohmann::json batchJson;
            batchJson["queries"] = nlohmann::json::array();
            runBatch(queries, perQueryStats, submitter, args, args.pauseBeforeStart and repeat == args.pauseBatch, batchJson);
            resultJson["batches"].push_back(batchJson);
        }

        const auto jsonPath = args.jsonOutput.empty()
            ? (std::filesystem::path(config.workingDir.getValue()) / "BenchRunnerResults.json").string()
            : args.jsonOutput;
        std::ofstream jsonFile(jsonPath);
        jsonFile << resultJson.dump(4);
        std::cout << "\nResults written to " << jsonPath << '\n';
        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Fatal: " << e.what() << '\n';
        return 1;
    }
}

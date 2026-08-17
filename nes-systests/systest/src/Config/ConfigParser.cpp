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

#include <Config/ConfigParser.hpp>

#include <cstddef>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <argparse/argparse.hpp>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>

#include <Config/Config.hpp>
#include <Configurations/Util.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeYaml.hpp> ///NOLINT(misc-include-cleaner)
#include <Runner/Topology.hpp>
#include <Util/Logger/Logger.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace
{
using argparse::ArgumentParser;

void parseArgumentsOrExit(ArgumentParser& program, const int argc, const char** argv) /// NOLINT(readability-function-cognitive-complexity)
{
    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::runtime_error& err)
    {
        std::cerr << "Error parsing arguments: " << err.what() << '\n';
        std::cerr << program << '\n';
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }
    catch (const std::exception& err)
    {
        std::cerr << "Unexpected error during argument parsing: " << err.what() << '\n';
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }
}

void configureArgumentParser(ArgumentParser& program)
{
    const auto defaultDisableConfigPath = std::string{TEST_CONFIGURATION_DIR} + "/systest-disable.yaml";

    program.add_argument("-t", "--testLocations")
        .help("directly specified test files, directories, or multiple locations to discover test files in. "
              "If a directory is given, all .test files are discovered recursively. "
              "Use 'path/to/testfile:testnumber' to run a specific test by testnumber within a file. Default: " TEST_DISCOVER_DIR)
        .nargs(argparse::nargs_pattern::any);
    program.add_argument("-g", "--groups").help("run specific test groups").nargs(argparse::nargs_pattern::at_least_one);
    program.add_argument("-e", "--exclude-groups")
        .help("ignore groups, takes precedence over -g")
        .nargs(argparse::nargs_pattern::at_least_one);
    program.add_argument("--disableConfigFile")
        .default_value(defaultDisableConfigPath)
        .help("path to the default systest disable config file");
    program.add_argument("--ignoreDisableConfigFile").help("ignore the disable config file").flag();

    program.add_argument("-l", "--list").flag().help("list all discovered tests and test groups");
    program.add_argument("--log-path").help("set the logging path");
    program.add_argument("-d", "--debug").flag().help("dump the query plan and enable debug logging");
    program.add_argument("--data").help("path to the directory where input CSV files are stored");
    program.add_argument("-w", "--workerConfig").help("load worker config file (.yaml)");
    program.add_argument("-q", "--queryCompilerConfig").help("load query compiler config file (.yaml)");
    program.add_argument("--workingDir")
        .help("change the working directory. This directory contains source and result files. Default: " PATH_TO_BINARY_DIR
              "/nes-systests/");
    program.add_argument("-r", "--remote").flag().help("use the remote grpc backend");
    program.add_argument("-c", "--clusterConfig").nargs(1).help("path to the cluster topology file");
    program.add_argument("--shuffle").flag().help("run the test files in random order");
    program.add_argument("--shuffle-seed")
        .help("the seed the test file order is shuffled with, so a shuffled run repeats that order. Implies --shuffle")
        .scan<'i', int>();
    program.add_argument("-n", "--numberConcurrentQueries")
        .help("number of concurrent queries. Default: 6")
        .default_value(6)
        .scan<'i', int>();
    program.add_argument("--sequential").flag().help("force sequential query execution. Equivalent to `-n 1`");
    program.add_argument("--query-timeout")
        .help("fail a query that has not reached a terminal state after this many seconds. 0 waits forever. Default: 300")
        .scan<'i', int>();
    program.add_argument("--endless").flag().help("keep issuing the discovered queries to the worker");
    program.add_argument("--benchmark-rounds")
        .help("how many times -b repeats the queries, keeping each query's best time. Default: 1")
        .scan<'i', int>();
    program.add_argument("--endless-rounds").help("how many times --endless repeats the queries. Unlimited by default").scan<'i', int>();
    program.add_argument("--endless-seconds").help("how long --endless keeps issuing queries. Unlimited by default").scan<'i', int>();
    program.add_argument("--optimizer")
        .default_value<std::vector<std::string>>({})
        .append()
        .help("changes optimizer default values. e.g. join_strategy=HASH_JOIN");
    program.add_argument("--")
        .help("arguments passed to the worker config, e.g., `-- --worker.query_engine.number_of_worker_threads=10`")
        .default_value(std::vector<std::string>{})
        .remaining();
    program.add_argument("-b")
        .help("Benchmark (time) all specified queries and store results into 'BenchmarkResults.json' in the result directory")
        .default_value(false)
        .implicit_value(true);
}

void loadDisableConfig(const ArgumentParser& program, NES::Config& config)
{
    if (program.is_used("--ignoreDisableConfigFile"))
    {
        return;
    }

    const auto disableConfigFilePath = program.get<std::string>("--disableConfigFile");
    if (not std::filesystem::is_regular_file(disableConfigFilePath))
    {
        if (program.is_used("--disableConfigFile"))
        {
            std::cerr << "Configured systest disable config file does not exist: " << disableConfigFilePath << '\n';
            std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
        }
        return;
    }

    try
    {
        config.overwriteConfigWithYAMLFileInput(disableConfigFilePath);
        config.globalExcludedGroups = config.excludeGroups.getValues()
            | std::views::transform([](const auto& value) { return value.getValue(); }) | std::ranges::to<std::vector<std::string>>();
        config.excludeGroups.clear();
    }
    catch (const std::exception& err)
    {
        std::cerr << "Failed to read systest disable config file '" << disableConfigFilePath << "': " << err.what() << '\n';
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }
}

void applyBenchmarkMode(const ArgumentParser& program, NES::Config& config)
{
    if (not program.is_used("-b"))
    {
        return;
    }

    config.benchmark = true;
    if ((program.is_used("-n") || program.is_used("--numberConcurrentQueries")) && program.get<int>("--numberConcurrentQueries") > 1)
    {
        NES_ERROR("Cannot run systest in Benchmarking mode with concurrency enabled!");
        std::cout << "Cannot run systest in benchmarking mode with concurrency enabled!\n";
        std::exit(-1); ///NOLINT(concurrency-mt-unsafe)
    }

    std::cout << "Running systests in benchmarking mode. Only one query is run at a time!\n";
    std::cout << "Any included differential queries and queries expecting an error will be skipped.\n";
    config.numberConcurrentQueries = 1;
}

/// Records the choice only, because the logger takes this run's file once the whole configuration is known.
void applyDebugMode(const ArgumentParser& program, NES::Config& config)
{
    if (program.is_used("-d"))
    {
        config.debugLogging = true;
    }
}

void applyInputLocations(const ArgumentParser& program, NES::Config& config)
{
    if (program.is_used("--data"))
    {
        config.testDataDir = program.get<std::string>("--data");
    }

    if (program.is_used("--log-path"))
    {
        config.logFilePath = program.get<std::string>("--log-path");
    }

    if (program.is_used("--workingDir"))
    {
        config.workingDir = program.get<std::string>("--workingDir");
    }
}

void addTestQueryNumbers(NES::Config& config, const std::string& testNumberStr)
{
    std::stringstream ss(testNumberStr);
    std::string item;
    while (std::getline(ss, item, ','))
    {
        if (const size_t dashPos = item.find('-'); dashPos != std::string::npos)
        {
            const int start = std::stoi(item.substr(0, dashPos));
            const int end = std::stoi(item.substr(dashPos + 1));
            for (int i = start; i <= end; ++i)
            {
                config.testQueryNumbers.add(i);
            }
            continue;
        }

        config.testQueryNumbers.add(std::stoi(item));
    }
}

std::filesystem::path parseTestLocationPath(const std::string& testFileDefinition, NES::Config& config)
{
    const size_t delimiterPos = testFileDefinition.find(':');
    if (delimiterPos == std::string::npos)
    {
        return {testFileDefinition};
    }

    addTestQueryNumbers(config, testFileDefinition.substr(delimiterPos + 1));
    return {testFileDefinition.substr(0, delimiterPos)};
}

std::vector<std::filesystem::path> findAllInTree(const std::filesystem::path& wanted, const std::filesystem::path& root)
{
    std::vector<std::filesystem::path> hits;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(root, std::filesystem::directory_options::skip_permission_denied))
    {
        if (entry.is_regular_file() && entry.path().filename() == wanted)
        {
            hits.emplace_back(entry.path());
        }
    }
    return hits;
}

void applyDiscoveredTestLocation(const std::filesystem::path& testFilePath, const std::filesystem::path& discoverRoot, NES::Config& config)
{
    /// A bare file name is looked up under the discovery root, so a test can be named without its folder.
    const auto allMatches = findAllInTree(testFilePath.filename(), discoverRoot);
    if (allMatches.empty())
    {
        std::cerr << '\'' << testFilePath << "' could not be located in any test discover directory.\n";
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }

    if (allMatches.size() == 1)
    {
        config.directlySpecifiedTestFiles = allMatches.front();
        return;
    }

    std::cerr << "Ambiguous test name '" << testFilePath << "':\n";
    for (const auto& path : allMatches)
    {
        std::cerr << "  • " << path << '\n';
    }
    std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
}

void applyTestLocations(const ArgumentParser& program, NES::Config& config)
{
    if (not program.is_used("--testLocations"))
    {
        return;
    }

    const std::filesystem::path discoverRoot{config.testsDiscoverDir.getValue()};

    for (const auto testLocations = program.get<std::vector<std::string>>("--testLocations"); const auto& location : testLocations)
    {
        const auto testFilePath = parseTestLocationPath(location, config);
        if (std::filesystem::is_directory(testFilePath))
        {
            /// A directory narrows the search below the root rather than moving it, so a file keeps the key it would
            /// have had from the root however the run named it. Several directories narrow to the union of them.
            config.searchDirs.emplace_back(testFilePath);
            continue;
        }

        if (std::filesystem::is_regular_file(testFilePath))
        {
            config.directlySpecifiedTestFiles = testFilePath;
            continue;
        }

        applyDiscoveredTestLocation(testFilePath, discoverRoot, config);
    }
}

void addSequenceOptionValues(const ArgumentParser& program, const std::string& argumentName, decltype(NES::Config::testGroups)& option)
{
    if (not program.is_used(argumentName))
    {
        return;
    }

    for (const auto& value : program.get<std::vector<std::string>>(argumentName))
    {
        option.add(value);
    }
}

void applyGroupSelection(const ArgumentParser& program, NES::Config& config)
{
    addSequenceOptionValues(program, "-g", config.testGroups);
    addSequenceOptionValues(program, "--exclude-groups", config.excludeGroups);
}

/// Flattens a nested YAML mapping into the dotted keys a worker matches its configuration options by.
void flattenInto(const YAML::Node& node, const std::string& prefix, std::unordered_map<std::string, std::string>& flattened)
{
    for (const auto& entry : node)
    {
        const auto key = prefix.empty() ? entry.first.as<std::string>() : prefix + "." + entry.first.as<std::string>();
        if (entry.second.IsMap())
        {
            flattenInto(entry.second, key, flattened);
            continue;
        }
        flattened.insert_or_assign(key, entry.second.as<std::string>());
    }
}

/// Reads the `workers:` block of a topology file, which the runner registers one `CREATE WORKER` for.
std::vector<NES::TopologyWorker> readTopologyWorkers(const YAML::Node& workers)
{
    if (not workers.IsSequence())
    {
        return {};
    }

    std::vector<NES::TopologyWorker> topology;
    topology.reserve(workers.size());
    for (const auto& worker : workers)
    {
        NES::TopologyWorker declared{
            .host = NES::Host{worker["host"].as<std::string>()},
            .dataAddress = {},
            .maxOperators = std::nullopt,
            .downstream = {},
            .config = {}};
        if (worker["data_address"])
        {
            declared.dataAddress = worker["data_address"].as<std::string>();
        }
        if (worker["max_operators"])
        {
            declared.maxOperators = worker["max_operators"].as<uint64_t>();
        }
        if (worker["downstream"])
        {
            declared.downstream = worker["downstream"].as<std::vector<NES::Host>>();
        }
        if (worker["config"])
        {
            flattenInto(worker["config"], {}, declared.config);
        }
        topology.push_back(std::move(declared));
    }
    return topology;
}

void applyExecutionOptions(const ArgumentParser& program, NES::Config& config)
{
    if (program.is_used("--shuffle-seed"))
    {
        config.shuffleSeed = program.get<int>("--shuffle-seed");
        config.randomQueryOrder = true;
    }

    if (program.is_used("--shuffle"))
    {
        config.randomQueryOrder = true;
    }

    config.remoteWorker = program.get<bool>("--remote");

    /// Read only when the invocation gives a topology.
    /// The option carries a default path, and loading it regardless would place every run on a topology nobody asked for.
    if (program.is_used("--clusterConfig"))
    {
        try
        {
            config.clusterConfigPath = program.get<std::string>("--clusterConfig");
            auto clusterConfigYAML = YAML::LoadFile(config.clusterConfigPath.getValue());
            NES::ClusterConfiguration clusterConfig;
            clusterConfig.allowSinkPlacement = clusterConfigYAML["allow_sink_placement"].as<std::vector<NES::Host>>();
            clusterConfig.allowSourcePlacement = clusterConfigYAML["allow_source_placement"].as<std::vector<NES::Host>>();
            clusterConfig.workers = readTopologyWorkers(clusterConfigYAML["workers"]);
            config.clusterConfig = clusterConfig;
        }
        catch (std::exception& e)
        {
            std::cerr << "Error loading cluster config: " << e.what() << '\n';
            std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
        }
    }

    if (program.is_used("-n"))
    {
        config.numberConcurrentQueries = program.get<int>("-n");
    }

    if (program.is_used("--sequential"))
    {
        config.numberConcurrentQueries = 1;
    }

    if (program.is_used("--query-timeout"))
    {
        config.queryTimeoutSeconds = program.get<int>("--query-timeout");
    }

    if (program.is_used("--endless"))
    {
        config.endlessMode = true;
    }

    if (program.is_used("--benchmark-rounds"))
    {
        config.benchmarkRounds = program.get<int>("--benchmark-rounds");
    }

    if (program.is_used("--endless-rounds"))
    {
        config.endlessRounds = program.get<int>("--endless-rounds");
        config.endlessMode = true;
    }

    if (program.is_used("--endless-seconds"))
    {
        config.endlessSeconds = program.get<int>("--endless-seconds");
        config.endlessMode = true;
    }
}

void setValidatedConfigFile(const ArgumentParser& program, const std::string& argumentName, decltype(NES::Config::workerConfig)& option)
{
    if (not program.is_used(argumentName))
    {
        return;
    }

    option = program.get<std::string>(argumentName);
    if (not std::filesystem::is_regular_file(option.getValue()))
    {
        std::cerr << option.getValue() << " is not a file.\n";
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }
}

void applyConfigurationFiles(const ArgumentParser& program, NES::Config& config)
{
    setValidatedConfigFile(program, "-w", config.workerConfig);
    setValidatedConfigFile(program, "-q", config.queryCompilerConfig);
}

void applyOptimizerConfiguration(const ArgumentParser& program, NES::Config& config)
{
    if (not program.is_used("--optimizer"))
    {
        return;
    }

    std::unordered_map<std::string, std::string> optimizerRawConfig;
    for (const auto& optimizerConfigString : program.get<std::vector<std::string>>("--optimizer"))
    {
        if (auto pos = optimizerConfigString.find('='); pos != std::string::npos)
        {
            optimizerRawConfig[optimizerConfigString.substr(0, pos)] = optimizerConfigString.substr(pos + 1);
            continue;
        }

        std::cerr << "Invalid --optimizer argument. Requires argument like 'CONFIG=VALUE' but got '" << optimizerConfigString << "'\n";
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }

    NES::QueryOptimizerConfiguration queryOptimizerConfig;
    queryOptimizerConfig.overwriteConfigWithCommandLineInput(optimizerRawConfig);
    config.queryOptimizerConfig = queryOptimizerConfig;
    /// The parse above rejects a key that matches no option.
    /// This keeps the raw form too, because the coordinator reads these back as keys and values.
    config.optimizerOverrides = std::move(optimizerRawConfig);
}

void applySingleNodeWorkerConfiguration(const ArgumentParser& program, NES::Config& config)
{
    if (not program.is_used("--"))
    {
        return;
    }

    auto confVec = program.get<std::vector<std::string>>("--");
    const int workerArgc = static_cast<int>(confVec.size()) + 1;
    std::vector<const char*> workerArgv;
    workerArgv.reserve(workerArgc + 1);
    workerArgv.push_back("systest");
    for (auto& arg : confVec)
    {
        workerArgv.push_back(arg.c_str());
    }

    config.singleNodeWorkerConfig = NES::loadConfiguration<NES::SingleNodeWorkerConfiguration>(workerArgc, workerArgv.data());

    /// The load above rejects a key that matches no option.
    /// This keeps the raw form too, because the runner declares it on the worker that the coordinator then starts.
    for (const auto& argument : confVec)
    {
        const auto separator = argument.find('=');
        if (separator == std::string::npos)
        {
            std::cerr << "Invalid worker argument. Requires argument like '--worker.x=VALUE' but got '" << argument << "'\n";
            std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
        }
        auto key = argument.substr(0, separator);
        if (key.starts_with("--"))
        {
            key = key.substr(2);
        }
        config.workerOverrides.insert_or_assign(std::move(key), argument.substr(separator + 1));
    }
}

void handleMetaCommands(const ArgumentParser& program, const NES::Config& config)
{
    if (program.is_used("--list"))
    {
        std::cout << NES::discoverTestFiles(config);
        std::exit(0); ///NOLINT(concurrency-mt-unsafe)
    }

    if (program.is_used("--help"))
    {
        std::cout << program << '\n';
        std::exit(0); ///NOLINT(concurrency-mt-unsafe)
    }
}
}

namespace NES
{

Config parseConfig(const int argc, const char** argv)
{
    ArgumentParser program("systest");
    configureArgumentParser(program);
    parseArgumentsOrExit(program, argc, argv);

    auto config = Config();
    loadDisableConfig(program, config);
    applyBenchmarkMode(program, config);
    applyDebugMode(program, config);
    applyInputLocations(program, config);
    applyTestLocations(program, config);
    applyGroupSelection(program, config);
    applyExecutionOptions(program, config);
    applyConfigurationFiles(program, config);
    applyOptimizerConfiguration(program, config);
    applySingleNodeWorkerConfiguration(program, config);
    handleMetaCommands(program, config);
    return config;
}

}

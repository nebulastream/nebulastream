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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <initializer_list>
#include <iostream>
#include <optional>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <argparse/argparse.hpp>
#include <fmt/format.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>

#include <Config/Config.hpp>
#include <Config/Topology.hpp>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Configurations/ConfigResolution.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeYaml.hpp> ///NOLINT(misc-include-cleaner)
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

/// Readers for the topology file, so the file is read with `as<NES::ClusterConfiguration>()`.
/// They throw instead of returning false, so the error message contains the key. The bare conversion error does not.
/// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) a missing key gives an undefined node
namespace YAML
{
namespace
{
void requireMapWithKeys(const Node& node, const std::string_view what, const std::initializer_list<std::string_view> knownKeys)
{
    if (not node.IsMap())
    {
        throw std::runtime_error(fmt::format("{} must be a map", what));
    }
    for (const auto& entry : node)
    {
        if (const auto key = entry.first.as<std::string>(); std::ranges::find(knownKeys, key) == knownKeys.end())
        {
            throw std::runtime_error(fmt::format("{} has the unknown key '{}', known keys: {}", what, key, fmt::join(knownKeys, ", ")));
        }
    }
}

template <typename T>
T readRequired(const Node& node, const std::string& key, const std::string_view what)
{
    if (not node[key].IsDefined() or node[key].IsNull())
    {
        throw std::runtime_error(fmt::format("{} must set '{}'", what, key));
    }
    return node[key].as<T>();
}

template <typename T>
std::optional<T> readOptional(const Node& node, const std::string& key)
{
    return node[key].IsDefined() and not node[key].IsNull() ? std::optional{node[key].as<T>()} : std::nullopt;
}

void requireDeclared(const std::vector<NES::Host>& hosts, const std::vector<NES::TopologyWorker>& workers, const std::string_view what)
{
    for (const auto& host : hosts)
    {
        if (not std::ranges::contains(workers, host, &NES::TopologyWorker::host))
        {
            throw std::runtime_error(fmt::format("{} contains '{}', which is not one of the workers", what, host));
        }
    }
}

NES::TopologyWorker readWorker(const Node& node)
{
    requireMapWithKeys(node, "a worker", {"host", "data_address", "max_operators", "downstream", "config"});
    return NES::TopologyWorker{
        .host = readRequired<NES::Host>(node, "host", "a worker"),
        .dataAddress = readRequired<std::string>(node, "data_address", "a worker"),
        .maxOperators = readOptional<uint64_t>(node, "max_operators"),
        .downstream = readOptional<std::vector<NES::Host>>(node, "downstream").value_or(std::vector<NES::Host>{}),
        .config = node["config"].IsDefined() and not node["config"].IsNull() ? NES::flattenYAMLConfig(node["config"])
                                                                             : NES::Schema<NES::LiteralConfigValue, NES::Ordered>{}};
}
}

template <>
struct convert<NES::ClusterConfiguration>
{
    static bool decode(const Node& node, NES::ClusterConfiguration& cluster)
    {
        requireMapWithKeys(node, "a topology", {"workers", "allow_source_placement", "allow_sink_placement"});
        if (not node["workers"].IsSequence())
        {
            throw std::runtime_error("a topology must set 'workers' to a list");
        }
        for (const auto& worker : node["workers"])
        {
            cluster.workers.push_back(readWorker(worker));
        }
        cluster.allowSourcePlacement = readRequired<std::vector<NES::Host>>(node, "allow_source_placement", "a topology");
        cluster.allowSinkPlacement = readRequired<std::vector<NES::Host>>(node, "allow_sink_placement", "a topology");
        /// The first entry of each list is the default host of a source or sink that sets none, so an empty list has no default.
        if (cluster.workers.empty() or cluster.allowSourcePlacement.empty() or cluster.allowSinkPlacement.empty())
        {
            throw std::runtime_error("workers, allow_source_placement and allow_sink_placement must each list at least one worker");
        }
        requireDeclared(cluster.allowSourcePlacement, cluster.workers, "allow_source_placement");
        requireDeclared(cluster.allowSinkPlacement, cluster.workers, "allow_sink_placement");
        for (const auto& worker : cluster.workers)
        {
            requireDeclared(worker.downstream, cluster.workers, fmt::format("the downstream list of '{}'", worker.host));
        }
        return true;
    }
};
}

/// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)

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
        .help(
            "directly specified test files, directories, or multiple locations to discover test files in. "
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
        .help(
            "change the working directory. This directory contains source and result files. Default: " PATH_TO_BINARY_DIR "/nes-systests/");
    program.add_argument("-r", "--remote").flag().help("use the remote grpc backend");
    program.add_argument("-c", "--clusterConfig").nargs(1).help("path to the cluster topology file");
    program.add_argument("--shuffle").flag().help("run queries in random order");
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
    program.add_argument("--endless").flag().help("continuously issue queries to the worker");
    program.add_argument("--endless-rounds")
        .help("how many times --endless repeats the queries. Implies --endless. Unlimited by default")
        .scan<'i', int>();
    program.add_argument("--endless-seconds")
        .help("how long --endless keeps issuing queries, in seconds. Implies --endless. Unlimited by default")
        .scan<'i', int>();
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
    program.add_argument("--benchmark-rounds")
        .help("how many times -b repeats the queries, keeping each query's best time. Default: 1")
        .scan<'i', int>();
}

void loadDisableConfig(const ArgumentParser& program, NES::SystestConfiguration& config)
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
        const YAML::Node disableConfig = YAML::LoadFile(disableConfigFilePath);
        /// YAML key lookup returns an undefined node for missing optional properties.
        /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        config.excludeGroupsConfiguredInDisableConfig
            = disableConfig["exclude_groups"].IsDefined() && disableConfig["exclude_groups"].IsSequence();
        if (config.excludeGroupsConfiguredInDisableConfig)
        {
            config.globalExcludedGroups = disableConfig["exclude_groups"].as<std::vector<std::string>>();
        }
        if (disableConfig["disabled_test_files"].IsDefined() && disableConfig["disabled_test_files"].IsSequence())
        {
            config.disabledTestFiles = disableConfig["disabled_test_files"].as<std::vector<std::string>>();
        }
        /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    }
    catch (const std::exception& err)
    {
        std::cerr << "Failed to read systest disable config file '" << disableConfigFilePath << "': " << err.what() << '\n';
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }
}

void applyBenchmarkMode(const ArgumentParser& program, NES::SystestConfiguration& config)
{
    if (not program.is_used("-b"))
    {
        return;
    }

    config.benchmark = true;
    if (program.is_used("--benchmark-rounds"))
    {
        config.benchmarkRounds = program.get<int>("--benchmark-rounds");
    }

    /// A measuring run is limited to one query at a time, so -b and -n above 1 contradict each other.
    if ((program.is_used("-n") || program.is_used("--numberConcurrentQueries")) && program.get<int>("--numberConcurrentQueries") > 1)
    {
        const auto message = fmt::format(
            "Cannot combine -b with -n {}: a measuring run submits one query at a time. Drop one of the two.",
            program.get<int>("--numberConcurrentQueries"));
        NES_ERROR("{}", message);
        std::cout << message << '\n';
        std::exit(-1); ///NOLINT(concurrency-mt-unsafe)
    }

    std::cout << "Running systests in benchmarking mode. Only one query is run at a time!\n";
    std::cout << "Any included differential queries and queries expecting an error will be skipped.\n";
}

void applyDebugMode(const ArgumentParser& program)
{
    if (program.is_used("-d"))
    {
        NES::Logger::setupLogging("systest.log", NES::LogLevel::LOG_DEBUG);
    }
}

void applyInputLocations(const ArgumentParser& program, NES::SystestConfiguration& config)
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

void addTestQueryNumbers(NES::SystestConfiguration& config, const std::string& testNumberStr)
{
    std::stringstream ss(testNumberStr);
    std::string item;
    while (std::getline(ss, item, ','))
    {
        const size_t dashPos = item.find('-');
        if (dashPos != std::string::npos)
        {
            const int start = std::stoi(item.substr(0, dashPos));
            const int end = std::stoi(item.substr(dashPos + 1));
            for (int i = start; i <= end; ++i)
            {
                config.testQueryNumbers.push_back(i);
            }
            continue;
        }

        config.testQueryNumbers.push_back(std::stoi(item));
    }
}

/// Splits `path` or `path:test_numbers` into the two.
std::pair<std::filesystem::path, std::string> splitTestLocation(const std::string& testFileDefinition)
{
    const size_t delimiterPos = testFileDefinition.find(':');
    if (delimiterPos == std::string::npos)
    {
        return {std::filesystem::path{testFileDefinition}, std::string{}};
    }

    return {std::filesystem::path{testFileDefinition.substr(0, delimiterPos)}, testFileDefinition.substr(delimiterPos + 1)};
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

void applyDiscoveredTestLocation(
    const std::filesystem::path& testFilePath, const std::vector<std::filesystem::path>& searchDirs, NES::SystestConfiguration& config)
{
    /// A bare file name is looked up in every directory that the run searches, so a test can be given without its folder.
    std::vector<std::filesystem::path> allMatches;
    for (const auto& searchDir : searchDirs)
    {
        auto matches = findAllInTree(testFilePath.filename(), searchDir);
        allMatches.insert(allMatches.end(), matches.begin(), matches.end());
    }
    std::ranges::sort(allMatches);
    const auto duplicates = std::ranges::unique(allMatches);
    allMatches.erase(duplicates.begin(), duplicates.end());

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

void applyTestLocations(const ArgumentParser& program, NES::SystestConfiguration& config)
{
    if (not program.is_used("--testLocations"))
    {
        return;
    }

    std::vector<std::filesystem::path> directories;
    std::vector<std::pair<std::filesystem::path, std::string>> files;
    for (const auto& location : program.get<std::vector<std::string>>("--testLocations"))
    {
        if (auto split = splitTestLocation(location); std::filesystem::is_directory(split.first))
        {
            directories.push_back(std::move(split.first));
        }
        else
        {
            files.push_back(std::move(split));
        }
    }

    /// A bare file name is looked up in the search directories, which are fixed before any lookup. Collecting them
    /// first keeps the result independent of argument order, which matters when a directory lies outside the root or
    /// resolves a name that is ambiguous under it.
    for (const auto& directory : directories)
    {
        config.testDiscoverDirs.push_back(directory.string());
    }

    auto searchDirs = config.testDiscoverDirs | std::views::transform([](const auto& option) { return std::filesystem::path{option}; })
        | std::ranges::to<std::vector<std::filesystem::path>>();
    if (searchDirs.empty())
    {
        searchDirs.emplace_back(config.testDiscoverRoot);
    }

    for (const auto& [testFilePath, testNumbers] : files)
    {
        if (not testNumbers.empty())
        {
            addTestQueryNumbers(config, testNumbers);
        }

        if (std::filesystem::is_regular_file(testFilePath))
        {
            config.directlySpecifiedTestFiles = testFilePath;
        }
        else
        {
            applyDiscoveredTestLocation(testFilePath, searchDirs, config);
        }
    }
}

void addSequenceOptionValues(
    const ArgumentParser& program, const std::string& argumentName, decltype(NES::SystestConfiguration::testGroups)& option)
{
    if (not program.is_used(argumentName))
    {
        return;
    }

    for (const auto& value : program.get<std::vector<std::string>>(argumentName))
    {
        option.push_back(value);
    }
}

void applyGroupSelection(const ArgumentParser& program, NES::SystestConfiguration& config)
{
    addSequenceOptionValues(program, "-g", config.testGroups);
    if (program.is_used("--exclude-groups"))
    {
        config.excludedGroupsProvidedOnCommandLine = true;
    }
    addSequenceOptionValues(program, "--exclude-groups", config.excludeGroups);
}

void applyExecutionOptions(const ArgumentParser& program, NES::SystestConfiguration& config)
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

    /// Without --clusterConfig the default topology is loaded, so every run has one.
    try
    {
        if (program.is_used("--clusterConfig"))
        {
            config.clusterConfigPath = program.get<std::string>("--clusterConfig");
        }
        config.clusterConfig = YAML::LoadFile(config.clusterConfigPath).as<NES::ClusterConfiguration>();
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error loading cluster config '" << config.clusterConfigPath << "': " << e.what() << '\n';
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
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

void setValidatedConfigFile(const ArgumentParser& program, const std::string& argumentName, std::string& configFilePath)
{
    if (not program.is_used(argumentName))
    {
        return;
    }

    configFilePath = program.get<std::string>(argumentName);
    if (not std::filesystem::is_regular_file(configFilePath))
    {
        std::cerr << configFilePath << " is not a file.\n";
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }
}

void applyConfigurationFiles(const ArgumentParser& program, NES::SystestConfiguration& config)
{
    setValidatedConfigFile(program, "-q", config.queryCompilerConfig);
}

void applyOptimizerConfiguration(const ArgumentParser& program, NES::SystestConfiguration& config)
{
    if (not program.is_used("--optimizer"))
    {
        return;
    }
    std::vector<std::string> arguments;
    for (const auto& value : program.get<std::vector<std::string>>("--optimizer"))
    {
        arguments.push_back("--optimizer." + value);
    }
    auto merged = NES::mergeConfigLayers(
        {{.name = "optimizer arguments", .literals = NES::parseCommandLineConfig(arguments)},
         {.name = "command line", .literals = config.workerOptimizerConfigLiterals}});
    if (!merged.overwrites.empty())
    {
        throw NES::InvalidConfigParameter("Optimizer options must be specified only once");
    }
    config.workerOptimizerConfigLiterals = std::move(merged.literals);
}

void applySingleNodeWorkerConfiguration(const ArgumentParser& program, NES::SystestConfiguration& config)
{
    if (not program.is_used("--"))
    {
        return;
    }

    config.workerOptimizerConfigLiterals = NES::parseCommandLineConfig(program.get<std::vector<std::string>>("--"));
}

void applyWorkerConfigFile(const ArgumentParser& program, NES::SystestConfiguration& config)
{
    if (not program.is_used("-w"))
    {
        return;
    }
    try
    {
        auto merged = NES::mergeConfigLayers(
            {NES::ConfigLayer{
                 .name = "config file", .literals = NES::flattenYAMLConfig(std::filesystem::path{program.get<std::string>("-w")})},
             NES::ConfigLayer{.name = "command line", .literals = std::move(config.workerOptimizerConfigLiterals)}});
        if (!merged.overwrites.empty())
        {
            throw NES::InvalidConfigParameter(
                "The config file and the command line options must not both set the same option, but both set: {}",
                fmt::join(
                    merged.overwrites
                        | std::views::transform([](const auto& overwrite) { return NES::toLowerCase(fmt::format("{}", overwrite.name)); }),
                    ", "));
        }
        config.workerOptimizerConfigLiterals = std::move(merged.literals);
    }
    catch (const NES::Exception& e)
    {
        std::cerr << e.what() << '\n';
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }
}

void validateWorkerOptimizerConfig(const NES::SystestConfiguration& config)
{
    const auto declaredConfig = NES::createConfigSchema(
        NES::SingleNodeWorkerConfiguration::getConfigSchema(), NES::QueryOptimizerConfiguration::getConfigSchema());
    if (const auto resolved = NES::toExpected(NES::resolveConfigFullyQualified(config.workerOptimizerConfigLiterals, declaredConfig));
        not resolved.has_value())
    {
        std::cerr << "Invalid worker/optimizer configuration arguments: " << fmt::format("{}", resolved.error()) << '\n';
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }
}

void handleMetaCommands(const ArgumentParser& program, const NES::SystestConfiguration& config)
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

SystestConfiguration parseConfig(const int argc, const char** argv)
{
    ArgumentParser program("systest");
    configureArgumentParser(program);
    parseArgumentsOrExit(program, argc, argv);

    auto config = SystestConfiguration();
    loadDisableConfig(program, config);
    applyBenchmarkMode(program, config);
    applyDebugMode(program);
    applyInputLocations(program, config);
    applyTestLocations(program, config);
    applyGroupSelection(program, config);
    applyExecutionOptions(program, config);
    applyConfigurationFiles(program, config);
    applySingleNodeWorkerConfiguration(program, config);
    applyOptimizerConfiguration(program, config);
    applyWorkerConfigFile(program, config);
    validateWorkerOptimizerConfig(config);
    handleMetaCommands(program, config);
    return config;
}

}

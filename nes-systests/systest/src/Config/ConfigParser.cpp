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
#include <Config/Config.hpp>
#include <Configurations/Util.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeYaml.hpp> ///NOLINT(misc-include-cleaner)
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <argparse/argparse.hpp>
#include <fmt/format.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <QueryOptimizerConfiguration.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerConfig.hpp>

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
    program.add_argument("--shuffle").flag().help("run queries in random order");
    program.add_argument("-n", "--numberConcurrentQueries")
        .help("number of concurrent queries. Default: 6")
        .default_value(6)
        .scan<'i', int>();
    program.add_argument("--sequential").flag().help("force sequential query execution. Equivalent to `-n 1`");
    program.add_argument("--endless").flag().help("continuously issue queries to the worker");
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
    program.add_argument("--show-query-performance").flag().help("print per-query performance timing in the console output");
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
        config.excludeGroupsConfiguredInDisableConfig
            = disableConfig["exclude_groups"].IsDefined() && disableConfig["exclude_groups"].IsSequence();
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

void applyBenchmarkMode(const ArgumentParser& program, NES::SystestConfiguration& config)
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
                config.testQueryNumbers.add(i);
            }
            continue;
        }

        config.testQueryNumbers.add(std::stoi(item));
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
        config.testDiscoverDirs.add(directory.string());
    }

    auto searchDirs = config.testDiscoverDirs.getValues()
        | std::views::transform([](const auto& option) { return std::filesystem::path{option.getValue()}; })
        | std::ranges::to<std::vector<std::filesystem::path>>();
    if (searchDirs.empty())
    {
        searchDirs.emplace_back(config.testDiscoverRoot.getValue());
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
        option.add(value);
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
    if (program.is_used("--shuffle"))
    {
        config.randomQueryOrder = true;
    }

    config.remoteWorker = program.get<bool>("--remote");

    try
    {
        if (program.is_used("--clusterConfig"))
        {
            config.clusterConfigPath = program.get<std::string>("--clusterConfig");
        }
        auto clusterConfigYAML = YAML::LoadFile(config.clusterConfigPath.getValue());
        NES::SystestClusterConfiguration clusterConfig;
        clusterConfig.allowSinkPlacement = clusterConfigYAML["allow_sink_placement"].as<std::vector<NES::Host>>();
        clusterConfig.allowSourcePlacement = clusterConfigYAML["allow_source_placement"].as<std::vector<NES::Host>>();
        for (const auto& worker : clusterConfigYAML["workers"])
        {
            NES::SingleNodeWorkerConfiguration config;
            /// Check if worker has config key
            if (worker["config"].IsDefined() && !worker["config"].IsNull())
            {
                config.overwriteConfigWithYAMLNode(worker["config"]);
            }

            clusterConfig.workers.push_back(NES::WorkerConfig{
                .host = worker["host"].as<NES::Host>(),
                .dataAddress = worker["data_address"].as<std::string>(),
                .maxOperators = worker["max_operators"].IsDefined()
                    ? NES::Capacity(NES::CapacityKind::Limited{worker["max_operators"].as<size_t>()})
                    : NES::Capacity(NES::CapacityKind::Unlimited{}),
                .downstream
                = worker["downstream"].IsDefined() ? worker["downstream"].as<std::vector<NES::Host>>() : std::vector<NES::Host>{},
                .config = config,
            });
        }
        config.clusterConfig = clusterConfig;
    }
    catch (std::exception& e)
    {
        std::cerr << "Error loading cluster config: " << e.what() << '\n';
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

    if (program.is_used("--show-query-performance"))
    {
        config.showQueryPerformance = true;
    }

    if (program.is_used("--endless"))
    {
        config.endlessMode = true;
    }
}

void setValidatedConfigFile(
    const ArgumentParser& program, const std::string& argumentName, decltype(NES::SystestConfiguration::workerConfig)& option)
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

void applyConfigurationFiles(const ArgumentParser& program, NES::SystestConfiguration& config)
{
    setValidatedConfigFile(program, "-w", config.workerConfig);
    setValidatedConfigFile(program, "-q", config.queryCompilerConfig);
}

void applyOptimizerConfiguration(const ArgumentParser& program, NES::SystestConfiguration& config)
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
}

void applySingleNodeWorkerConfiguration(const ArgumentParser& program, NES::SystestConfiguration& config)
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
    applyOptimizerConfiguration(program, config);
    applySingleNodeWorkerConfiguration(program, config);
    handleMetaCommands(program, config);
    return config;
}

}

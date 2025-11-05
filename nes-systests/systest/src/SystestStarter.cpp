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

#include <chrono>
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
#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeYaml.hpp> ///NOLINT(misc-include-cleaner)
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Signal.hpp>
#include <argparse/argparse.hpp>
#include <fmt/format.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestConfiguration.hpp>
#include <SystestExecutor.hpp>
#include <SystestState.hpp>
#include <Thread.hpp>
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

    program.add_argument("-t", "--testLocation")
        .help("directly specified test file, e.g., fliter.test or a directory to discover test files in.  Use "
              "'path/to/testfile:testnumber' to run a specific test by testnumber within a file. Default: " TEST_DISCOVER_DIR);
    program.add_argument("-g", "--groups").help("run a specific test groups").nargs(argparse::nargs_pattern::at_least_one);
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
        .help("arguments passed to the worker config, e.g., `-- --worker.queryEngine.numberOfWorkerThreads=10`")
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

std::filesystem::path parseTestLocationPath(const std::string& testFileDefinition, NES::SystestConfiguration& config)
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

std::vector<std::filesystem::path> resolveTestArg(const std::filesystem::path& arg, const std::filesystem::path& discoverRoot)
{
    if (std::filesystem::exists(arg))
    {
        return {std::filesystem::canonical(arg)};
    }
    return findAllInTree(arg.filename(), discoverRoot);
}

void applyDiscoveredTestLocation(
    const std::filesystem::path& testFilePath, const std::filesystem::path& discoverRoot, NES::SystestConfiguration& config)
{
    const auto matches = resolveTestArg(testFilePath, discoverRoot);
    if (matches.empty())
    {
        std::cerr << '\'' << testFilePath << "' could not be located under '" << discoverRoot << "'.\n";
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }

    if (matches.size() == 1)
    {
        config.directlySpecifiedTestFiles = matches.front();
        return;
    }

    std::cerr << "Ambiguous test name '" << testFilePath << "':\n";
    for (const auto& path : matches)
    {
        std::cerr << "  • " << path << '\n';
    }
    std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
}

void applyTestLocation(const ArgumentParser& program, NES::SystestConfiguration& config)
{
    if (not program.is_used("--testLocation"))
    {
        return;
    }

    const auto testFilePath = parseTestLocationPath(program.get<std::string>("--testLocation"), config);
    if (std::filesystem::is_directory(testFilePath))
    {
        config.testsDiscoverDir = testFilePath;
        return;
    }

    if (std::filesystem::is_regular_file(testFilePath))
    {
        config.directlySpecifiedTestFiles = testFilePath;
        return;
    }

    applyDiscoveredTestLocation(testFilePath, config.testsDiscoverDir.getValue(), config);
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
            clusterConfig.workers.push_back(NES::WorkerConfig{
                .host = worker["host"].as<NES::Host>(),
                .data = worker["data"].as<std::string>(),
                .maxOperators = worker["max_operators"].IsDefined()
                    ? NES::Capacity(NES::CapacityKind::Limited{worker["max_operators"].as<size_t>()})
                    : NES::Capacity(NES::CapacityKind::Unlimited{}),
                .downstream
                = worker["downstream"].IsDefined() ? worker["downstream"].as<std::vector<NES::Host>>() : std::vector<NES::Host>{},
                .config = {}});
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
        std::cout << NES::Systest::loadTestFileMap(config);
        std::exit(0); ///NOLINT(concurrency-mt-unsafe)
    }

    if (program.is_used("--help"))
    {
        std::cout << program << '\n';
        std::exit(0); ///NOLINT(concurrency-mt-unsafe)
    }
}

NES::SystestConfiguration parseConfiguration(int argc, const char** argv)
{
    ArgumentParser program("systest");
    configureArgumentParser(program);
    parseArgumentsOrExit(program, argc, argv);

    auto config = NES::SystestConfiguration();
    loadDisableConfig(program, config);
    applyBenchmarkMode(program, config);
    applyDebugMode(program);
    applyInputLocations(program, config);
    applyTestLocation(program, config);
    applyGroupSelection(program, config);
    applyExecutionOptions(program, config);
    applyConfigurationFiles(program, config);
    applyOptimizerConfiguration(program, config);
    applySingleNodeWorkerConfiguration(program, config);
    handleMetaCommands(program, config);
    return config;
}
}

int main(int argc, const char** argv)
{
    NES::setupSignalHandlers();
    const auto startTime = std::chrono::high_resolution_clock::now();
    NES::Thread::initializeThread(NES::Host("systest"), "main");

    auto config = parseConfiguration(argc, argv);
    NES::SystestExecutor executor(std::move(config));
    const auto result = executor.executeSystests();

    switch (result.returnType)
    {
        case SystestExecutorResult::ReturnType::SUCCESS: {
            const auto endTime = std::chrono::high_resolution_clock::now();
            const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
            fmt::print(
                "{}\nTotal execution time: {} ms ({:.3f} seconds)\n",
                result.outputMessage,
                duration.count(),
                std::chrono::duration_cast<std::chrono::duration<double>>(duration).count());
            return 0;
        }
        case SystestExecutorResult::ReturnType::FAILED: {
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
            PRECONDITION(result.errorCode, "Returning with as 'FAILED_WITH_EXCEPTION_CODE', but did not provide error code");
            NES_ERROR("{}", result.outputMessage);
            std::cout << result.outputMessage << '\n';
            std::cout << "Total execution time: " << duration.count() << " ms ("
                      << std::chrono::duration_cast<std::chrono::duration<double>>(duration).count() << " seconds)" << '\n';
            return result.errorCode.value();
        }
    }
}

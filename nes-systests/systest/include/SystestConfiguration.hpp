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

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

struct SystestClusterConfiguration
{
    std::vector<WorkerConfig> workers;
    std::vector<Host> allowSourcePlacement;
    std::vector<Host> allowSinkPlacement;
};

/// Note: for now we ignore/override the here specified default values with ones provided by argparse in `SystestExecutor::parseConfiguration()`
struct SystestConfiguration final
{
    /// Directory to lookup test files in.
    std::string testsDiscoverDir = TEST_DISCOVER_DIR;
    /// Directory to lookup test data files in.
    std::string testDataDir = SYSTEST_EXTERNAL_DATA_DIR;
    /// Directory to lookup configuration files.
    std::string configDir = TEST_CONFIGURATION_DIR;
    /// Path to the log file.
    std::string logFilePath;
    /// Directly specified test files. If directly specified no lookup at the test discovery dir will happen.
    std::string directlySpecifiedTestFiles;
    /// Directly specified query numbers within the directly specified test files.
    std::vector<uint64_t> testQueryNumbers;
    /// File extension to find test files for.
    std::string testFileExtension = ".test";
    /// Directory with source and result files.
    std::string workingDir = PATH_TO_BINARY_DIR "/nes-systests/working-dir";
    /// Run queries in random order.
    bool randomQueryOrder = false;
    /// Number of maximal concurrently running queries.
    uint64_t numberConcurrentQueries = 6;
    /// Records the execution time of each query.
    bool benchmark = false;
    /// Test groups to run.
    std::vector<std::string> testGroups;
    /// Test groups to exclude.
    std::vector<std::string> excludeGroups;
    /// Test files to disable.
    std::vector<std::string> disabledTestFiles;
    /// Used query compiler config file (.yaml).
    std::string queryCompilerConfig;
    /// Use remote worker.
    bool remoteWorker = false;
    /// Cluster configuration.
    std::string clusterConfigPath = TEST_CONFIGURATION_DIR "/topologies/two-node.yaml";
    /// Print per-query performance timing in the console output.
    bool showQueryPerformance = false;
    /// Continuously issue queries to the worker.
    bool endlessMode = false;

    bool excludeGroupsConfiguredInDisableConfig = false;
    bool excludedGroupsProvidedOnCommandLine = false;
    std::vector<std::string> globalExcludedGroups;

    SystestClusterConfiguration clusterConfig;
    /// Worker/optimizer config literals from the `-w` file and the command line after `--`
    /// (empty = nothing passed). They stay literals so per-test-file configuration overrides can
    /// be layered on top before resolving against SingleNodeWorkerConfiguration's declared schema.
    Schema<LiteralConfigValue, Ordered> workerOptimizerConfigLiterals;
    /// Explicitly resolved defaults: tests construct SystestConfiguration without going through
    /// the starter (which overwrites this from the `--` section), and the config structs are not
    /// default-initialized to their defaults.
    QueryOptimizerConfiguration queryOptimizerConfig = defaultConfiguration<QueryOptimizerConfiguration>();
};
}

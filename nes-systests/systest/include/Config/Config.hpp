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

#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/SequenceOption.hpp>
#include <Runner/Topology.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{

class Config final : public BaseConfiguration
{
public:
    Config() = default;

    /// The command line parser overrides most of the defaults below, so a default applies only when no argument sets that option.
    /// The root every test file is keyed against, and the default place to search for one.
    /// A test file key is its path relative to this, so moving the root renames every catalog entry the run makes.
    /// A directory given on the command line narrows the search below without moving this, so the same file keys the
    /// same however the run was invoked.
    StringOption testsDiscoverDir
        = {"tests_discover_dir", TEST_DISCOVER_DIR, "Directory to lookup test files in. Default: " TEST_DISCOVER_DIR};
    StringOption testDataDir
        = {"test_data_dir", SYSTEST_EXTERNAL_DATA_DIR, "Directory to lookup test data files in. Default: " SYSTEST_EXTERNAL_DATA_DIR};
    StringOption logFilePath = {"logFilePath", "Path to the log file"};
    BoolOption debugLogging = {"debug_logging", "false", "log at debug level rather than info"};
    StringOption directlySpecifiedTestFiles
        = {"directly_specified_test_files",
           "",
           "Directly specified test files. If directly specified no lookup at the test discovery dir will happen."};
    SequenceOption<UIntOption> testQueryNumbers
        = {"test_query_numbers", "Directly specified test files. If directly specified no lookup at the test discovery dir will happen."};
    StringOption testFileExtension = {"test_file_extension", ".test", "File extension to find test files for. Default: .test"};
    StringOption workingDir = {"working_dir", PATH_TO_BINARY_DIR "/nes-systests/working-dir", "Directory with source and result files"};
    BoolOption randomQueryOrder = {"random_query_order", "false", "run the test files in random order"};
    UIntOption shuffleSeed = {"shuffle_seed", "0", "the seed the test file order is shuffled with. 0 draws one and prints it"};
    UIntOption numberConcurrentQueries = {"number_concurrent_queries", "6", "number of maximal concurrently running queries"};
    UIntOption queryTimeoutSeconds
        = {"query_timeout_seconds", "300", "fail a query that has not reached a terminal state after this long. 0 waits forever"};
    /// Reports how long each query took and what it read, rather than only whether it passed.
    /// The result check keeps running underneath, because a fast wrong answer is not a measurement.
    BoolOption benchmark = {"benchmark_queries", "false", "report the execution time and throughput of each query"};
    UIntOption benchmarkRounds = {"benchmark_rounds", "1", "how many times a benchmark repeats the queries, keeping each query's best"};
    StringOption benchmarkReport
        = {"benchmark_report", "BenchmarkResults.json", "the file a benchmark writes its rows to, under the working directory"};
    SequenceOption<StringOption> testGroups = {"test_groups", "test groups to run"};
    SequenceOption<StringOption> excludeGroups = {"exclude_groups", "test groups to exclude"};
    SequenceOption<StringOption> disabledTestFiles = {"disabled_test_files", "test files to disable"};
    StringOption workerConfig = {"worker_config", "", "used worker config file (.yaml)"};
    StringOption queryCompilerConfig = {"query_compiler_config", "", "used query compiler config file (.yaml)"};
    BoolOption remoteWorker = {"remote_worker", "false", "use remote worker"};
    StringOption clusterConfigPath = {"cluster_config", TEST_CONFIGURATION_DIR "/topologies/two-node.yaml", "cluster configuration"};
    /// Runs the discovered queries over and over to keep a worker under load, rather than to assert anything.
    /// A run that never stops has no place in a test matrix, so one of the two limits below ends it.
    BoolOption endlessMode = {"endless_mode", "false", "keep issuing the discovered queries to the worker"};
    UIntOption endlessRounds = {"endless_rounds", "0", "how many times endless mode repeats the queries. 0 is unlimited"};
    UIntOption endlessSeconds = {"endless_seconds", "0", "how long endless mode keeps issuing queries. 0 is unlimited"};

    /// The groups the disable config file excludes, kept apart from the ones the command line excludes so a failure can
    /// say which of the two turned a test file off.
    std::vector<std::string> globalExcludedGroups;

    /// The subtrees of the discovery root a run searches, which the directories given on the command line set.
    /// Empty searches the whole root. Several directories narrow to the union of them, and each file still keys
    /// against the root rather than the directory it was found under.
    std::vector<std::filesystem::path> searchDirs;

    ClusterConfiguration clusterConfig;
    std::optional<SingleNodeWorkerConfiguration> singleNodeWorkerConfig;
    std::optional<QueryOptimizerConfiguration> queryOptimizerConfig;

    /// The optimizer and worker settings the command line gave, as keys and values.
    /// The parsed configurations above validate the same input and reject a key that matches no option.
    /// The runner submits these raw, because the coordinator and the worker apply them rather than this process.
    std::unordered_map<std::string, std::string> optimizerOverrides;
    std::unordered_map<std::string, std::string> workerOverrides;

protected:
    std::vector<BaseOption*> getOptions() override;
};
}

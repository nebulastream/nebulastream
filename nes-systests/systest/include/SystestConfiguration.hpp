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

#include <optional>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/SequenceOption.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{

class SystestConfiguration final : public BaseConfiguration
{
public:
    SystestConfiguration() = default;

    /// Note: for now we ignore/override the here specified default values with ones provided by argparse in `SystestExecutor::parseConfiguration()`
    StringOption testsDiscoverDir
        = {"tests_discover_dir", TEST_DISCOVER_DIR, "Directory to lookup test files in. Default: " TEST_DISCOVER_DIR};
    StringOption testDataDir
        = {"test_data_dir", SYSTEST_EXTERNAL_DATA_DIR, "Directory to lookup test data files in. Default: " SYSTEST_EXTERNAL_DATA_DIR};
    StringOption configDir
        = {"config_dir", TEST_CONFIGURATION_DIR, "Directory to lookup configuration files. Default: " TEST_CONFIGURATION_DIR};
    StringOption logFilePath = {"logFilePath", "Path to the log file"};
    StringOption directlySpecifiedTestFiles
        = {"directly_specified_test_files",
           "",
           "Directly specified test files. If directly specified no lookup at the test discovery dir will happen."};
    SequenceOption<UIntOption> testQueryNumbers
        = {"test_query_numbers", "Directly specified test files. If directly specified no lookup at the test discovery dir will happen."};
    StringOption testFileExtension = {"test_file_extension", ".test", "File extension to find test files for. Default: .test"};
    StringOption workingDir = {"working_dir", PATH_TO_BINARY_DIR "/nes-systests/working-dir", "Directory with source and result files"};
    BoolOption randomQueryOrder = {"random_query_order", "false", "run queries in random order"};
    UIntOption numberConcurrentQueries = {"number_concurrent_queries", "6", "number of maximal concurrently running queries"};
    BoolOption benchmark = {"benchmark_queries", "false", "Records the execution time of each query"};
    SequenceOption<StringOption> testGroups = {"test_groups", "test groups to run"};
    SequenceOption<StringOption> excludeGroups = {"exclude_groups", "test groups to exclude"};
    StringOption workerConfig = {"worker_config", "", "used worker config file (.yaml)"};
    StringOption queryCompilerConfig = {"query_compiler_config", "", "used query compiler config file (.yaml)"};
    StringOption grpcAddressUri
        = {"grpc",
           "",
           R"(The address to try to bind to the server in URI form. If
the scheme name is omitted, "dns:///" is assumed. To bind to any address,
please use IPv6 any, i.e., [::]:<port>, which also accepts IPv4
connections.  Valid values include dns:///localhost:1234,
192.168.1.1:31416, dns:///[::1]:27182, etc.)"};
    BoolOption endlessMode = {"query_compiler_config", "false", "continuously issue queries to the worker"};

    std::optional<SingleNodeWorkerConfiguration> singleNodeWorkerConfig;

protected:
    std::vector<BaseOption*> getOptions() override;
};
}

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

    /// Note: for now we ignore/override the here specified default values with ones provided by argparse in `readConfiguration()`
    StringOption testsDiscoverDir
        = {"testsDiscoverDir", TEST_DISCOVER_DIR, "Directory to lookup test files in. Default: " TEST_DISCOVER_DIR};
    StringOption testDataDir
        = {"testDataDir", SYSTEST_EXTERNAL_DATA_DIR, "Directory to lookup test data files in. Default: " SYSTEST_EXTERNAL_DATA_DIR};
    StringOption configDir
        = {"configDir", TEST_CONFIGURATION_DIR, "Directory to lookup configuration files. Default: " TEST_CONFIGURATION_DIR};
    StringOption logFilePath = {"logFilePath", "Path to the log file"};
    StringOption directlySpecifiedTestFiles
        = {"directlySpecifiedTestFiles",
           "",
           "Directly specified test files. If directly specified no lookup at the test discovery dir will happen."};
    SequenceOption<UIntOption> testQueryNumbers
        = {"testQueryNumbers", "Directly specified test files. If directly specified no lookup at the test discovery dir will happen."};
    StringOption testFileExtension = {"testFileExtension", ".test", "File extension to find test files for. Default: .test"};
    StringOption workingDir = {"workingDir", PATH_TO_BINARY_DIR "/nes-systests/working-dir", "Directory with source and result files"};
    BoolOption randomQueryOrder = {"randomQueryOrder", "false", "run queries in random order"};
    UIntOption numberConcurrentQueries = {"numberConcurrentQueries", "6", "number of maximal concurrently running queries"};
    BoolOption benchmark = {"Benchmark Queries", "false", "Records the execution time of each query"};
    SequenceOption<StringOption> testGroups = {"testGroups", "test groups to run"};
    SequenceOption<StringOption> excludeGroups = {"excludeGroups", "test groups to exclude"};
    StringOption workerConfig = {"workerConfig", "", "used worker config file (.yaml)"};
    StringOption queryCompilerConfig = {"queryCompilerConfig", "", "used query compiler config file (.yaml)"};
    StringOption topology = {"server", "", "run distributed, based on topology file (.yaml)"};
    BoolOption endlessMode = {"endlessMode", "false", "continuously issue queries to the worker"};

    std::optional<SingleNodeWorkerConfiguration> singleNodeWorkerConfig;

protected:
    std::vector<BaseOption*> getOptions() override;
};
}

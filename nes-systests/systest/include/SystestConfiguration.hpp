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

namespace NES::Configuration
{

class SystestConfiguration final : public Configurations::BaseConfiguration
{
public:
    SystestConfiguration() = default;

    /// Note: for now we ignore/override the here specified default values with ones provided by argparse in `readConfiguration()`
    Configurations::StringOption testsDiscoverDir
        = {"testsDiscoverDir", TEST_DISCOVER_DIR, "Directory to lookup test files in. Default: " TEST_DISCOVER_DIR};
    Configurations::StringOption testDataDir
        = {"testDataDir", TEST_DATA_DIR, "Directory to lookup test data files in. Default: " TEST_DATA_DIR};
    Configurations::StringOption configDir
        = {"configDir", TEST_CONFIGURATION_DIR, "Directory to lookup configuration files. Default: " TEST_CONFIGURATION_DIR};
    Configurations::StringOption directlySpecifiedTestFiles
        = {"directlySpecifiedTestFiles",
           "",
           "Directly specified test files. If directly specified no lookup at the test discovery dir will happen."};
    Configurations::SequenceOption<Configurations::UIntOption> testQueryNumbers
        = {"testQueryNumbers", "Directly specified test files. If directly specified no lookup at the test discovery dir will happen."};
    Configurations::StringOption testFileExtension
        = {"testFileExtension", ".test", "File extension to find test files for. Default: .test"};
    Configurations::StringOption workingDir
        = {"workingDir", PATH_TO_BINARY_DIR "/nes-systests/working-dir", "Directory with source and result files"};
    Configurations::BoolOption randomQueryOrder = {"randomQueryOrder", "false", "run queries in random order"};
    Configurations::UIntOption numberConcurrentQueries = {"numberConcurrentQueries", "6", "number of maximal concurrently running queries"};
    Configurations::BoolOption benchmark = {"Benchmark Queries", "false", "Records the execution time of each query"};
    Configurations::SequenceOption<Configurations::StringOption> testGroups = {"testGroups", "test groups to run"};
    Configurations::SequenceOption<Configurations::StringOption> excludeGroups = {"excludeGroups", "test groups to exclude"};
    Configurations::StringOption workerConfig = {"workerConfig", "", "used worker config file (.yaml)"};
    Configurations::StringOption queryCompilerConfig = {"queryCompilerConfig", "", "used query compiler config file (.yaml)"};
    Configurations::StringOption grpcAddressUri
        = {"grpc",
           "",
           R"(The address to try to bind to the server in URI form. If
the scheme name is omitted, "dns:///" is assumed. To bind to any address,
please use IPv6 any, i.e., [::]:<port>, which also accepts IPv4
connections.  Valid values include dns:///localhost:1234,
192.168.1.1:31416, dns:///[::1]:27182, etc.)"};

    std::optional<SingleNodeWorkerConfiguration> singleNodeWorkerConfig;

protected:
    std::vector<BaseOption*> getOptions() override;
};
}

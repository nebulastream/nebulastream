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

#include <string>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ScalarOption.hpp>

namespace NES::Configuration
{

class SystestConfiguration final : public Configurations::BaseConfiguration
{
public:
    Configurations::StringOption cacheDir
        = {"cacheDir", CACHE_DIR, "Directory to store protobuf (.pb) cache files into. Default: " CACHE_DIR};
    Configurations::StringOption testsDiscoverDir
        = {"testsDiscoverDir", TEST_DISCOVER_DIR, "Directory to lookup test files in. Default: " TEST_DISCOVER_DIR};
    Configurations::StringOption directlySpecifiedTestsFiles
        = {"directlySpecifiedTestsFiles",
           "",
           "Directly specified test files. If directly specified no lookup at the test discovery dir will happen."};
    Configurations::StringOption testFileExtension
        = {"testFileExtension", ".test", "File extension to find test files for. Default: .test"};
    Configurations::StringOption resultDir
    = {"resultDir", PATH_TO_BINARY_DIR "/tests/result/", "Directory for query results"};
    Configurations::BoolOption randomQueryOrder
    = {"randomQueryOrder", "false", "run queries in random order"};
    Configurations::BoolOption useCachedQueries = {"useCachedQueries", "false", "use cached queries"};
    Configurations::UIntOption numberComcurrentQueries
= {"numberConcurrentQueries", "6", "nuber of maximal concurrently running queries"};
    Configurations::StringOption grpcAddressUri
        = {"grpc",
           "",
           R"(The address to try to bind to the server in URI form. If
the scheme name is omitted, "dns:///" is assumed. To bind to any address,
please use IPv6 any, i.e., [::]:<port>, which also accepts IPv4
connections.  Valid values include dns:///localhost:1234,
192.168.1.1:31416, dns:///[::1]:27182, etc.)"};

protected:
    std::vector<BaseOption*> getOptions() override
    {
        return {&cacheDir, &testsDiscoverDir, &directlySpecifiedTestsFiles, &testFileExtension, &grpcAddressUri};
    }

public:
    SystestConfiguration() = default;
};
}

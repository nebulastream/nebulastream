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
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>

namespace NES::Configuration
{

class SingleNodeWorkerConfiguration final : public Configurations::BaseConfiguration
{
public:
    /**
     * @brief GRPC Server Address URI. By default, it binds to any address and listens on port 8080
     */
    Configurations::StringOption grpcAddressUri
        = {"grpc",
           "[::]:8080",
           R"(The address to try to bind to the server in URI form. If
the scheme name is omitted, "dns:///" is assumed. To bind to any address,
please use IPv6 any, i.e., [::]:<port>, which also accepts IPv4
connections.  Valid values include dns:///localhost:1234,
192.168.1.1:31416, dns:///[::1]:27182, etc.)"};

protected:
    std::vector<BaseOption*> getOptions() override { return {&engineConfiguration, &queryCompilerConfiguration, &grpcAddressUri}; }

public:
    SingleNodeWorkerConfiguration() = default;
    Configurations::WorkerConfiguration engineConfiguration = {"engineConfiguration", "NodeEngine Configuration"};
    Configurations::QueryCompilerConfiguration queryCompilerConfiguration = {"queryCompilerConfiguration", "QueryCompiler Configuration"};
};

///TODO #130: Generalize and move into `nes-configuration`
/// CLI > ConfigFile
template <typename T>
auto loadConfiguration(int argc, const char** argv)
{
    std::unordered_map<std::string, std::string> commandLineParams;
    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];
        size_t pos = arg.find('=');
        if (pos != std::string::npos)
        {
            commandLineParams[arg.substr(0, pos)] = arg.substr(pos + 1);
        }
        else
        {
            NES_WARNING("Argument \"{}\" is missing '=' for key-value pairing.", arg);
        }
    }

    T config;

    if (const auto configPath = commandLineParams.find("--" + Configurations::CONFIG_PATH); configPath != commandLineParams.end())
    {
        config.overwriteConfigWithYAMLFileInput(configPath->second);
    }

    config.overwriteConfigWithCommandLineInput(commandLineParams);

    return config;
}
}

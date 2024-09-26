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
#include <Configurations/PrintingVisitor.hpp>

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

///TODO(#130): Generalize and move into `nes-configuration`
/// CLI > ConfigFile
template <typename T>
auto loadConfiguration(const int argc, const char** argv)
{
    /// Convert the POSIX command line arguments to a map of strings.
    std::map<std::string, std::string> commandLineParams;
    for (int i = 1; i < argc; ++i)
    {
        const size_t pos = std::string(argv[i]).find('=');
        const std::string arg{argv[i]};
        commandLineParams.insert({arg.substr(0, pos), arg.substr(pos + 1, arg.length() - 1)});
    }

    /// Create a configuration object with default values.
    T config;

    /// Read options from the YAML file.
    if (const auto configPath = commandLineParams.find("--" + Configurations::CONFIG_PATH); configPath != commandLineParams.end())
    {
        config.overwriteConfigWithYAMLFileInput(configPath->second);
    }

    /// Options specified on the command line have the highest precedence.
    config.overwriteConfigWithCommandLineInput(commandLineParams);

    return config;
}

[[maybe_unused]] static std::string getHelp()
{
    SingleNodeWorkerConfiguration config{};
    Configurations::PrintingVisitor visitor;
    config.accept(visitor);
    return visitor.toString();
}
} /// namespace NES::Configuration

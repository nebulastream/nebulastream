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
#include <cstddef>
#include <iostream>
#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/PrintingVisitor.hpp>

namespace NES::Configurations
{
template <typename T>
void generateHelp(std::ostream& ostream)
{
    T config{};
    PrintingVisitor visitor{ostream};
    config.accept(visitor);
}

template <typename T>
std::optional<T> loadConfiguration(const int argc, const char** argv)
{
    /// Convert the POSIX command line arguments to a map of strings.
    std::unordered_map<std::string, std::string> commandLineParams;
    for (int i = 1; i < argc; ++i)
    {
        const size_t pos = std::string(argv[i]).find('=');
        const std::string arg{argv[i]};
        if (arg == "--help")
        {
            generateHelp<T>(std::cout);
            return std::nullopt;
        }
        commandLineParams.insert({arg.substr(0, pos), arg.substr(pos + 1, arg.length() - 1)});
    }

    /// Create a configuration object with default values.
    T config;

    /// Read options from the YAML file.
    if (const auto configPath = commandLineParams.find("--" + CONFIG_PATH); configPath != commandLineParams.end())
    {
        config.overwriteConfigWithYAMLFileInput(configPath->second);
    }

    /// Options specified on the command line have the highest precedence.
    config.overwriteConfigWithCommandLineInput(commandLineParams);

    return config;
}
}

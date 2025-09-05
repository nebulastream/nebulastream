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

#include <Configurations/ArgParseVisitor.hpp>
#include <Configurations/PrintingVisitor.hpp>

namespace NES
{

template <typename T>
void print(const T& configuration, std::ostream& ostream)
{
    PrintingVisitor visitor{ostream};
    configuration.accept(visitor);
}

template <typename T>
std::expected<T, std::string> loadConfiguration(const int argc, const char** argv)
{
    T config;
    argparse::ArgumentParser parser("Test Arguments");
    ArgParseVisitor parserCreator{parser};
    config.accept(parserCreator);

    parser.add_argument("--config_path").help("Path to configuration file");
    try
    {
        parser.parse_args(argc, argv);
    }
    catch (const std::exception& e)
    {
        std::stringstream message;
        fmt::println(message, "{}", e.what());
        fmt::println(message, "{}", fmt::streamed(parser));
        return std::unexpected{message.str()};
    }

    if (parser.is_used("--config_path"))
    {
        config.overwriteConfigWithYAMLFileInput(parser.get<std::string>("--config_path"));
    }

    ArgParseParserVisitor visitor{parser};
    config.accept(visitor);

    return config;
}
}

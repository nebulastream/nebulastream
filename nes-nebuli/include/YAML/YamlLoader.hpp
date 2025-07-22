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
#include <string>
#include <fstream>
#include <iostream>

#include <yaml-cpp/yaml.h>

#include <ErrorHandling.hpp>

namespace NES::CLI {

template <typename ConfigType>
class YamlLoader
{
    static ConfigType loadFromFile(const std::string& filePath)
    {
        if (!std::filesystem::exists(filePath))
        {
            throw QueryDescriptionNotReadable("File '{}' does not exist", filePath);
        }

        std::ifstream file{filePath};
        if (!file.is_open())
        {
            throw QueryDescriptionNotReadable("Cannot open file '{}': {}", filePath, std::strerror(errno));
        }

        file.exceptions(std::ifstream::failbit | std::ifstream::badbit);
        return loadFromStream(file);
    }

    static ConfigType loadFromStream(std::istream& stream) { return YAML::Load(stream).as<ConfigType>(); }

public:
    static ConfigType load(const std::string& inputArgument)
    {
        const std::string source = inputArgument == "-" ? "stdin" : inputArgument;

        try
        {
            if (inputArgument == "-")
            {
                return loadFromStream(std::cin);
            }
            return loadFromFile(inputArgument);
        }
        catch (const YAML::ParserException& e)
        {
            throw QueryDescriptionNotReadable("YAML syntax error in '{}' at line {}: {}", source, e.mark.line + 1, e.what());
        }
        catch (const YAML::BadConversion& e)
        {
            throw QueryDescriptionNotReadable("YAML conversion error in '{}': {}", source, e.what());
        }
        catch (const YAML::Exception& e)
        {
            throw QueryDescriptionNotReadable("YAML error in '{}': {}", source, e.what());
        }
        catch (const std::ios_base::failure& e)
        {
            throw QueryDescriptionNotReadable("IO error reading '{}': {}", source, e.what());
        }
    }
};

}

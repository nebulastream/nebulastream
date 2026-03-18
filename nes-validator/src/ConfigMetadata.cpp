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

#include <Validator/ConfigMetadata.hpp>

#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <SourceValidationRegistry.hpp>
#include <SinkValidationRegistry.hpp>

// Source config headers
#include <Sources/FileSourceValidation.hpp>
#include <GeneratorSourceValidation.hpp>
#include <TCPSourceValidation.hpp>

// Sink config headers
#include <Sinks/FileSinkValidation.hpp>
#include <Sinks/PrintSinkValidation.hpp>
#include <VoidSinkValidation.hpp>
#include <ChecksumSinkValidation.hpp>

namespace NES::Validator
{

namespace
{

/// Escape a string for JSON output (handles quotes and backslashes).
std::string jsonEscape(const std::string& s)
{
    std::string result;
    result.reserve(s.size() + 2);
    for (char c : s)
    {
        switch (c)
        {
            case '"': result += "\\\""; break;
            case '\\': result += "\\\\"; break;
            case '\n': result += "\\n"; break;
            case '\r': result += "\\r"; break;
            case '\t': result += "\\t"; break;
            default: result += c;
        }
    }
    return result;
}

/// Convert a ConfigType variant to its string representation.
std::string configTypeToString(const DescriptorConfig::ConfigType& value)
{
    return std::visit(
        [](const auto& v) -> std::string
        {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>)
                return v;
            else if constexpr (std::is_same_v<T, bool>)
                return v ? "true" : "false";
            else if constexpr (std::is_same_v<T, char>)
                return std::string(1, v);
            else if constexpr (std::is_same_v<T, EnumWrapper>)
                return v.getValue();
            else
                return std::to_string(v);
        },
        value);
}

/// Convert a parameterMap to a JSON array string of field metadata.
std::string parameterMapToJson(const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer>& parameterMap)
{
    std::string json = "[";
    bool first = true;
    for (const auto& [name, container] : parameterMap)
    {
        if (!first)
            json += ",";
        first = false;

        const auto typeName = container.getTypeName();
        const auto defaultValue = container.getDefaultValue();
        const bool required = !defaultValue.has_value();

        json += "{\"name\":\"" + jsonEscape(name) + "\"";
        json += ",\"type\":\"" + typeName + "\"";
        json += ",\"required\":" + std::string(required ? "true" : "false");
        if (defaultValue.has_value())
        {
            json += ",\"defaultValue\":\"" + jsonEscape(configTypeToString(defaultValue.value())) + "\"";
        }
        json += "}";
    }
    json += "]";
    return json;
}

/// Lookup table: source type name → pointer to its parameterMap.
const std::unordered_map<std::string, const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer>*>&
getSourceParamMaps()
{
    static const std::unordered_map<std::string, const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer>*>
        map = {
            {"FILE", &ConfigParametersCSV::parameterMap},
            {"GENERATOR", &ConfigParametersGenerator::parameterMap},
            {"TCP", &ConfigParametersTCP::parameterMap},
        };
    return map;
}

/// Lookup table: sink type name → pointer to its parameterMap.
const std::unordered_map<std::string, const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer>*>&
getSinkParamMaps()
{
    static const std::unordered_map<std::string, const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer>*>
        map = {
            {"FILE", &ConfigParametersFile::parameterMap},
            {"PRINT", &ConfigParametersPrint::parameterMap},
            {"VOID", &ConfigParametersVoid::parameterMap},
            {"CHECKSUM", &ConfigParametersChecksum::parameterMap},
        };
    return map;
}

/// Convert a vector of names to a JSON array.
std::string namesToJson(const std::vector<std::string>& names)
{
    std::string json = "[";
    bool first = true;
    for (const auto& name : names)
    {
        if (!first)
            json += ",";
        first = false;
        json += "\"" + jsonEscape(name) + "\"";
    }
    json += "]";
    return json;
}

/// Uppercase a string for case-insensitive lookup.
std::string toUpper(const std::string& s)
{
    std::string result = s;
    for (auto& c : result)
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return result;
}

} // anonymous namespace

std::string getRegisteredSourceTypes()
{
    return namesToJson(SourceValidationRegistry::instance().getRegisteredNames());
}

std::string getRegisteredSinkTypes()
{
    return namesToJson(SinkValidationRegistry::instance().getRegisteredNames());
}

std::string getSourceConfigFields(const std::string& sourceType)
{
    const auto& maps = getSourceParamMaps();
    if (auto it = maps.find(toUpper(sourceType)); it != maps.end())
    {
        return parameterMapToJson(*it->second);
    }
    return "[]";
}

std::string getSinkConfigFields(const std::string& sinkType)
{
    const auto& maps = getSinkParamMaps();
    if (auto it = maps.find(toUpper(sinkType)); it != maps.end())
    {
        return parameterMapToJson(*it->second);
    }
    return "[]";
}

} // namespace NES::Validator

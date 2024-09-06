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

#include <cstdint>
#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <memory>
#include <unordered_map>
#include <utility>
#include <variant>
#include <API/Schema.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES::Sources
{

class SourceDescriptor
{
public:
    /// Tag struct that tags a config key with a type.
    /// The tagged type allows to determine the correct variant of a config paramater, without supplying it as a template parameter.
    /// Therefore, config keys defined in a single place are sufficient to retrieve parameters from the config, reducing the surface for errors.
    ///-Todo: could store validate function in ConfigKey that performs parameter specific validation.
    template <typename T>
    struct ConfigKey
    {
        std::string key;
        std::optional<T> defaultValue;
        using Type = T;

        ConfigKey(std::string key) : key(std::move(key)), defaultValue(std::nullopt) { }
        ConfigKey(std::string key, T defaultValue) : key(std::move(key)), defaultValue(std::move(defaultValue)) { }
    };

    using ConfigType = std::variant<
        int32_t,
        uint32_t,
        bool,
        char,
        float,
        double,
        std::string,
        Configurations::TCPDecideMessageSize,
        Configurations::InputFormat>;
    using Config = std::unordered_map<std::string, ConfigType>;

    /// Constructor used during initial parsing to create an initial SourceDescriptor.
    explicit SourceDescriptor(std::string logicalSourceName, std::string sourceType);
    /// Constructor used before applying schema inference.
    explicit SourceDescriptor(std::string sourceType, Configurations::InputFormat inputFormat, Config&& config);
    /// Constructor used after schema inference, when all required information are available.
    explicit SourceDescriptor(
        std::shared_ptr<Schema> schema, std::string sourceType, Configurations::InputFormat inputFormat, Config&& config);

    ///-Todo: clean up constructors
    explicit SourceDescriptor(
        std::string logicalSourceName,
        std::shared_ptr<Schema> schema,
        std::string sourceType,
        Configurations::InputFormat inputFormat,
        Config&& config);
    ~SourceDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceDescriptor);
    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs);

    [[nodiscard]] std::shared_ptr<Schema> getSchema() const;
    std::string getLogicalSourceName() const;

    std::string getLogicalSourceName() const;
    void setSchema(const std::shared_ptr<Schema>& schema);
    [[nodiscard]] std::string getSourceType() const;

    void setSourceType(std::string sourceType);
    [[nodiscard]] const Configurations::InputFormat& getInputFormat() const;

    /// Passing by const&, because unordered_map lookup requires std::string (vs std::string_view)
    void setConfigType(const std::string& key, ConfigType value);

    /// Takes a key that is a tagged ConfigKey, with a string key and a tagged type.
    /// Uses the key to retrieve to lookup the config paramater.
    /// Uses the taggeg type to retrieve the correct type from the variant value in the configuration.
    template <typename ConfigKey>
    typename ConfigKey::Type getFromConfig(const ConfigKey& key) const
    {
        const auto& value = config.at(key.key);
        return std::get<typename ConfigKey::Type>(value);
    }

    /// In contrast to getFromConfig(), tryGetFromConfig checks if the key exists and if the tagged type is correct.
    /// If not, tryGetFromConfig returns a nullopt, otherwise it returns an optional containing a value of the tagged type.
    template <typename ConfigKey>
    std::optional<typename ConfigKey::Type> tryGetFromConfig(const ConfigKey& configKey) const
    {
        if (config.contains(configKey.key) && std::holds_alternative<typename ConfigKey::Type>(config.at(configKey.key)))
        {
            const auto& value = config.at(configKey.key);
            return std::get<typename ConfigKey::Type>(value);
        }
        NES_DEBUG("SourceDescriptor did not contain key: {}, with type: {}", configKey.key, typeid(ConfigKey).name());
        return std::nullopt;
    }

    template <typename T>
    static std::optional<T> stringParameterAs(std::string stringParameter)
    {
        if constexpr (std::is_same_v<T, int32_t>)
        {
            return std::stoi(stringParameter);
        }
        else if constexpr (std::is_same_v<T, uint32_t>)
        {
            return std::stoul(stringParameter);
        }
        else if constexpr (std::is_same_v<T, bool>)
        {
            return stringParameter == "true"; ///-Todo: improve
        }
        else if constexpr (std::is_same_v<T, char>)
        {
            if (stringParameter.length() != 0)
            {
                NES_ERROR("Char SourceDescriptor config paramater must not be empty.")
                return std::nullopt;
            }
            return stringParameter[0];
        }
        else if constexpr (std::is_same_v<T, float>)
        {
            return std::stof(stringParameter);
        }
        else if constexpr (std::is_same_v<T, double>)
        {
            return std::stod(stringParameter);
        }
        else if constexpr (std::is_same_v<T, std::string>)
        {
            return stringParameter;
        }
        else if constexpr (std::is_same_v<T, Configurations::TCPDecideMessageSize>)
        {
            return magic_enum::enum_cast<Configurations::TCPDecideMessageSize>(stringParameter).value();
        }
        else if constexpr (std::is_same_v<T, Configurations::InputFormat>)
        {
            return magic_enum::enum_cast<Configurations::InputFormat>(stringParameter).value();
        }
        else
        {
            return std::nullopt;
        }
    }

    template <typename ConfigKey>
    static void
    validateAndFormatParameter(const ConfigKey& configKey, const std::map<std::string, std::string>& config, Config& validatedConfig)
    {
        if (config.contains(configKey.key))
        {
            std::optional<typename ConfigKey::Type> formattedParameter
                = SourceDescriptor::stringParameterAs<typename ConfigKey::Type>(config.at(configKey.key));
            if (formattedParameter.has_value())
            {
                validatedConfig.emplace(std::make_pair(configKey.key, formattedParameter.value()));
                return; /// success
            }
            throw CannotFormatSourceData(configKey.key + " Parameter formatting unsuccessful.");
        }

        if (configKey.defaultValue.has_value())
        {
            validatedConfig.emplace(std::make_pair(configKey.key, configKey.defaultValue.value()));
            return; /// success
        }

        throw CannotFormatSourceData(configKey.key + " was not configured and no default value was available.");
    };

private:
    /// 'schema', 'sourceName', and 'inputFormat' are shared by all sources and are therefore not part of the config.
    ///-Todo: make struct? (schema: getter/setter, logicalSourceName: getter, sourceName: getter/setter, inputFormat: getter, config: getter/setter)
    std::shared_ptr<Schema> schema;
    std::string logicalSourceName;
    std::string sourceType;
    Configurations::InputFormat inputFormat{};
    Config config;

    friend std::ostream& operator<<(std::ostream& out, const Config& config);
};

}

/// Specializing the fmt ostream_formatter to accept SourceDescriptor objects.
/// Allows to call fmt::format("SourceDescriptor: {}", sourceDescriptorObject); and therefore also works with our logging.
namespace fmt
{
template <>
struct formatter<NES::Sources::SourceDescriptor> : ostream_formatter
{
};
}

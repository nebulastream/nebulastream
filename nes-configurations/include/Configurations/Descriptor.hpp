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
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES::Configurations
{

/// Todo #355 : refactor usages of Descriptor, specifically in Sources and Sinks, into general configuration
/// Config: The design principle of the Descriptor config is that the entire definition of the configuration happens in one place.
/// When defining a 'ConfigParameter', all information relevant for a configuration parameter are defined:
/// - the type
/// - the name
/// - the validation function
/// All functions that operate on the config operate on ConfigParameters and can therefore access all relevant information.
/// This design makes it difficult to use the wrong (string) name to access a parameter, the wrong type, e.g., in a templated function, or
/// to forget to define a validation function. Also, changing the type/name/validation function of a config parameter can be done in a single place.
class DescriptorConfig
{
public:
    using ConfigType = std::variant<int32_t, uint32_t, bool, char, float, double, std::string, EnumWrapper>;
    using Config = std::unordered_map<std::string, ConfigType>;

    /// Tag struct that tags a config key with a type.
    /// The tagged type allows to determine the correct variant of a config paramater, without supplying it as a template parameter.
    /// Therefore, config keys defined in a single place are sufficient to retrieve parameters from the config, reducing the surface for errors.
    template <typename T, typename U = void>
    struct ConfigParameter
    {
        using Type = T;
        using EnumType = U;
        using ValidateFunc = std::function<std::optional<T>(const std::unordered_map<std::string, std::string>& config)>;

        ConfigParameter(std::string name, std::optional<T> defaultValue, ValidateFunc&& validateFunc)
            : name(std::move(name)), validateFunc(std::move(validateFunc)), defaultValue(std::move(defaultValue))
        {
            static_assert(
                not(std::is_same_v<EnumWrapper, T> && std::is_same_v<void, U>),
                "An EnumWrapper config parameter must define the enum type as a template parameter.");
        }

        const std::string name;
        const ValidateFunc validateFunc;
        const std::optional<T> defaultValue;

        operator const std::string&() const { return name; }

        std::optional<ConfigType> validate(const std::unordered_map<std::string, std::string>& config) const
        {
            return this->validateFunc(config);
        }

        static constexpr bool isEnumWrapper() { return not(std::is_same_v<U, void>); }
    };

    /// Uses type erasure to create a container for ConfigParameters, which are templated.
    /// @note Expects ConfigParameter to have a 'validate' function.
    class ConfigParameterContainer
    {
    public:
        template <typename T>
        requires(!std::same_as<std::decay_t<T>, ConfigParameterContainer>)
        ConfigParameterContainer(T&& configParameter)
            : configParameter(std::make_shared<ConfigParameterModel<T>>(std::forward<T>(configParameter)))
        {
        }

        std::optional<ConfigType> validate(const std::unordered_map<std::string, std::string>& config) const
        {
            return configParameter->validate(config);
        }

        std::optional<ConfigType> getDefaultValue() const { return configParameter->getDefaultValue(); }

        /// Describes what a ConfigParameter that is in the ConfigParameterContainer does (interface).
        struct ConfigParameterConcept
        {
            virtual ~ConfigParameterConcept() = default;
            virtual std::optional<ConfigType> validate(const std::unordered_map<std::string, std::string>& config) const = 0;
            virtual std::optional<ConfigType> getDefaultValue() const = 0;
        };

        /// Defines the concrete behavior of the ConfigParameterConcept, i.e., which specific functions from T to call.
        template <typename T>
        struct ConfigParameterModel : ConfigParameterConcept
        {
            ConfigParameterModel(const T& configParameter) : configParameter(configParameter) { }
            std::optional<ConfigType> validate(const std::unordered_map<std::string, std::string>& config) const override
            {
                return configParameter.validate(config);
            }
            std::optional<ConfigType> getDefaultValue() const override { return configParameter.defaultValue; }

        private:
            T configParameter;
        };

        std::shared_ptr<const ConfigParameterConcept> configParameter;
    };

private:
    template <typename T, typename EnumType>
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
            using namespace std::literals::string_view_literals;
            auto caseInsensitiveEqual = [](const unsigned char leftChar, const unsigned char rightChar)
            { return std::tolower(leftChar) == std::tolower(rightChar); };

            if (std::ranges::equal(stringParameter, "true"sv, caseInsensitiveEqual))
            {
                return true;
            }
            if (std::ranges::equal(stringParameter, "false"sv, caseInsensitiveEqual))
            {
                return false;
            }
            return std::nullopt;
        }
        else if constexpr (std::is_same_v<T, char>)
        {
            if (stringParameter.length() != 1)
            {
                throw InvalidConfigParameter(fmt::format(
                    "Char Descriptor config paramater must be of length one, but got: {}, which is of size: {}.",
                    stringParameter,
                    stringParameter.length()));
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
        else if constexpr (std::is_same_v<T, EnumWrapper>)
        {
            if (auto enumWrapperValue = EnumWrapper(stringParameter); enumWrapperValue.asEnum<EnumType>().has_value()
                && magic_enum::enum_contains<EnumType>(enumWrapperValue.asEnum<EnumType>().value()))
            {
                return enumWrapperValue;
            }
            NES_ERROR("Failed to convert EnumWrapper with value: {}, to InputFormat enum.", stringParameter);
            return std::nullopt;
        }
        else
        {
            return std::nullopt;
        }
    }

public:
    template <typename ConfigParameter>
    static std::optional<typename ConfigParameter::Type>
    tryGet(const ConfigParameter& configParameter, const std::unordered_map<std::string, std::string>& config)
    {
        /// No specific validation and formatting function defined, using default formatter.
        if (config.contains(configParameter))
        {
            return stringParameterAs<typename ConfigParameter::Type, typename ConfigParameter::EnumType>(config.at(configParameter));
        }
        /// The user did not specify the parameter, if a default value is available, return the default value.
        if (configParameter.defaultValue.has_value())
        {
            return configParameter.defaultValue;
        }
        NES_ERROR("ConfigParameter: {}, is not available in config and there is no default value.", configParameter.name);
        return std::nullopt;
    };

    /// Takes ConfigParameters as inputs and creates an unordered map using the 'key' form the ConfigParameter as key and the ConfigParameter as value.
    /// This function should be used at the end of the Config definition of a source, e.g., the ConfigParametersTCP definition.
    /// The map makes it possible that we can simply iterate over all config parameters to check if the user provided all mandatory
    /// parameters and whether the configuration is valid. Additionally, we can quickly check if there are unsupported parameters.
    template <typename... Args>
    static std::unordered_map<std::string, ConfigParameterContainer> createConfigParameterContainerMap(Args&&... parameters)
    {
        return std::unordered_map<std::string, ConfigParameterContainer>(
            {std::make_pair(parameters.name, std::forward<Args>(parameters))...});
    }
};

/// The Descriptor is an IMMUTABLE struct (all members and functions must be const) that:
/// 1. Is a generic descriptor that can fully describe any kind of Source.
/// 2. Is (de-)serializable, making it possible to send to other nodes.
/// 3. Is part of the main interface of 'nes-sources': The SourceProvider takes a Descriptor and returns a fully configured Source.
/// 4. Is used by the frontend to validate and format string configs.
struct Descriptor
{
    explicit Descriptor(DescriptorConfig::Config&& config);
    ~Descriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const Descriptor& descriptor);
    friend bool operator==(const Descriptor& lhs, const Descriptor& rhs) = default;

    /// Takes a key that is a tagged ConfigParameter, with a string key and a tagged type.
    /// Uses the key to retrieve to lookup the config paramater.
    /// Uses the taggeg type to retrieve the correct type from the variant value in the configuration.
    template <typename ConfigParameter>
    auto getFromConfig(const ConfigParameter& configParameter) const
    {
        const auto& value = config.at(configParameter);
        if constexpr (ConfigParameter::isEnumWrapper())
        {
            const EnumWrapper enumWrapper = std::get<EnumWrapper>(value);
            return enumWrapper.asEnum<typename ConfigParameter::EnumType>().value();
        }
        else
        {
            return std::get<typename ConfigParameter::Type>(value);
        }
    }

    /// In contrast to getFromConfig(), tryGetFromConfig checks if the key exists and if the tagged type is correct.
    /// If not, tryGetFromConfig returns a nullopt, otherwise it returns an optional containing a value of the tagged type.
    template <typename ConfigParameter>
    std::optional<typename ConfigParameter::Type> tryGetFromConfig(const ConfigParameter& configParameter) const
    {
        if (config.contains(configParameter) && std::holds_alternative<typename ConfigParameter::Type>(config.at(configParameter)))
        {
            const auto& value = config.at(configParameter);
            return std::get<typename ConfigParameter::Type>(value);
        }
        NES_DEBUG("Descriptor did not contain key: {}, with type: {}", configParameter, typeid(ConfigParameter).name());
        return std::nullopt;
    }

    const DescriptorConfig::Config config;

protected:
    std::string toStringConfig() const;

private:
    friend std::ostream& operator<<(std::ostream& out, const DescriptorConfig::Config& config);
};

}

/// Specializing the fmt ostream_formatter to accept Descriptor objects.
/// Allows to call fmt::format("Descriptor: {}", descriptorObject); and therefore also works with our logging.
namespace fmt
{
template <>
struct formatter<NES::Configurations::Descriptor> : ostream_formatter
{
};

}

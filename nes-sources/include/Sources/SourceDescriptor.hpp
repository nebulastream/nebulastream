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
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <API/Schema.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Sources/EnumWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES::Sources
{

/// The SourceDescriptor:
/// 1. Is a generic descriptor that can fully describe any kind of Source.
/// 2. Is (de-)serializable, making it possible to send to other nodes.
/// 3. Is part of the main interface of 'nes-sources': The SourceProvider takes a SourceDescriptor and returns a fully configured Source.
/// 4. Is used by the frontend to validate and format string configs.
/// Config: The design principle of the SourceDescriptor config is that the entire definition of the configuration happens in one place.
/// When defining a 'ConfigParameter', all information relevant for a configuration parameter are defined:
/// - the type
/// - the name
/// - the validation function
/// All functions that operate on the config operate on ConfigParameters and can therefore access all relevant information.
/// This design makes it difficult to use the wrong (string) name to access a parameter, the wrong type, e.g., in a templated function, or
/// to forget to define a validation function. Also, changing the type/name/validation function of a config parameter can be done in a single place.
struct SourceDescriptor
{
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
        using ValidateFunc = std::function<std::optional<T>(const std::map<std::string, std::string>& config)>;

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

        std::optional<ConfigType> validate(const std::map<std::string, std::string>& config) const { return this->validateFunc(config); }

        static constexpr bool isEnumWrapper() { return not(std::is_same_v<U, void>); }
    };

    /// Uses type erasure to create a container for ConfigParameters, which are templated.
    /// @note Expects ConfigParameter to have a 'validate' function.
    class ConfigParameterContainer
    {
    public:
        template <typename T>
        ConfigParameterContainer(T&& configParameter) : configParameter(std::make_shared<Model<T>>(std::forward<T>(configParameter)))
        {
        }

        std::optional<ConfigType> validate(const std::map<std::string, std::string>& config) const
        {
            return configParameter->validate(config);
        }

        std::optional<ConfigType> getDefaultValue() const { return configParameter->getDefaultValue(); }

        struct Concept
        {
            virtual ~Concept() = default;
            virtual std::optional<ConfigType> validate(const std::map<std::string, std::string>& config) const = 0;
            virtual std::optional<ConfigType> getDefaultValue() const = 0;
        };

        template <typename T>
        struct Model : Concept
        {
            Model(const T& configParameter) : configParameter(configParameter) { }
            std::optional<ConfigType> validate(const std::map<std::string, std::string>& config) const override
            {
                return configParameter.validate(config);
            }
            std::optional<ConfigType> getDefaultValue() const override { return configParameter.defaultValue; }

        private:
            T configParameter;
        };

        std::shared_ptr<const Concept> configParameter;
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

    /// Constructor used during initial parsing to create an initial SourceDescriptor.
    explicit SourceDescriptor(std::string logicalSourceName, std::string sourceType);
    /// Constructor used before applying schema inference.
    /// Todo: used only in serialization -> can potentially remove, after refactoring serialization
    explicit SourceDescriptor(std::string sourceType, Configurations::InputFormat inputFormat, Config&& config);
    /// Constructor used after schema inference, when all required information are available.
    /// Todo: used only in serialization -> can potentially remove, after refactoring serialization
    explicit SourceDescriptor(
        std::shared_ptr<Schema> schema, std::string sourceType, Configurations::InputFormat inputFormat, Config&& config);

    /// Used by Sources to create a valid SourceDescriptor.
    explicit SourceDescriptor(
        std::string logicalSourceName,
        std::shared_ptr<Schema> schema,
        std::string sourceType,
        Configurations::InputFormat inputFormat,
        Config&& config);
    ~SourceDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceDescriptor);
    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs);

    /// Passing by const&, because unordered_map lookup requires std::string (vs std::string_view)
    void setConfigType(const std::string& key, ConfigType value);

    /// Takes a key that is a tagged ConfigParameter, with a string key and a tagged type.
    /// Uses the key to retrieve to lookup the config paramater.
    /// Uses the taggeg type to retrieve the correct type from the variant value in the configuration.
    template <typename ConfigParameter>
    auto getFromConfig(const ConfigParameter& key) const
    {
        const auto& value = config.at(key);
        if constexpr (ConfigParameter::isEnumWrapper())
        {
            const EnumWrapper enumWrapper = std::get<EnumWrapper>(value);
            return enumWrapper.toEnum<typename ConfigParameter::EnumType>().value();
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
        NES_DEBUG("SourceDescriptor did not contain key: {}, with type: {}", configParameter, typeid(ConfigParameter).name());
        return std::nullopt;
    }

    template <typename ConfigParameter>
    static std::optional<typename ConfigParameter::Type>
    tryGet(const ConfigParameter& configParameter, const std::map<std::string, std::string>& config)
    {
        /// No specific validation and formatting function defined, using default formatter.
        if (config.contains(configParameter))
        {
            return SourceDescriptor::stringParameterAs<typename ConfigParameter::Type, typename ConfigParameter::EnumType>(
                config.at(configParameter));
        }
        /// The user did not specify the parameter, if a default value is available, return the default value.
        if (configParameter.defaultValue.has_value())
        {
            return configParameter.defaultValue;
        }
        NES_ERROR("ConfigParameter: {}, is not available in config and there is no default value.", configParameter.name);
        return std::nullopt;
    };

    /// 'schema', 'sourceName', and 'inputFormat' are shared by all sources and are therefore not part of the config.
    std::shared_ptr<Schema> schema;
    std::string logicalSourceName;
    std::string sourceType;
    Configurations::InputFormat inputFormat{};

private:
    Config config;
    friend std::ostream& operator<<(std::ostream& out, const Config& config);


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
        else if constexpr (std::is_same_v<T, EnumWrapper>)
        {
            const auto enumWrapperValue = EnumWrapper::create(stringParameter);
            if (enumWrapperValue.toEnum<EnumType>().has_value()
                && magic_enum::enum_contains<EnumType>(enumWrapperValue.toEnum<EnumType>().value()))
            {
                return enumWrapperValue;
            }
            NES_ERROR("Failed to convert EnumWrapper with value: {}, to InputFormat enum.", enumWrapperValue);
            return std::nullopt;
        }
        else
        {
            return std::nullopt;
        }
    }
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

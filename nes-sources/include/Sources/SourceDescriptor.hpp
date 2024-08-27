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
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <API/Schema.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Sources
{

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class SourceDescriptor
{
public:
    /// Tag struct that tags a config key with a type.
    /// The tagged type allows to determine the correct variant of a config paramater, without supplying it as a template parameter.
    /// Therefore, config keys defined in a single place are sufficient to retrieve parameters from the config, reducing the surface for errors.
    /// For example, the TCPSource defines:
    ///   static inline const SourceDescriptor::ConfigKey<std::string> HOST{"socket_host"};
    /// HOST can then be used to set configurations to a descriptor and to retrieve configurations:
    ///   Set: sourceDescriptorConfig.emplace(std::make_pair(ConfigParametersTCP::HOST.key, tcpSourceConfig->sockethost())); <-- from serialization.
    ///   Get: descriptor->getFromConfig(ConfigParametersTCP::HOST); <-- The ConfigKey is enough to retrieve the parameter with the correct type.
    template <typename T>
    struct ConfigKey
    {
        std::string key;
        using Type = T;

        ConfigKey(std::string key) : key(std::move(key)) { }
    };

    using ConfigType = std::variant<int32_t, uint32_t, bool, char, float, double, std::string, Configurations::TCPDecideMessageSize>;
    using Config = std::unordered_map<std::string, ConfigType>;

    /// Constructor used during initial parsing to create an initial SourceDescriptor.
    explicit SourceDescriptor(std::string sourceName);
    /// Constructor used before applying schema inference.
    explicit SourceDescriptor(std::string sourceName, Configurations::InputFormat inputFormat, Config&& config);
    /// Constructor used after schema inference, when all required information are available.
    explicit SourceDescriptor(SchemaPtr schema, std::string sourceName, Configurations::InputFormat inputFormat, Config&& config);
    ~SourceDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceHandle);
    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs);

    [[nodiscard]] SchemaPtr getSchema() const;

    std::string getLogicalSourceName() const;
    void setSchema(const SchemaPtr& schema);
virtual std::string toString() const = 0;

    [[nodiscard]] virtual bool equal(SourceDescriptor& other) const = 0;

    virtual ~SourceDescriptor() = default;

    [[nodiscard]] const std::string& getSourceName() const;

    void setSourceName(std::string sourceName);
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
        if (config.contains(configKey.key) && std::holds_alternative<ConfigKey::Type>(config.at(configKey.key)))
        {
            const auto& value = config.at(configKey.key);
            return std::get<typename ConfigKey::Type>(value);
        }
        NES_TRACE("SourceDescriptor did not contain key: {}, with type: {}", configKey.key, typeid(ConfigKey).name());
        return std::nullopt;
    }

private:
    /// 'schema', 'sourceName', and 'inputFormat' are shared by all sources and are therefore not part of the config.
    SchemaPtr schema;
    std::string logicalSourceName;
    std::string sourceType;
};

}

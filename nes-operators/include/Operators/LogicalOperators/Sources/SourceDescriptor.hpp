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

#include <memory>
#include <unordered_map>
#include <API/Schema.hpp>
#include <Configurations/ConfigurationsNames.hpp>
namespace NES
{

class SourceDescriptor
{
public:
    using ConfigType
        = std::variant<int32_t, int64_t, uint32_t, uint64_t, bool, char, float, double, std::string, Configurations::TCPDecideMessageSize>;
    using Config = std::unordered_map<std::string, ConfigType>;

    static inline const std::string PLUGIN_NAME_CSV = "CSV"; ///-Todo: looks ugly
    static inline const std::string PLUGIN_NAME_TCP = "TCP";

    explicit SourceDescriptor(std::string sourceName);
    explicit SourceDescriptor(std::string sourceName, Configurations::InputFormat inputFormat, Config&& config);
    explicit SourceDescriptor(SchemaPtr schema, std::string sourceName, Configurations::InputFormat inputFormat, Config&& config);
    ~SourceDescriptor() = default;

    SchemaPtr getSchema() const;
    void setSchema(const SchemaPtr& schema);

    [[nodiscard]] std::string getSourceName() const;
    void setSourceName(std::string sourceName);

    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceHandle);

    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs);

    [[nodiscard]] const Configurations::InputFormat& getInputFormat() const;

    [[nodiscard]] const Config& getConfig() const;

    /// Passing by const&, because unordered_map lookup requires std::string (vs std::string_view)
    void setConfigType(const std::string& key, ConfigType value);

private:
    ///-Todo: workerId <-- currently always the same
    SchemaPtr schema;
    std::string sourceName;
    Configurations::InputFormat inputFormat{};
    Config config;

    friend std::ostream& operator<<(std::ostream& out, const Config& config);
};

} /// namespace NES

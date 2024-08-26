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

#include <Exceptions/RuntimeException.hpp>
namespace NES::Sources
{

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class SourceDescriptor
{
public:
    ///-Todo: can we somehow move Configurations::TCPDecideMessageSize out of the ConfigType variant? (it is too specialized)
    using ConfigType = std::variant<int32_t, uint32_t, bool, char, float, double, std::string, Configurations::TCPDecideMessageSize>;
    using Config = std::unordered_map<std::string, ConfigType>;

    explicit SourceDescriptor(std::string sourceName);
    explicit SourceDescriptor(std::string sourceName, Configurations::InputFormat inputFormat, Config&& config);
    explicit SourceDescriptor(SchemaPtr schema, std::string sourceName, Configurations::InputFormat inputFormat, Config&& config);
    ~SourceDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceHandle);
    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs);

    SchemaPtr getSchema() const;

    std::string getLogicalSourceName() const;

    void setSchema(const SchemaPtr& schema);
virtual std::string toString() const = 0;

    [[nodiscard]] virtual bool equal(SourceDescriptor& other) const = 0;

    virtual ~SourceDescriptor() = default;

    [[nodiscard]] const std::string& getSourceName() const;

    void setSourceName(std::string sourceName);
    [[nodiscard]] const Configurations::InputFormat& getInputFormat() const;

    [[nodiscard]] const Config& getConfig() const;

    /// Passing by const&, because unordered_map lookup requires std::string (vs std::string_view)
    void setConfigType(const std::string& key, ConfigType value);

    template <typename T>
    T getFromConfig(const std::string& key)
    {
        return std::get<T>(config.at(key));
    }

private:
    /// 'schema', 'sourceName', and 'inputFormat' are shared by all sources and are therefore not part of the config.
    SchemaPtr schema;
    std::string logicalSourceName;
    std::string sourceType;
};

}

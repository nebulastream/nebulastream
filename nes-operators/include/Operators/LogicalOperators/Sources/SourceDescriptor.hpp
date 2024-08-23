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
#include <Exceptions/RuntimeException.hpp>
namespace NES
{

class SourceDescriptor
{
    using SourceDescriptorTypes = std::
        variant<int32_t, uint32_t, bool, float, double, std::string, Configurations::InputFormat, Configurations::TCPDecideMessageSize>;

public:
    using Config = std::unordered_map<std::string, SourceDescriptorTypes>;

    static inline const std::string PLUGIN_NAME_CSV = "CSV";
    static inline const std::string PLUGIN_NAME_TCP = "TCP";

    explicit SourceDescriptor(SchemaPtr schema, std::string sourceName, Config&& config);
    ~SourceDescriptor() = default;

    SchemaPtr getSchema() const; ///-todo: can we remove the getter?
    void setSchema(const SchemaPtr& schema); ///-todo: can we remove the setter?

    [[nodiscard]] std::string getSourceName() const;
    void setSourceName(std::string sourceName);

    virtual std::string toString() const = 0; ///-todo: convert toString function to ostream operator '<<'

    [[nodiscard]] virtual bool equal(SourceDescriptor& other) const = 0; ///-todo: overload '=='

private:
    SchemaPtr schema;
    std::string sourceName;
    Config config;
};

} /// namespace NES

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
#include <Exceptions/RuntimeException.hpp>
namespace NES
{

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class SourceDescriptor
{
public:
    static inline const std::string PLUGIN_NAME_CSV = "CSV";
    static inline const std::string PLUGIN_NAME_TCP = "TCP";

    explicit SourceDescriptor(SchemaPtr schema);

    explicit SourceDescriptor(SchemaPtr schema, std::string logicalSourceName);

    explicit SourceDescriptor(SchemaPtr schema, std::string logicalSourceName, std::string sourceType);

    SchemaPtr getSchema() const;

    std::string getLogicalSourceName() const;

    void setSchema(const SchemaPtr& schema);

    virtual std::string toString() const = 0;

    [[nodiscard]] virtual bool equal(SourceDescriptor& other) const = 0;

    virtual ~SourceDescriptor() = default;

    [[nodiscard]] const std::string& getSourceType() const;

    void setSourceType(std::string sourceName);

private:
    SchemaPtr schema;
    std::string logicalSourceName;
    std::string sourceType;
};

} /// namespace NES

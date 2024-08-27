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

#include <sstream>
#include <utility>
#include <API/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
namespace NES::Sources
{

SourceDescriptor::SourceDescriptor(SchemaPtr schema)
{
    this->schema = schema;
}

SourceDescriptor::SourceDescriptor(SchemaPtr schema, std::string logicalSourceName)
    : schema(std::move(schema)), logicalSourceName(std::move(logicalSourceName)), sourceType("INVALID")
{
}

SourceDescriptor::SourceDescriptor(SchemaPtr schema, std::string logicalSourceName, std::string sourceName)
    : schema(std::move(schema)), logicalSourceName(std::move(logicalSourceName)), sourceType(std::move(sourceName))
{
}

SchemaPtr SourceDescriptor::getSchema() const
{
    return schema;
}

std::string SourceDescriptor::getLogicalSourceName() const
{
    return logicalSourceName;
}

void SourceDescriptor::setSchema(const SchemaPtr& schema)
{
    this->schema = schema;
}

const std::string& SourceDescriptor::getSourceName() const
{
    return sourceType;
}

void SourceDescriptor::setSourceName(std::string sourceName)
{
    this->sourceType = std::move(sourceName);
}

const Configurations::InputFormat& SourceDescriptor::getInputFormat() const
{
    return this->inputFormat;
}

void SourceDescriptor::setConfigType(const std::string& key, ConfigType value)
{
    if (!this->config.contains(key))
    {
        return;
    }
    this->config.at(key) = std::move(value);
}


/// Define a ConfigPrinter to generate print functions for all options of the std::variant 'ConfigType'.
template <typename T>
struct is_enum_type : std::is_enum<T>
{
};
struct ConfigPrinter
{
    std::ostream& out;

    /// General case for non-enum types.
    template <typename T>
    std::enable_if_t<!is_enum_type<T>::value> operator()(const T& value) const
    {
        out << value;
    }

    /// Specialization for enum types.
    template <typename T>
    std::enable_if_t<is_enum_type<T>::value> operator()(const T& value) const
    {
        out << std::string(magic_enum::enum_name(value));
    }
};
std::ostream& operator<<(std::ostream& out, const SourceDescriptor::Config& config)
{
    for (const auto& [key, value] : config)
    {
        out << "  " << key << ": ";
        std::visit(ConfigPrinter{out}, value);
        out << '\n';
    }
    out << "}";
    return out;
}

std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceHandle)
{
    return out << "SourceDescriptor:"
               << "\nSource name: " << sourceHandle.sourceName << "\nSchema: " << sourceHandle.schema->toString()
               << "\nInputformat: " << std::string(magic_enum::enum_name(sourceHandle.inputFormat)) << "\nConfig:\n"
               << sourceHandle.config;
}

/// Define a helper struct to generate equal comparison functions for std::visit.
struct VariantComparator
{
    template <typename T, typename U>
    bool operator()(const T& lhs, const U& rhs) const
    {
        if constexpr (std::is_same_v<T, U>)
        {
            return lhs == rhs;
        }
        else
        {
            return false;
        }
    }
};
bool operator==(const SourceDescriptor::Config& lhs, const SourceDescriptor::Config& rhs)
{
    if (lhs.size() != rhs.size())
    {
        return false;
    }

    for (const auto& [key, value] : lhs)
    {
        auto it = rhs.find(key);
        if (it == rhs.end())
        {
            return false;
        }
        if (!std::visit(VariantComparator{}, value, it->second))
        {
            return false;
        }
    }

    return true;
}
bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
{
    return lhs.schema == rhs.getSchema() && lhs.sourceName == rhs.sourceName && lhs.config == rhs.config;
}

} /// namespace NES

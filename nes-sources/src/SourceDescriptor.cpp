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

#include <ostream>
#include <sstream>
#include <variant>
#include <API/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::Sources
{

SourceDescriptor::SourceDescriptor(std::string logicalSourceName) : logicalSourceName(std::move(logicalSourceName))
{
}
SourceDescriptor::SourceDescriptor(std::string sourceType, Configurations::InputFormat inputFormat, Config&& config)
    : sourceType(std::move(sourceType)), inputFormat(inputFormat), config(std::move(config))
{
}

SourceDescriptor::SourceDescriptor(
    std::shared_ptr<Schema> schema, std::string sourceType, Configurations::InputFormat inputFormat, Config&& config)
    : schema(std::move(schema)), sourceType(std::move(sourceType)), inputFormat(std::move(inputFormat)), config(std::move(config))
{
}
SourceDescriptor::SourceDescriptor(
    std::string logicalSourceName,
    std::shared_ptr<Schema> schema,
    std::string sourceType,
    Configurations::InputFormat inputFormat,
    Config&& config)
    : schema(std::move(schema))
    , logicalSourceName(std::move(logicalSourceName))
    , sourceType(std::move(sourceType))
    , inputFormat(std::move(inputFormat))
    , config(std::move(config))
{
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

std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceDescriptor)
{
    return out << "SourceDescriptor:"
               << "\nSource type: " << sourceDescriptor.sourceType
               << "\nSchema: " << ((sourceDescriptor.schema) ? sourceDescriptor.schema->toString() : "NULL")
               << "\nInputformat: " << std::string(magic_enum::enum_name(sourceDescriptor.inputFormat)) << "\nConfig:\n"
               << sourceDescriptor.config;
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
    return lhs.schema == rhs.schema && lhs.sourceType == rhs.sourceType && lhs.config == rhs.config;
}

}

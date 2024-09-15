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
#include <Configurations/Descriptor.hpp>

namespace NES::Sources
{

Descriptor::Descriptor(Config&& config) : config(std::move(config))
{
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
std::ostream& operator<<(std::ostream& out, const Descriptor::Config& config)
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

std::ostream& operator<<(std::ostream& out, const Descriptor& descriptor)
{
    return out << "\nDescriptor: " << descriptor.config;
}


std::string Descriptor::toStringConfig() const
{
    std::stringstream ss;
    ss << this->config;
    return ss.str();
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
bool operator==(const Descriptor::Config& lhs, const Descriptor::Config& rhs)
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
bool operator==(const Descriptor& lhs, const Descriptor& rhs)
{
    return lhs.config == rhs.config;
}

} /// namespace NES

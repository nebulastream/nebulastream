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
#include <ranges>
#include <sstream>
#include <variant>
#include <Configurations/Descriptor.hpp>

namespace NES::Configurations
{

Descriptor::Descriptor(DescriptorConfig::Config&& config) : config(std::move(config))
{
}

/// Define a ConfigPrinter to generate print functions for all options of the std::variant 'ConfigType'.
struct ConfigPrinter
{
    std::ostream& out;

    template <typename T>
    void operator()(const T& value) const
    {
        if constexpr (!std::is_enum_v<T>)
        {
            out << value;
        }
        else
        {
            out << std::string(magic_enum::enum_name(value));
        }
    }
};

std::ostream& operator<<(std::ostream& out, const DescriptorConfig::Config& config)
{
    if (config.empty())
    {
        return out;
    }
    out << config.begin()->first << ": ";
    std::visit(ConfigPrinter{out}, config.begin()->second);
    for (const auto& [key, value] : config | std::views::drop(1))
    {
        out << ", " << key << ": ";
        std::visit(ConfigPrinter{out}, value);
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const Descriptor& descriptor)
{
    return out << "\nDescriptor( " << descriptor.config << " )";
}

std::string Descriptor::toStringConfig() const
{
    std::stringstream ss;
    ss << this->config;
    return ss.str();
}

}

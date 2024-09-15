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

#include <string>
#include <fmt/std.h>
#include <magic_enum.hpp>

namespace NES::Configurations
{
class EnumWrapper
{
public:
    template <typename EnumType>
    static EnumWrapper create(EnumType enumValue)
    {
        return EnumWrapper(std::string(magic_enum::enum_name<EnumType>(enumValue)));
    }

    static EnumWrapper create(std::string enumValueAsString) { return EnumWrapper(std::move(enumValueAsString)); }

    template <typename EnumType>
    std::optional<EnumType> toEnum() const
    {
        return magic_enum::enum_cast<EnumType>(value);
    }

    std::string_view getValue() const { return value; }

    friend bool operator==(const EnumWrapper& lhs, const EnumWrapper& rhs) { return lhs.value == rhs.value; }

    friend std::ostream& operator<<(std::ostream& out, const EnumWrapper& enumWrapper) { return out << enumWrapper.value; }

private:
    EnumWrapper(std::string&& value) : value(std::move(value)) { }
    std::string value;
};

}
namespace fmt
{
template <>
struct formatter<NES::Configurations::EnumWrapper> : ostream_formatter
{
};
}

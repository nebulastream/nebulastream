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

#include <optional>
#include <string>
#include <utility>
#include <fmt/std.h> ///NOLINT(misc-include-cleaner)
#include <magic_enum/magic_enum.hpp>

namespace NES::Configurations
{

template <typename T>
concept Enum = std::is_enum_v<T>;

/// The EnumWrapper allows to represent an arbitrary Enum as a string, which is beneficial for variants. When defining a variant, all possible
/// types for the variant must be specified. In the case of Enums, this would be every possible Enum. By representing the Enum as a string,
/// all possible Enums can just be represented as a string, also greatly simplifying serialization, by getting rid of switch cases for the
/// different Enum options.
class EnumWrapper
{
public:
    explicit EnumWrapper(Enum auto enumValue) : value(std::string(magic_enum::enum_name(enumValue))) { }

    explicit EnumWrapper(std::string enumValueAsString) : value(std::move(enumValueAsString)) { }

    template <Enum EnumType>
    std::optional<EnumType> asEnum() const
    {
        return magic_enum::enum_cast<EnumType>(value);
    }

    const std::string& getValue() const { return value; }

    friend bool operator==(const EnumWrapper& lhs, const EnumWrapper& rhs) = default;

    friend std::ostream& operator<<(std::ostream& out, const EnumWrapper& enumWrapper) { return out << enumWrapper.value; }

private:
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

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

#include <cstdint>
#include <expected>
#include <string_view>
#include <DataTypes/RuntimeValue.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Input-boundary validation for query constants and per-row options. Logical inference establishes the integer type;
/// preparation checks its value before passing an unsigned option to execution. Generic scalar access remains contract-only.
[[nodiscard]] inline std::expected<uint64_t, Exception>
checkedUnsignedIntegerOption(const RuntimeValueView value, const std::string_view option)
{
    const auto converted = value.tryGetUnsignedInteger();
    if (!converted)
    {
        return std::unexpected(ArithmeticalError("{} expects a non-negative integer", option));
    }
    return *converted;
}

}

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
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

/// Parsing of the session's command lines and config file.

namespace NES::ConnTest
{

std::optional<std::string> slurpFile(const std::filesystem::path& path);

/// `FILL 3 120` -> `FILL`.
std::string_view verbOf(std::string_view line);

/// `FILL 3 120` -> `{3, 120}`.
/// Returns nullopt when an argument is missing or not a plain unsigned integer, so `5x` is rejected, not truncated.
std::optional<std::pair<std::uint64_t, std::uint64_t>> twoArgsOf(std::string_view line);

}

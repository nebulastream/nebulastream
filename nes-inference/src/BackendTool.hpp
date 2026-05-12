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

#include <cstddef>
#include <expected>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace NES
{

struct ToolVersion
{
    size_t major = 0;
    size_t minor = 0;

    bool operator==(const ToolVersion&) const = default;
    auto operator<=>(const ToolVersion&) const = default;
};

std::string format_as(const ToolVersion& version);

/// IREE major.minor must match the ireeruntime port we link against. A mismatch
/// produces VM bytecode with feature bits the runtime cannot decode.
constexpr ToolVersion expectedIreeVersion{.major = 3, .minor = 11};

/// OpenVINO converter/runtime versions must match the vcpkg OpenVINO port.
constexpr ToolVersion expectedOpenVinoVersion{.major = 2025, .minor = 3};

namespace detail
{

struct ToolDiscovery
{
    std::string name;
    std::filesystem::path path;
    std::optional<ToolVersion> version;
};

std::string format_as(const ToolDiscovery& tool);

/// Search $PATH for `name`. Returns nullopt if no executable entry was found.
std::optional<std::filesystem::path> findExecutable(std::string_view name);

/// Locate `name` on $PATH. When `checkVersion` is true, also invoke `--version`
/// and parse the first `MAJOR.MINOR`; an unparseable or missing version is an
/// error. When `checkVersion` is false, the returned `ToolDiscovery.version`
/// is `nullopt` and success depends only on the PATH lookup.
/// Never compares against an expected version — the caller owns that check.
std::expected<ToolDiscovery, std::string> discoverTool(std::string_view name, bool checkVersion);

struct RunResult
{
    int exitCode = -1;
    std::vector<std::byte> stdoutData;
    std::string stderrText;
    std::string errorMessage;
};

/// Execute `exe` with `args`. If `stdinData` is non-empty it is written to the
/// child's stdin and stdin is then closed. stdout is captured as bytes, stderr
/// as text.
RunResult runTool(const std::filesystem::path& exe, std::span<const std::string> args, std::span<const std::byte> stdinData);

}

}

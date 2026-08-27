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

#include <BackendTool.hpp>

#include <cstddef>
#include <cstdlib>
#include <exception>
#include <expected>
#include <filesystem>
#include <optional>
#include <regex>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <unistd.h>
#include <Util/Strings.hpp>
#include <Util/Subprocess.hpp>
#include <fmt/format.h>

namespace NES
{

std::string format_as(const ToolVersion& version)
{
    return fmt::format("{}.{}", version.major, version.minor);
}

namespace detail
{

std::string format_as(const ToolDiscovery& tool)
{
    if (tool.path.empty())
    {
        return fmt::format("{}: Not Found", tool.name);
    }
    if (tool.version.has_value())
    {
        return fmt::format("{}: {}", tool.name, format_as(*tool.version));
    }
    return fmt::format("{}: available", tool.name);
}

std::optional<std::filesystem::path> findExecutable(std::string_view name)
{
    /// NOLINTNEXTLINE(concurrency-mt-unsafe) PATH is read once at tool-discovery time, before any worker threads are running
    const char* pathEnv = std::getenv("PATH");
    if (pathEnv == nullptr)
    {
        return std::nullopt;
    }
    const std::string_view paths{pathEnv};
    size_t start = 0;
    while (start <= paths.size())
    {
        size_t end = paths.find(':', start);
        if (end == std::string_view::npos)
        {
            end = paths.size();
        }
        if (end > start)
        {
            std::filesystem::path candidate = std::filesystem::path(paths.substr(start, end - start)) / std::string(name);
            std::error_code errCode;
            if (std::filesystem::exists(candidate, errCode) && ::access(candidate.c_str(), X_OK) == 0)
            {
                return candidate;
            }
        }
        if (end == paths.size())
        {
            break;
        }
        start = end + 1;
    }
    return std::nullopt;
}

namespace
{

/// Extract the first "MAJOR.MINOR" from a `--version` output, ignoring patch/build suffixes.
std::optional<ToolVersion> extractToolVersion(std::string_view versionOutput)
{
    static const std::regex VersionRegex{R"((\d+)\.(\d+))"};
    std::cmatch match;
    if (!std::regex_search(versionOutput.data(), versionOutput.data() + versionOutput.size(), match, VersionRegex))
    {
        return std::nullopt;
    }
    auto major = NES::from_chars<size_t>(std::string_view{match[1].first, match[1].second});
    auto minor = NES::from_chars<size_t>(std::string_view{match[2].first, match[2].second});
    if (!major.has_value() || !minor.has_value())
    {
        return std::nullopt;
    }
    return ToolVersion{.major = *major, .minor = *minor};
}

}

std::expected<ToolDiscovery, std::string> discoverTool(std::string_view name, bool checkVersion)
{
    auto path = findExecutable(name);
    if (!path.has_value())
    {
        return std::unexpected(fmt::format("{} is not in PATH", name));
    }

    if (!checkVersion)
    {
        return ToolDiscovery{.name = std::string(name), .path = std::move(*path), .version = std::nullopt};
    }

    auto result = runTool(*path, std::vector<std::string>{"--version"}, {});
    if (!result.errorMessage.empty())
    {
        return std::unexpected(fmt::format("could not invoke `{} --version`: {}", name, result.errorMessage));
    }
    if (result.exitCode != 0)
    {
        return std::unexpected(fmt::format("`{} --version` exited with code {}", name, result.exitCode));
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) bytes-to-text for captured stdout
    std::string versionOutput(reinterpret_cast<const char*>(result.stdoutData.data()), result.stdoutData.size());
    auto version = extractToolVersion(versionOutput);
    if (!version.has_value())
    {
        return std::unexpected(fmt::format("could not parse version from `{} --version` output:\n{}", name, versionOutput));
    }

    return ToolDiscovery{.name = std::string(name), .path = std::move(*path), .version = std::move(version)};
}

RunResult runTool(const std::filesystem::path& exe, std::span<const std::string> args, std::span<const std::byte> stdinData)
{
    RunResult result;

    std::vector<std::string> argv;
    argv.reserve(args.size() + 1);
    argv.emplace_back(exe.string());
    for (const auto& arg : args)
    {
        argv.emplace_back(arg);
    }

    try
    {
        subprocess::Popen proc(
            argv, subprocess::input{subprocess::PIPE}, subprocess::output{subprocess::PIPE}, subprocess::error{subprocess::PIPE});

        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-char for subprocess input
        const auto* inputPtr = reinterpret_cast<const char*>(stdinData.data());
        auto [out, err] = proc.communicate(inputPtr, stdinData.size());

        result.exitCode = proc.retcode();

        if (out.length > 0)
        {
            const auto bytes = std::as_bytes(std::span{out.buf}.first(out.length));
            result.stdoutData.assign(bytes.begin(), bytes.end());
        }
        if (err.length > 0)
        {
            result.stderrText.assign(err.buf.data(), err.length);
        }
    }
    catch (const subprocess::CalledProcessError& e)
    {
        result.errorMessage = e.what();
    }
    catch (const subprocess::OSError& e)
    {
        result.errorMessage = e.what();
    }
    catch (const std::exception& e)
    {
        result.errorMessage = e.what();
    }

    return result;
}

}

}

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

#include <Version.hpp>

#include <Util/VersionPluginList.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <BuildInfo.hpp>

#include <algorithm>
#include <cstddef>
#include <iostream>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Formats the plugins linked into this binary as one comma-separated line per registry kind. The registries announce
/// themselves via VersionPluginList, so the listing reflects exactly what is compiled into the current binary.
std::string formatPluginList()
{
    const auto pluginsByKind = VersionPluginList::instance().list();
    if (pluginsByKind.empty())
    {
        return "  plugins:        (none)\n";
    }
    /// Align the plugin names by padding each kind label to the longest one.
    std::size_t longestKind = 0;
    for (const auto& [kind, names] : pluginsByKind)
    {
        longestKind = std::max(longestKind, kind.size());
    }
    std::string formatted = "  plugins:\n";
    for (const auto& [kind, names] : pluginsByKind)
    {
        formatted += fmt::format("    {:<{}} {}\n", kind + ":", longestKind + 1, fmt::join(names, ", "));
    }
    return formatted;
}
}

std::string formatVersion(const std::string_view binaryName)
{
    return fmt::format(
        "{} {}\n"
        "  commit:         {}\n"
        "  built:          {}\n"
        "  build:          {}\n"
        "  sanitizer:      {}\n"
        "  compiler:       {}\n"
        "  compiler flags: {}\n"
        "  stdlib:         {}\n"
        "  log level:      {}\n"
        "  vcpkg baseline: {}\n"
        "{}",
        binaryName,
        BuildInfo::version,
        BuildInfo::gitCommit,
        BuildInfo::timestamp,
        BuildInfo::buildType,
        BuildInfo::sanitizer,
        BuildInfo::compiler,
        BuildInfo::compilerFlags,
        BuildInfo::stdlib,
        BuildInfo::logLevel,
        BuildInfo::vcpkgBaseline,
        formatPluginList());
}

void printVersion(const std::string_view binaryName)
{
    std::cout << formatVersion(binaryName);
}

bool hasVersionFlag(const int argc, const char* const* argv)
{
    PRECONDITION(argc >= 0, "argc must be non-negative");
    const bool argvPresentOrEmpty = argv != nullptr || argc == 0;
    PRECONDITION(argvPresentOrEmpty, "argc must be zero when argv is null");

    auto args = std::span{argv, static_cast<size_t>(argc)} | std::views::filter([](const auto* arg) { return arg != nullptr; })
        | std::views::transform([](const auto* arg) { return std::string_view{arg}; });
    return std::ranges::any_of(args, [](const std::string_view argView) { return argView == "-v" || argView == "--version"; });
}

}

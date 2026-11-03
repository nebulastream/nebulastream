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

#include <UdfBridgeRegistry.hpp>

#include <filesystem>
#include <string>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

UdfBridgeRegistry& UdfBridgeRegistry::instance()
{
    static UdfBridgeRegistry inst;
    return inst;
}

std::filesystem::path currentExecutableDirectory()
{
    return std::filesystem::read_symlink("/proc/self/exe").parent_path();
}

std::filesystem::path resolveUdfBridgeLibraryPath(const UdfLanguage language, const UdfExecution execution)
{
    const auto& registry = UdfBridgeRegistry::instance();
    if (const auto create = registry.find(fmt::format("{}{}", magic_enum::enum_name(language), magic_enum::enum_name(execution))))
    {
        const auto bridge = (*create)();
        return bridge.libraryFileName.empty() ? std::filesystem::path{}
                                              : currentExecutableDirectory() / "nes-udf-bridges" / bridge.libraryFileName;
    }
    const auto shipped = registry.getRegisteredNames();
    if (shipped.empty())
    {
        throw PythonUdfsNotEnabled(
            "{} execution of {} UDFs is not available: this build has no support for Python UDFs (configure with "
            "-DNES_ENABLE_PYTHON_UDFS=ON)",
            magic_enum::enum_name(execution),
            magic_enum::enum_name(language));
    }
    throw UnsupportedUdfLanguage(
        "{} execution of {} UDFs is not available in this build (available: {})",
        magic_enum::enum_name(execution),
        magic_enum::enum_name(language),
        fmt::join(shipped, ", "));
}

}

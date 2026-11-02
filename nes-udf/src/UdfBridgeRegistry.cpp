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
#include <string_view>
#include <fmt/ranges.h>
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

UdfBridge resolveUdfBridge(const std::string_view bridge)
{
    const auto& registry = UdfBridgeRegistry::instance();
    if (const auto create = registry.find(std::string{bridge}))
    {
        return (*create)();
    }
    const auto shipped = registry.getRegisteredNames();
    if (shipped.empty())
    {
        throw PythonUdfsNotEnabled(
            "BRIDGE '{}' is not available: this build has no support for Python UDFs (configure with -DNES_ENABLE_PYTHON_UDFS=ON)", bridge);
    }
    throw UnsupportedUdfLanguage("'{}' is not a UDF bridge of this build (available: {})", bridge, fmt::join(shipped, ", "));
}

std::filesystem::path resolveUdfBridgeLibraryPath(const UdfBridge& bridge)
{
    INVARIANT(!bridge.libraryFileName.empty(), "A bridge compiled into the query pipeline has no library");
    return currentExecutableDirectory() / "nes-udf-bridges" / bridge.libraryFileName;
}

}

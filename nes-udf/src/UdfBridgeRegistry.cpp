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
#include <string_view>
#include <unordered_map>

#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Bridge name -> shipped bridge filename, relative to <currentExecutableDirectory()>/nes-udf-bridges/.
/// Whether it was actually built is a file-existence check left to UdfCatalog::registerUdf.
const std::unordered_map<std::string_view, std::string_view> kBuiltinBridges{
    {"cpython", "libnes-cpython-udf-bridge.so"},
    {"pypy", "libnes-pypy-udf-bridge.so"},
};
}

std::filesystem::path currentExecutableDirectory()
{
    return std::filesystem::read_symlink("/proc/self/exe").parent_path();
}

std::filesystem::path resolveBuiltinUdfBridgePath(const std::string_view bridge)
{
    const auto it = kBuiltinBridges.find(bridge);
    if (it == kBuiltinBridges.end())
    {
        throw NES::UnsupportedUdfLanguage("'{}' is not a built-in UDF bridge", bridge);
    }
    return currentExecutableDirectory() / "nes-udf-bridges" / it->second;
}

}

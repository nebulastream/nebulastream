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
/// Language name -> shipped bridge filename, relative to <currentExecutableDirectory()>/nes-udf-bridges/.
/// Deliberately unconditional and compile-time-known: whether the bridge was actually built into this
/// deployment (e.g. Python's Development.Embed wasn't found) is a file-existence question, left to
/// UdfCatalog::registerUdf's existing check -- not this table.
const std::unordered_map<std::string_view, std::string_view> kBuiltinBridges{
    {"python", "libnes-python-udf-bridge.so"},
};
}

std::filesystem::path currentExecutableDirectory()
{
    return std::filesystem::read_symlink("/proc/self/exe").parent_path();
}

std::filesystem::path resolveBuiltinUdfBridgePath(const std::string_view language)
{
    const auto it = kBuiltinBridges.find(language);
    if (it == kBuiltinBridges.end())
    {
        throw NES::UnsupportedUdfLanguage("'{}' is not a built-in UDF language", language);
    }
    return currentExecutableDirectory() / "nes-udf-bridges" / it->second;
}

}

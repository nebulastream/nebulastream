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

#include <filesystem>
#include <functional>
#include <string>
#include <string_view>
#include <Util/RuntimeRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

/// BRIDGE value selecting Codon (ahead-of-time compiled into the query pipeline). Unlike the interpreter bridges it has
/// no shipped `.so`.
inline constexpr std::string_view CodonUdfBridge = "codon";

/// What a `BRIDGE` clause value selects.
struct UdfBridge
{
    UdfExecution execution;
    /// File name of the bridge library shipped in <executable dir>/nes-udf-bridges/. Empty for a bridge that is compiled
    /// into the query pipeline (Codon).
    std::string libraryFileName;
};

using UdfBridgeRegistryReturnType = UdfBridge;
using UdfBridgeFn = std::function<UdfBridgeRegistryReturnType()>;

/// The bridges this build ships, keyed by their BRIDGE name. All of them are Python bridges registered by nes-python-udf,
/// so the registry is empty in a build without Python UDFs.
class UdfBridgeRegistry : public RuntimeRegistry<UdfBridgeRegistry, std::string, UdfBridgeFn, /*CaseSensitive*/ false>
{
public:
    static UdfBridgeRegistry& instance();
};

/// Directory containing the running executable (resolved via /proc/self/exe on Linux). Anchor for
/// locating shipped UDF bridge `.so` files at a fixed, deployment-relative path.
[[nodiscard]] std::filesystem::path currentExecutableDirectory();

/// Resolves a `BRIDGE` clause value. Throws NES::PythonUdfsNotEnabled if this build ships no bridge at all and
/// NES::UnsupportedUdfLanguage if `bridge` names none of the shipped ones.
[[nodiscard]] UdfBridge resolveUdfBridge(std::string_view bridge);

/// Where the shipped library of an interpreter bridge lives. Doesn't check the file exists: the worker reports a missing
/// bridge when it loads it, the same way as a bad FROM path.
[[nodiscard]] std::filesystem::path resolveUdfBridgeLibraryPath(const UdfBridge& bridge);

}

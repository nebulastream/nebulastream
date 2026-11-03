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
#include <Util/RuntimeRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

/// A way to execute UDFs of one language that this build ships.
struct UdfBridge
{
    /// File name of the bridge library shipped in <executable dir>/nes-udf-bridges/. Empty if the UDF is compiled into
    /// the query pipeline (Inline).
    std::string libraryFileName;
};

using UdfBridgeRegistryReturnType = UdfBridge;
using UdfBridgeFn = std::function<UdfBridgeRegistryReturnType()>;

/// The bridges this build ships, keyed by language and execution, e.g. "PythonInProcess". All of them are registered by
/// nes-python-udf, so the registry is empty in a build without Python UDFs.
class UdfBridgeRegistry : public RuntimeRegistry<UdfBridgeRegistry, std::string, UdfBridgeFn, /*CaseSensitive*/ false>
{
public:
    static UdfBridgeRegistry& instance();
};

/// Directory containing the running executable (resolved via /proc/self/exe on Linux). Anchor for
/// locating shipped UDF bridge `.so` files at a fixed, deployment-relative path.
[[nodiscard]] std::filesystem::path currentExecutableDirectory();

/// Where the shipped library that runs UDFs of this language and execution lives; empty if the UDF is compiled into the
/// query pipeline. Throws NES::PythonUdfsNotEnabled if this build ships no bridge at all and NES::UnsupportedUdfLanguage if
/// it ships none for this combination. Doesn't check the file exists: the worker reports a missing bridge when it loads it.
[[nodiscard]] std::filesystem::path resolveUdfBridgeLibraryPath(UdfLanguage language, UdfExecution execution);

}

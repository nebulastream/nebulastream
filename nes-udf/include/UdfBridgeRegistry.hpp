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
#include <string_view>

namespace NES
{

/// Directory containing the running executable (resolved via /proc/self/exe on Linux). Anchor for
/// locating shipped UDF bridge `.so` files at a fixed, deployment-relative path.
[[nodiscard]] std::filesystem::path currentExecutableDirectory();

/// Resolves a `BRIDGE` clause value (e.g. "python") to the shipped bridge path. Doesn't check the file
/// exists -- UdfCatalog::registerUdf does, so a missing bridge and a bad FROM path fail the same way.
/// Throws NES::UnsupportedUdfLanguage if `bridge` names no built-in bridge.
[[nodiscard]] std::filesystem::path resolveBuiltinUdfBridgePath(std::string_view bridge);

}

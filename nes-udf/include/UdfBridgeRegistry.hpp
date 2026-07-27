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

/// The directory containing the currently running executable (Linux: resolved via /proc/self/exe).
/// Used as the anchor for locating shipped UDF bridge `.so` files at a fixed, deployment-relative
/// location, so resolution works identically for a dev build, an installed package, or a Docker image
/// -- unlike a path baked in at compile time from the build tree.
[[nodiscard]] std::filesystem::path currentExecutableDirectory();

/// Resolves a `LANGUAGE` clause value (e.g. "python") to the path where NES ships that language's
/// bridge in this deployment: `<currentExecutableDirectory()>/nes-udf-bridges/<bridge filename>`.
/// Does NOT check whether the file actually exists there -- that stays UdfCatalog::registerUdf's job,
/// so "bridge not built into this deployment" and "bad FROM path" produce one consistent error.
/// Throws NES::UnsupportedUdfLanguage if `language` names no built-in bridge.
[[nodiscard]] std::filesystem::path resolveBuiltinUdfBridgePath(std::string_view language);

}

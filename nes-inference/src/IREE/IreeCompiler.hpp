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

#include <expected>
#include <filesystem>
#include <optional>

#include <BackendTool.hpp>
#include <Inference.hpp>
#include <Model.hpp>

namespace NES
{

/// Wraps the `iree-compile` tool. Internal — exposed to consumers through the
/// free function `compileIreeModel` in `Inference.hpp`.
class IreeCompiler
{
public:
    IreeCompiler();

    [[nodiscard]] bool available() const
    {
        return !discovery.path.empty() && discovery.version.has_value() && *discovery.version == expectedIreeVersion;
    }

    [[nodiscard]] const std::filesystem::path& path() const { return discovery.path; }

    [[nodiscard]] std::optional<ToolVersion> version() const { return discovery.version; }

    [[nodiscard]] std::expected<CompiledModel, CompileError> compile(const ImportedModel& imported) const;

private:
    detail::ToolDiscovery discovery;
};

}

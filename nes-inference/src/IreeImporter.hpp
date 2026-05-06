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

#include <Inference.hpp>
#include <IreeTool.hpp>
#include <Model.hpp>

namespace NES
{

/// Wraps the `iree-import-onnx` tool. Internal — exposed to consumers through
/// the free function `importOnnxModel` in `Inference.hpp`.
class IreeImporter
{
public:
    IreeImporter();

    [[nodiscard]] bool available() const { return !discovery.path.empty(); }

    [[nodiscard]] const std::filesystem::path& path() const { return discovery.path; }

    [[nodiscard]] std::expected<ImportedModel, ImportError> importOnnx(const std::filesystem::path& onnxPath) const;

private:
    detail::ToolDiscovery discovery;
};

}

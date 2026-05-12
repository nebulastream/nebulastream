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

#include <array>
#include <expected>
#include <filesystem>
#include <string_view>

#include <BackendTool.hpp>
#include <Inference.hpp>
#include <Model.hpp>

namespace NES
{

class OpenVinoImporter
{
public:
    OpenVinoImporter();

    [[nodiscard]] bool available() const
    {
        return !discovery.path.empty() && discovery.version.has_value() && *discovery.version == expectedOpenVinoVersion;
    }

    [[nodiscard]] std::expected<ImportedModel, ImportError> importModel(const std::filesystem::path& modelPath) const;

private:
    static constexpr std::array<std::string_view, 7> SupportedExtensions{".onnx", ".pb", ".pbtxt", ".meta", ".tflite", ".pdmodel", ".pt2"};

    [[nodiscard]] static bool isSupportedInput(const std::filesystem::path& modelPath);

    detail::ToolDiscovery discovery;
};

}

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

#include <cstddef>
#include <expected>
#include <string>
#include <string_view>
#include <vector>

namespace NES
{

struct ModelSignature
{
    std::string functionName;
    std::vector<size_t> inputShape;
    std::vector<size_t> outputShape;

    bool operator==(const ModelSignature&) const = default;
};

/// Extracts the model signature (function name, input/output shapes) from
/// MLIR textual form as produced by `iree-import-onnx`. Purely textual
/// analysis — does not invoke any external tool.
class MlirAnalyzer
{
public:
    /// Parse the primary `func.func` signature. Expects a single
    /// `!torch.vtensor<[...],f32>` input and output as produced by the ONNX importer.
    /// Returns an error if the MLIR is missing the expected structure or uses
    /// an unsupported element type.
    [[nodiscard]] std::expected<ModelSignature, std::string> analyze(std::string_view mlir) const;
};

}

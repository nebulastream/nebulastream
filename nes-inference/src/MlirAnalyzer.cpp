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

#include <MlirAnalyzer.hpp>

#include <cstddef>
#include <expected>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <Util/Strings.hpp>
#include <fmt/format.h>

namespace NES
{

namespace
{

std::optional<std::vector<size_t>> parseShape(std::string_view shapeCsv)
{
    std::vector<size_t> shape;
    if (shapeCsv.empty())
    {
        return shape;
    }
    size_t start = 0;
    while (start <= shapeCsv.size())
    {
        size_t end = shapeCsv.find(',', start);
        if (end == std::string_view::npos)
        {
            end = shapeCsv.size();
        }
        const auto dim = shapeCsv.substr(start, end - start);
        if (dim.empty())
        {
            return std::nullopt;
        }
        if (dim == "?")
        {
            shape.push_back(1);
        }
        else
        {
            auto value = NES::from_chars<size_t>(dim);
            if (!value.has_value())
            {
                return std::nullopt;
            }
            shape.push_back(*value);
        }
        if (end == shapeCsv.size())
        {
            break;
        }
        start = end + 1;
    }
    return shape;
}

std::optional<TensorElementType> parseTensorElementType(std::string_view type)
{
    if (type == "f32")
    {
        return TensorElementType::FLOAT32;
    }
    if (type == "ui8" || type == "u8")
    {
        return TensorElementType::UINT8;
    }
    if (type == "i64" || type == "si64")
    {
        return TensorElementType::INT64;
    }
    return std::nullopt;
}

}

///NOLINTNEXTLINE(readability-convert-member-functions-to-static) hide implementation detail
std::expected<ModelSignature, std::string> MlirAnalyzer::analyze(std::string_view mlir) const
{
    /// Matches `func.func @name(%arg : !torch.vtensor<[1,100],f32>) -> !torch.vtensor<[1,100],f32>`
    /// as emitted by iree-import-onnx.
    static const std::regex SignatureRegex{
        R"(func\.func\s+@([a-zA-Z_][a-zA-Z0-9_]*)\s*\(\s*%[a-zA-Z0-9_]+\s*:\s*!torch\.vtensor<\[([0-9,?]*)\],([a-z][a-z0-9]+)>\s*\)\s*->\s*!torch\.vtensor<\[([0-9,?]*)\],([a-z][a-z0-9]+)>)"};

    std::cmatch match;
    if (!std::regex_search(mlir.data(), mlir.data() + mlir.size(), match, SignatureRegex))
    {
        return std::unexpected(std::string{"could not find func.func signature in MLIR"});
    }

    const auto inputElementType = parseTensorElementType(match[3].str());
    const auto outputElementType = parseTensorElementType(match[5].str());
    if (!inputElementType.has_value() || !outputElementType.has_value())
    {
        return std::unexpected(
            fmt::format("unsupported tensor element types: input={}, output={}", match[3].str(), match[5].str()));
    }

    auto inputShape = parseShape(std::string_view{match[2].first, match[2].second});
    auto outputShape = parseShape(std::string_view{match[4].first, match[4].second});
    if (!inputShape.has_value() || !outputShape.has_value())
    {
        return std::unexpected(std::string{"failed to parse tensor shape"});
    }
    if (inputShape->empty() || outputShape->empty())
    {
        return std::unexpected(std::string{"tensor shape is empty"});
    }

    /// IREE runtime looks up entry points by their fully qualified name `module.<name>`.
    return ModelSignature{
        .functionName = "module." + match[1].str(),
        .inputShape = std::move(*inputShape),
        .outputShape = std::move(*outputShape),
        .inputElementType = *inputElementType,
        .outputElementType = *outputElementType};
}

}

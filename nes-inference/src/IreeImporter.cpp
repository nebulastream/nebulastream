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

#include <IreeImporter.hpp>

#include <expected>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Util/Logger/Logger.hpp>
#include <Inference.hpp>
#include <IreeTool.hpp>
#include <MlirAnalyzer.hpp>
#include <Model.hpp>
#include <ModelAccess.hpp>

namespace NES
{

IreeImporter::IreeImporter()
{
    discovery.name = "iree-import-onnx";
    auto found = detail::discoverTool(discovery.name, /*checkVersion*/ false);
    if (!found.has_value())
    {
        NES_WARNING("{}: {}", discovery.name, found.error());
        return;
    }
    discovery = std::move(*found);
    NES_INFO("{}", detail::format_as(discovery));
}

std::expected<ImportedModel, ImportError> IreeImporter::importOnnx(const std::filesystem::path& onnxPath) const
{
    if (!available())
    {
        return std::unexpected(ImportError{"iree-import-onnx is not available"});
    }
    if (onnxPath.filename().extension() != ".onnx")
    {
        return std::unexpected(ImportError{"Loading does only support `.onnx` models at the moment"});
    }

    const std::vector<std::string> args{onnxPath.string()};
    auto result = detail::runTool(discovery.path, args, {});
    if (!result.errorMessage.empty())
    {
        NES_ERROR("iree-import-onnx launch error: {}", result.errorMessage);
        return std::unexpected(ImportError{fmt::format("Model import failed: {}", result.errorMessage)});
    }
    if (result.exitCode != 0)
    {
        NES_ERROR("iree-import-onnx error:\n{} {}\n```\n{}```", discovery.path.string(), fmt::join(args, " "), result.stderrText);
        return std::unexpected(ImportError{"Model import was not successful: Non Zero Exit Code."});
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-char for MLIR text
    const std::string_view mlir{reinterpret_cast<const char*>(result.stdoutData.data()), result.stdoutData.size()};

    /// Scrape the signature (function name, shapes) out of the MLIR text so
    /// downstream consumers — including the runtime session — have everything
    /// they need without re-parsing.
    auto signature = MlirAnalyzer{}.analyze(mlir);
    if (!signature.has_value())
    {
        return std::unexpected(ImportError{"could not analyze MLIR: " + signature.error()});
    }

    return detail::ModelAccess::makeImported(
        detail::RefCountedByteBuffer::fromBytes(result.stdoutData),
        std::move(signature->functionName),
        std::move(signature->inputShape),
        std::move(signature->outputShape),
        signature->inputElementType,
        signature->outputElementType);
}

}

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

#include <OpenVinoImporter.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <expected>
#include <filesystem>
#include <fstream>
#include <ios>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <openvino/core/node_output.hpp>
#include <openvino/core/partial_shape.hpp>
#include <openvino/core/type/element_type.hpp>
#include <scope_guard.hpp>

#include <Util/Logger/Logger.hpp>
#include <openvino/runtime/core.hpp>
#include <openvino/runtime/tensor.hpp>
#include <BackendTool.hpp>
#include <Inference.hpp>
#include <Model.hpp>
#include <ModelAccess.hpp>

namespace NES
{

namespace
{

std::filesystem::path makeTemporaryDirectory()
{
    static std::atomic_uint64_t counter = 0;
    const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
    auto path = std::filesystem::temp_directory_path() / fmt::format("nes-openvino-import-{}-{}", ticks, counter.fetch_add(1));
    std::filesystem::create_directories(path);
    return path;
}

std::expected<std::vector<std::byte>, ImportError> readBinaryFile(const std::filesystem::path& filePath)
{
    std::ifstream file(filePath, std::ios::binary);
    if (!file.good())
    {
        return std::unexpected(ImportError{fmt::format("Could not open `{}`", filePath.string())});
    }

    file.seekg(0, std::ios::end);
    const auto size = static_cast<size_t>(file.tellg());
    file.seekg(0, std::ios::beg);

    std::vector<std::byte> bytes(size);
    if (size > 0)
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte buffer file read
        file.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(size));
    }
    return bytes;
}

std::string lowerExtension(const std::filesystem::path& path)
{
    auto extension = path.extension().string();
    std::ranges::transform(
        extension, extension.begin(), [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
    return extension;
}

std::string outputStemFor(const std::filesystem::path& modelPath)
{
    auto stem = modelPath.stem().string();
    if (!stem.empty())
    {
        return stem;
    }
    return "model";
}

std::expected<std::vector<size_t>, ImportError> shapeFromPartial(const ov::PartialShape& partialShape, std::string_view role)
{
    if (partialShape.rank().is_dynamic())
    {
        return std::unexpected(ImportError{fmt::format("OpenVINO model {} tensor rank is dynamic", role)});
    }

    /// A dynamic dimension is resolved to 1: inference feeds the model one tuple at a
    /// time, so a dynamic batch dimension is exactly the case that works. Warn rather
    /// than reject, so a stock model does not have to be re-exported to be usable.
    /// For a FLOAT32 schema `ModelCatalog::registerModel` still rejects the model if the
    /// resolved shape does not match the field count the user declared.
    std::vector<size_t> shape;
    shape.reserve(partialShape.size());
    for (size_t index = 0; index < partialShape.size(); ++index)
    {
        const auto& dimension = partialShape[index];
        if (dimension.is_dynamic() || dimension.get_length() < 0)
        {
            NES_WARNING(
                "OpenVINO model {} tensor has a dynamic dimension at index {} (shape {}); assuming an extent of 1",
                role,
                index,
                partialShape.to_string());
            shape.push_back(1);
            continue;
        }
        shape.push_back(static_cast<size_t>(dimension.get_length()));
    }

    if (shape.empty())
    {
        return std::unexpected(ImportError{fmt::format("OpenVINO model {} tensor shape is empty", role)});
    }
    return shape;
}

/// With rank >= 2 the leading dimension is the batch. A fixed batch above 1 means the
/// model wants several samples per invocation, which inference cannot provide: it runs
/// the model once per tuple. Such a model would silently treat one tuple's fields as a
/// whole batch, so reject it. Rank 1 has no batch dimension to speak of, and a dynamic
/// batch was already resolved to 1 above.
std::expected<void, ImportError> validateBatchDimension(const std::vector<size_t>& shape)
{
    if (shape.size() >= 2 && shape.front() > 1)
    {
        return std::unexpected(ImportError{fmt::format(
            "OpenVINO model input tensor has a fixed batch dimension of {} (shape [{}]). Model inference evaluates one tuple at a "
            "time, so the leading dimension must be 1 or dynamic.",
            shape.front(),
            fmt::join(shape, ", "))});
    }
    return {};
}

template <typename NodeType>
std::expected<void, ImportError> validateF32Tensor(const ov::Output<NodeType>& tensor, std::string_view role)
{
    if (tensor.get_element_type() != ov::element::f32)
    {
        return std::unexpected(
            ImportError{fmt::format("OpenVINO model {} tensor must be f32, got {}", role, tensor.get_element_type().get_type_name())});
    }
    return {};
}

}

OpenVinoImporter::OpenVinoImporter()
{
    discovery.name = "ovc";
    auto found = detail::discoverTool(discovery.name, /*checkVersion*/ true);
    if (!found.has_value())
    {
        NES_WARNING("{}: {}", discovery.name, found.error());
        return;
    }
    discovery = std::move(*found);
    if (!discovery.version.has_value())
    {
        return;
    }
    if (const auto& version = discovery.version.value(); version != expectedOpenVinoVersion)
    {
        NES_WARNING("{}: version mismatch (got {}, expected {})", discovery.name, format_as(version), format_as(expectedOpenVinoVersion));
        return;
    }
    NES_INFO("{}", detail::format_as(discovery));
}

bool OpenVinoImporter::isSupportedInput(const std::filesystem::path& modelPath)
{
    /// SavedModel inputs are directories, and only those may come without an extension.
    /// An extension-less path that is not a directory is far more likely to be a typo
    /// (`/data/iris` for `/data/iris.onnx`) and deserves the friendly error below rather
    /// than an opaque `ovc` conversion failure.
    if (modelPath.filename().empty() || modelPath.extension().empty())
    {
        std::error_code error;
        return std::filesystem::is_directory(modelPath, error);
    }

    const auto extension = lowerExtension(modelPath);
    return std::ranges::contains(SupportedExtensions, extension);
}

std::expected<ImportedModel, ImportError> OpenVinoImporter::importModel(const std::filesystem::path& modelPath) const
{
    if (!available())
    {
        return std::unexpected(ImportError{"ovc is not available"});
    }
    if (!isSupportedInput(modelPath))
    {
        return std::unexpected(
            ImportError{"OpenVINO loading supports .onnx, .pb, .pbtxt, .meta, .tflite, .pdmodel, .pt2, or a SavedModel directory"});
    }

    const auto tempDir = makeTemporaryDirectory();
    SCOPE_EXIT
    {
        std::error_code error;
        std::filesystem::remove_all(tempDir, error);
        if (error)
        {
            NES_WARNING("Failed to remove temporary OpenVINO import directory {}: {}", tempDir.string(), error.message());
        }
    };

    const auto outputXmlPath = tempDir / fmt::format("{}.xml", outputStemFor(modelPath));
    const std::vector<std::string> args{modelPath.string(), "--compress_to_fp16=False", "--output_model", outputXmlPath.string()};
    auto result = detail::runTool(discovery.path, args, {});
    if (!result.errorMessage.empty())
    {
        NES_ERROR("ovc launch error: {}", result.errorMessage);
        return std::unexpected(ImportError{fmt::format("OpenVINO model conversion failed: {}", result.errorMessage)});
    }
    if (result.exitCode != 0)
    {
        NES_ERROR("ovc error:\n{} {}\n```\n{}```", discovery.path.string(), fmt::join(args, " "), result.stderrText);
        return std::unexpected(ImportError{"OpenVINO model conversion was not successful: Non Zero Exit Code."});
    }

    auto binPath = outputXmlPath;
    binPath.replace_extension(".bin");
    if (!std::filesystem::exists(outputXmlPath) || !std::filesystem::exists(binPath))
    {
        return std::unexpected(
            ImportError{fmt::format("OpenVINO conversion did not produce both XML and BIN files under `{}`", tempDir.string())});
    }

    auto xmlBytes = readBinaryFile(outputXmlPath);
    if (!xmlBytes)
    {
        return std::unexpected(xmlBytes.error());
    }
    auto binBytes = readBinaryFile(binPath);
    if (!binBytes)
    {
        return std::unexpected(binBytes.error());
    }

    try
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-text for OpenVINO XML payload
        const std::string xmlContent(reinterpret_cast<const char*>(xmlBytes->data()), xmlBytes->size());
        std::vector<std::uint8_t> weights(binBytes->size());
        std::ranges::transform(*binBytes, weights.begin(), [](std::byte value) { return static_cast<std::uint8_t>(value); });

        const ov::Core core;
        ov::Tensor weightsTensor(ov::element::u8, {weights.size()});
        if (!weights.empty())
        {
            std::memcpy(weightsTensor.data<std::uint8_t>(), weights.data(), weights.size());
        }
        auto model = core.read_model(xmlContent, weightsTensor);
        if (model->inputs().size() != 1 || model->outputs().size() != 1)
        {
            return std::unexpected(ImportError{"OpenVINO inference supports exactly one model input and one model output"});
        }

        const auto input = model->input(0);
        const auto output = model->output(0);
        if (auto valid = validateF32Tensor(input, "input"); !valid)
        {
            return std::unexpected(valid.error());
        }
        if (auto valid = validateF32Tensor(output, "output"); !valid)
        {
            return std::unexpected(valid.error());
        }

        auto inputShape = shapeFromPartial(input.get_partial_shape(), "input");
        if (!inputShape)
        {
            return std::unexpected(inputShape.error());
        }
        /// Only the input side decides how many samples one invocation covers. An output
        /// whose leading dimension is larger than one is a per-sample result (detection
        /// boxes, say), not a batch, so it is left alone.
        if (auto validBatch = validateBatchDimension(*inputShape); !validBatch)
        {
            return std::unexpected(validBatch.error());
        }
        auto outputShape = shapeFromPartial(output.get_partial_shape(), "output");
        if (!outputShape)
        {
            return std::unexpected(outputShape.error());
        }

        return detail::ModelAccess::makeImported(
            detail::RefCountedByteBuffer::fromBytes(*xmlBytes),
            detail::RefCountedByteBuffer::fromBytes(*binBytes),
            {},
            std::move(*inputShape),
            std::move(*outputShape));
    }
    catch (const std::exception& exception)
    {
        return std::unexpected(ImportError{fmt::format("Failed to analyze OpenVINO model: {}", exception.what())});
    }
}

}

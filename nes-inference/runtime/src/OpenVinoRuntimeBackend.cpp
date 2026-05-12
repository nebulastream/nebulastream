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

#include <OpenVinoRuntimeBackend.hpp>

#include <Util/Logger/Logger.hpp>
#include <Model.hpp>
#include <RuntimeBackend.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ios>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>
#include <openvino/core/shape.hpp>
#include <openvino/core/type/element_type.hpp>
#include <openvino/core/type/float16.hpp>
#include <openvino/runtime/core.hpp>
#include <openvino/runtime/properties.hpp>
#include <openvino/runtime/tensor.hpp>

namespace
{
std::string shapeToString(const ov::Shape& shape)
{
    std::ostringstream stream;
    stream << "[";
    for (size_t i = 0; i < shape.size(); ++i)
    {
        if (i > 0)
        {
            stream << ", ";
        }
        stream << shape.at(i);
    }
    stream << "]";
    return stream.str();
}

template <typename T>
void appendTensorValues(std::ostringstream& stream, const ov::Tensor& tensor)
{
    constexpr auto debugMargin = std::streamoff{64};
    const auto* data = tensor.data<const T>();
    const auto numberOfValues = tensor.get_size();
    for (size_t i = 0; i < numberOfValues; ++i)
    {
        if (i > 0)
        {
            stream << ", ";
        }

        if constexpr (std::is_same_v<T, uint8_t> || std::is_same_v<T, int8_t>)
        {
            stream << static_cast<int>(data[i]);
        }
        else if constexpr (std::is_same_v<T, bool>)
        {
            stream << (data[i] ? "true" : "false");
        }
        else if constexpr (std::is_same_v<T, ov::float16>)
        {
            stream << static_cast<float>(data[i]);
        }
        else
        {
            stream << data[i];
        }

        if (stream.tellp() >= static_cast<std::streamoff>(4096) - debugMargin)
        {
            stream << ", ...";
            return;
        }
    }
}

std::string formatTensor(const ov::Tensor& tensor)
{
    std::ostringstream stream;
    stream << "shape=" << shapeToString(tensor.get_shape()) << ", type=" << tensor.get_element_type().get_type_name() << ", values=[";

    const auto elementType = tensor.get_element_type();
    appendTensorValues<float>(stream, tensor);

    stream << "]";
    auto result = stream.str();
    if (result.size() > 4096)
    {
        result.resize(4096 - 3);
        result += "...";
    }
    return result;
}
}

namespace NES
{
RuntimeMetadata OpenVinoRuntimeBackend::setup(const CompiledModel& model)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-text for OpenVINO XML payload
    const std::string modelXml(reinterpret_cast<const char*>(model.getData().data()), model.getData().size());
    std::vector<std::uint8_t> modelBin(model.getAuxiliaryData().size());
    std::ranges::transform(model.getAuxiliaryData(), modelBin.begin(), [](std::byte value) { return static_cast<std::uint8_t>(value); });

    static ov::Core sharedCore;
    static std::mutex coreMutex;
    const ov::Shape modelInputShape(model.getInputShape().begin(), model.getInputShape().end());
    ov::Tensor weights(ov::element::u8, {modelBin.size()});
    if (!modelBin.empty())
    {
        std::memcpy(weights.data<std::uint8_t>(), modelBin.data(), modelBin.size());
    }

    const std::scoped_lock lock(coreMutex);
    auto openVinoModel = sharedCore.read_model(modelXml, weights);
    openVinoModel->reshape(modelInputShape);
    auto compiledModel = sharedCore.compile_model(
        openVinoModel,
        "CPU",
        ov::hint::execution_mode(ov::hint::ExecutionMode::ACCURACY),
        ov::hint::performance_mode(ov::hint::PerformanceMode::LATENCY));

    inferRequest = compiledModel.create_infer_request();
    inputElementType = compiledModel.input(0).get_element_type();
    inputShape = compiledModel.input(0).get_shape();
    outputElementType = compiledModel.output(0).get_element_type();
    outputShape = compiledModel.output(0).get_shape();

    return RuntimeMetadata{
        .inputShape = model.getInputShape(),
        .nDim = model.getNDim(),
        .functionName = model.getFunctionName(),
        .inputSize = model.inputSize(),
        .outputSize = model.outputSize()};
}

void OpenVinoRuntimeBackend::infer(std::byte* inputBuffer, size_t, std::byte* outputBuffer, size_t /*outputBufferSize*/)
{
    const ov::Tensor inputTensor(inputElementType, inputShape, inputBuffer);
    inferRequest.set_input_tensor(inputTensor);

    NES_DEBUG("Model input: {}", formatTensor(inferRequest.get_input_tensor()))

    const ov::Tensor outputTensor(outputElementType, outputShape, outputBuffer);
    inferRequest.set_output_tensor(0, outputTensor);

    inferRequest.infer();
    NES_DEBUG("Model output: {}", formatTensor(inferRequest.get_output_tensor(0)))
}
}

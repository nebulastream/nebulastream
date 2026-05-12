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
#include <ErrorHandling.hpp>
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

namespace NES
{

namespace
{
/// `key` pins the IR the compiled model was built from, so a cache hit can be decided by
/// comparing payloads rather than by pointer identity of a buffer that may have been freed.
struct CompiledModelCacheEntry
{
    CompiledModel key;
    ov::CompiledModel compiled;
};
}

RuntimeMetadata OpenVinoRuntimeBackend::setup(const CompiledModel& model)
{
    static ov::Core sharedCore;
    static std::mutex coreMutex;
    /// Every worker thread running the same operator calls setup with the same model, and
    /// read_model/compile_model take seconds for a large one. Caching by model keeps that
    /// cost at once per distinct model per process instead of once per thread. Entries live
    /// for the process lifetime: they are bounded by the number of distinct models a worker
    /// loads, and the payload they pin is already held by the operator using it.
    static std::vector<CompiledModelCacheEntry> compiledModelCache;

    const std::scoped_lock lock(coreMutex);

    ov::CompiledModel compiledModel;
    /// Model's equality compares the IR payload, the weights and the shapes, so a hit means
    /// an identical model, not merely one loaded from the same path.
    const auto cached = std::ranges::find_if(compiledModelCache, [&](const CompiledModelCacheEntry& entry) { return entry.key == model; });
    if (cached != compiledModelCache.end())
    {
        compiledModel = cached->compiled;
    }
    else
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-text for OpenVINO XML payload
        const std::string modelXml(reinterpret_cast<const char*>(model.getData().data()), model.getData().size());
        std::vector<std::uint8_t> modelBin(model.getAuxiliaryData().size());
        std::ranges::transform(
            model.getAuxiliaryData(), modelBin.begin(), [](std::byte value) { return static_cast<std::uint8_t>(value); });

        const ov::Shape modelInputShape(model.getInputShape().begin(), model.getInputShape().end());
        ov::Tensor weights(ov::element::u8, {modelBin.size()});
        if (!modelBin.empty())
        {
            std::memcpy(weights.data<std::uint8_t>(), modelBin.data(), modelBin.size());
        }

        auto openVinoModel = sharedCore.read_model(modelXml, weights);
        openVinoModel->reshape(modelInputShape);
        compiledModel = sharedCore.compile_model(
            openVinoModel,
            "CPU",
            ov::hint::execution_mode(ov::hint::ExecutionMode::ACCURACY),
            ov::hint::performance_mode(ov::hint::PerformanceMode::LATENCY));
        compiledModelCache.emplace_back(model, compiledModel);
    }

    inferRequest = compiledModel.create_infer_request();
    inputElementType = compiledModel.input(0).get_element_type();
    inputShape = compiledModel.input(0).get_shape();
    outputElementType = compiledModel.output(0).get_element_type();
    outputShape = compiledModel.output(0).get_shape();
    requiredInputSize = inputElementType.size() * ov::shape_size(inputShape);
    requiredOutputSize = outputElementType.size() * ov::shape_size(outputShape);

    return RuntimeMetadata{
        .inputShape = model.getInputShape(),
        .nDim = model.getNDim(),
        .functionName = model.getFunctionName(),
        .inputSize = model.inputSize(),
        .outputSize = model.outputSize()};
}

void OpenVinoRuntimeBackend::infer(
    std::byte* inputBuffer, size_t inputBufferSize, std::byte* outputBuffer, size_t outputBufferSize)
{
    if (inputBufferSize > requiredInputSize)
    {
        throw NES::InferenceRuntimeFailure(
            "Model Execution failed. Model input size {} B exceeds buffer capacity {} B", inputBufferSize, requiredInputSize);
    }

    if (outputBufferSize > requiredOutputSize)
    {
        throw NES::InferenceRuntimeFailure(
            "Model Execution failed. Model output size {} B exceeds buffer capacity {} B", outputBufferSize, requiredOutputSize);
    }

    const ov::Tensor inputTensor(inputElementType, inputShape, inputBuffer);
    inferRequest.set_input_tensor(inputTensor);

    const ov::Tensor outputTensor(outputElementType, outputShape, outputBuffer);
    inferRequest.set_output_tensor(0, outputTensor);

    inferRequest.infer();
}
}

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
#include <Execution/Operators/Streaming/InferModel/ONNXAdapter.hpp>
#include <Util/Logger/Logger.hpp>
#include <numeric>
#include <onnxruntime/core/session/onnxruntime_cxx_api.h>
#include <span>

namespace NES::Runtime::Execution::Operators {

static const auto memory_info = Ort::MemoryInfo::CreateCpu(OrtDeviceAllocator, OrtMemTypeCPU);

InferenceAdapterPtr ONNXAdapter::create() { return std::make_shared<ONNXAdapter>(); }
size_t ONNXAdapter::getModelOutputSizeInBytes() {
    NES_ASSERT(this->session.has_value(), "Session not initialized. Was loadModel called?");
    NES_ASSERT(this->env.has_value(), "Environment not initialized. Was loadModel called?");

    return std::accumulate(this->output_shape.begin(), this->output_shape.end(), 1, std::multiplies<>()) * sizeof(float);
}
size_t ONNXAdapter::getModelInputSizeInBytes() {
    NES_ASSERT(this->session.has_value(), "Session not initialized. Was loadModel called?");
    NES_ASSERT(this->env.has_value(), "Environment not initialized. Was loadModel called?");

    return std::accumulate(this->input_shape.begin(), this->input_shape.end(), 1, std::multiplies<>()) * sizeof(float);
}

void ONNXAdapter::inferInternal(std::span<int8_t>&& input, std::span<int8_t>&& output) {
    NES_ASSERT(env.has_value(), "Env not initialized");
    NES_ASSERT(session.has_value(), "Session not initialized");
    NES_ASSERT(input.size() % sizeof(float) == 0, "Input size must be a multiple of sizeof(float)");
    NES_ASSERT(output.size() % sizeof(float) == 0, "Output size must be a multiple of sizeof(float)");

    static_assert(std::endian::native == std::endian::little, "TODO: Big Endian");

    auto output_data_size = output.size() / (sizeof(float) / sizeof(uint8_t));
    auto output_data = reinterpret_cast<float*>(output.data());
    auto input_data_size = input.size() / (sizeof(float) / sizeof(uint8_t));
    auto input_data = reinterpret_cast<float*>(input.data());

    auto input_tensor =
        Ort::Value::CreateTensor<float>(memory_info, input_data, input_data_size, input_shape.data(), input_shape.size());
    auto output_tensor =
        Ort::Value::CreateTensor<float>(memory_info, output_data, output_data_size, output_shape.data(), output_shape.size());

    auto input_name = session->GetInputNameAllocated(0, allocator);
    auto output_name = session->GetOutputNameAllocated(0, allocator);
    const char* input_names[] = {input_name.get()};
    const char* output_names[] = {output_name.get()};

    Ort::RunOptions run_options;
    session->Run(run_options, input_names, &input_tensor, 1, output_names, &output_tensor, 1);
}

void ONNXAdapter::loadModel(const std::string& pathToModel) {
    NES_DEBUG("INITIALIZING MODEL:  {}", pathToModel);

    Ort::ThrowOnError(Ort::GetApi().GetAllocatorWithDefaultOptions(&this->allocator));
    this->env = Ort::Env();
    this->session = Ort::Session(*env, pathToModel.c_str(), Ort::SessionOptions{nullptr});

    session->GetInputCount();
    NES_DEBUG("Model: {}", pathToModel);
    NES_DEBUG("Input count: {}", session->GetInputCount());
    for (size_t i = 0; i < session->GetInputCount(); ++i) {
        auto ti = session->GetInputTypeInfo(i);
        auto shape_info = ti.GetTensorTypeAndShapeInfo();
        std::vector<int64_t> shape = shape_info.GetShape();

        if (std::any_of(shape.begin(), shape.end(), [](int64_t i) {
                return i == -1;
            })) {
            NES_ERROR("Model: {}, Input: {} Shape: {}", pathToModel, i, fmt::join(shape, ","));
            NES_ASSERT(false, "Dynamic shape not supported!");
        }

        NES_DEBUG("Input {} type: {}", i, ti.GetONNXType());
        NES_DEBUG("         name: {}", session->GetInputNameAllocated(i, allocator).get());
        NES_DEBUG("        shape: {}", fmt::join(shape, ","));
        NES_DEBUG(" element type: {}", shape_info.GetElementType());
        NES_DEBUG("element count: {}", shape_info.GetElementCount());
    }

    NES_DEBUG("Output count: {}", session->GetOutputCount());
    for (size_t i = 0; i < session->GetOutputCount(); ++i) {
        auto ti = session->GetOutputTypeInfo(i);
        auto shape_info = ti.GetTensorTypeAndShapeInfo();
        std::vector<int64_t> shape = shape_info.GetShape();

        if (std::any_of(shape.begin(), shape.end(), [](int64_t i) {
                return i == -1;
            })) {
            NES_ERROR("Model: {}, Output: {} Shape: {}", pathToModel, i, fmt::join(shape, ","));
            NES_ASSERT(false, "Dynamic shape not supported!");
        }

        NES_DEBUG("Output {} type: {}", i, ti.GetONNXType());
        NES_DEBUG("         name: {}", session->GetOutputNameAllocated(i, allocator).get());
        NES_DEBUG("         shape: {}", fmt::join(shape, ","));
        NES_DEBUG("  element type: {}", shape_info.GetElementType());
        NES_DEBUG(" element count: {}", shape_info.GetElementCount());
    }

    auto metaData = session->GetModelMetadata();
    NES_DEBUG("Producer name: {}", metaData.GetProducerNameAllocated(allocator).get());
    NES_DEBUG("Graph name: {}", metaData.GetGraphNameAllocated(allocator).get());
    NES_DEBUG("Description: {}", metaData.GetDescriptionAllocated(allocator).get());
    NES_DEBUG("Domain: {}", metaData.GetDomainAllocated(allocator).get());
    NES_DEBUG("Version: {}", metaData.GetVersion());

    NES_ASSERT(session->GetOutputCount() == 1 && session->GetInputCount() == 1, "Only single input/output models are supported");

    auto inputTypeInfo = session->GetInputTypeInfo(0);
    this->input_shape = inputTypeInfo.GetTensorTypeAndShapeInfo().GetShape();
    auto outputTypeInfo = session->GetOutputTypeInfo(0);
    this->output_shape = outputTypeInfo.GetTensorTypeAndShapeInfo().GetShape();
}

ONNXAdapter::~ONNXAdapter() = default;

}// namespace NES::Runtime::Execution::Operators

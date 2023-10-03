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
#include <Execution/Operators/ONNX/ONNXAdapter.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/beast/core/detail/base64.hpp>
#include <numeric>
#include <onnxruntime/core/session/onnxruntime_cxx_api.h>
#include <span>

namespace NES::Runtime::Execution::Operators {
static std::string_view remove_padding(const std::string_view& base64_string) {
    NES_ASSERT(!base64_string.empty(), "Cannot be empty");
    size_t input_length = base64_string.length();
    NES_ASSERT(input_length % 4 == 0, "Length must be a multiple of 4");

    size_t padding_count = 0;
    padding_count += base64_string[input_length - 1] == '=';
    padding_count += base64_string[input_length - 1] == '=' && base64_string[input_length - 2] == '=';

    return base64_string.substr(0, input_length - padding_count);
}

void ONNXAdapter::appendBase64EncodedData(const std::string_view& base64_encoded_data) {
    using namespace boost::beast::detail::base64;
    using Base64DecodeIterator = boost::archive::iterators::
        transform_width<boost::archive::iterators::binary_from_base64<std::string::const_iterator>, 8, 6>;

    auto base64_encoded_data_without_padding = remove_padding(base64_encoded_data);
    NES_ASSERT(input_buffer.size() + decoded_size(base64_encoded_data_without_padding.length()) <= input_buffer.capacity(),
               "Input buffer is full");

    std::copy(Base64DecodeIterator(base64_encoded_data_without_padding.begin()),
              Base64DecodeIterator(base64_encoded_data_without_padding.end()),
              std::back_inserter(input_buffer));
}

std::string ONNXAdapter::getBase64EncodedData() const {
    using Base64EncodeIterator = boost::archive::iterators::base64_from_binary<
        boost::archive::iterators::transform_width<std::vector<int8_t>::const_iterator, 6, 8>>;

    return {Base64EncodeIterator(output_buffer.begin()), Base64EncodeIterator(output_buffer.end())};
}

void ONNXAdapter::infer() {
    NES_DEBUG("Buffer Size: {}  ModelInputSize: {}", input_buffer.size(), getModelInputSizeInBytes())
    NES_ASSERT(input_buffer.size() == getModelInputSizeInBytes(), "Input buffer size does not match model input size");

    inferInternal(std::span(input_buffer), std::span(output_buffer));
    input_buffer.clear();
}

void ONNXAdapter::initializeModel(const std::string& pathToModel) {
    loadModel(pathToModel);
    auto input_size = getModelInputSizeInBytes();
    auto output_size = getModelOutputSizeInBytes();

    input_buffer.reserve(input_size);
    output_buffer.resize(output_size);
}

float ONNXAdapter::getResultAt(size_t i) const {
    NES_ASSERT(i < output_buffer.size() / 4, "Index out of bounds");
    return *reinterpret_cast<const float*>(output_buffer.data() + (i * 4));
}

static const auto memory_info = Ort::MemoryInfo::CreateCpu(OrtDeviceAllocator, OrtMemTypeCPU);

size_t ONNXAdapter::getModelOutputSizeInBytes() {
    NES_ASSERT(this->session, "Session not initialized. Was loadModel called?");

    return std::accumulate(this->output_shape.begin(), this->output_shape.end(), 1, std::multiplies<>()) * sizeof(float);
}

size_t ONNXAdapter::getModelInputSizeInBytes() {
    NES_ASSERT(this->session, "Session not initialized. Was loadModel called?");

    return std::accumulate(this->input_shape.begin(), this->input_shape.end(), 1, std::multiplies<>()) * sizeof(float);
}

void ONNXAdapter::inferInternal(std::span<int8_t>&& input, std::span<int8_t>&& output) {
    NES_ASSERT(session, "Session not initialized");
    NES_ASSERT(input.size() % sizeof(float) == 0, "Input size must be a multiple of sizeof(float)");
    NES_ASSERT(output.size() % sizeof(float) == 0, "Output size must be a multiple of sizeof(float)");
    OrtAllocator *allocator;
    Ort::ThrowOnError(Ort::GetApi().GetAllocatorWithDefaultOptions(&allocator));
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

    OrtAllocator *allocator;
    Ort::ThrowOnError(Ort::GetApi().GetAllocatorWithDefaultOptions(&allocator));
    this->session = std::make_unique<Ort::Session>(Ort::Env(), pathToModel.c_str(), Ort::SessionOptions{nullptr});

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
ONNXAdapterPtr ONNXAdapter::create() {
    return std::make_unique<ONNXAdapter>();
}

ONNXAdapter::~ONNXAdapter() = default;
}// namespace NES::Runtime::Execution::Operators
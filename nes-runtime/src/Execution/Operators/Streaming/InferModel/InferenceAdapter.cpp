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

#include <Execution/Operators/Streaming/InferModel/InferenceAdapter.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/beast/core/detail/base64.hpp>

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

void InferenceAdapter::appendBase64EncodedData(const std::string_view& base64_encoded_data) {
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

std::string InferenceAdapter::getBase64EncodedData() const {
    using Base64EncodeIterator = boost::archive::iterators::base64_from_binary<
        boost::archive::iterators::transform_width<std::vector<int8_t>::const_iterator, 6, 8>>;

    return {Base64EncodeIterator(output_buffer.begin()), Base64EncodeIterator(output_buffer.end())};
}

void InferenceAdapter::infer() {
    NES_DEBUG("Buffer Size: {}  ModelInputSize: {}", input_buffer.size(), getModelInputSizeInBytes())
    NES_ASSERT(input_buffer.size() == getModelInputSizeInBytes(), "Input buffer size does not match model input size");

    inferInternal(std::span(input_buffer), std::span(output_buffer));
    input_buffer.clear();
}

void InferenceAdapter::initializeModel(const std::string& pathToModel) {
    loadModel(pathToModel);
    auto input_size = getModelInputSizeInBytes();
    auto output_size = getModelOutputSizeInBytes();

    input_buffer.reserve(input_size);
    output_buffer.resize(output_size);
}
float InferenceAdapter::getResultAt(size_t i) const {
    NES_ASSERT(i < output_buffer.size() / 4, "Index out of bounds");
    return *reinterpret_cast<const float*>(output_buffer.data() + (i * 4));
}
}// namespace NES::Runtime::Execution::Operators
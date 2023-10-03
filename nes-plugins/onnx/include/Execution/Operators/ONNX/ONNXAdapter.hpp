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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_INFERMODEL_INFERENCEADAPTER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_INFERMODEL_INFERENCEADAPTER_HPP_

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace Ort {
    class Session;
}

namespace NES::Runtime::Execution::Operators {
    class ONNXAdapter;

    typedef std::unique_ptr<ONNXAdapter> ONNXAdapterPtr;

    class ONNXAdapter {
    public:
        ~ONNXAdapter();

        static ONNXAdapterPtr create();
        /**
         * @brief Initialize the Model
         * @param pathToModel file containing the serialized model
         */
        void initializeModel(const std::string &pathToModel);

        /**
         * @brief Runs inference on the Model for a single input tuple
         */
        void infer();

        /**
         * @brief accesses the ith field of the output
         * @param i index of the output value
         * @return float value
         */
        [[nodiscard]] float getResultAt(size_t i) const;

        /**
         * @brief loads base64 encoded data into the input tensor
         * @param string_view into the base64 encoded data. Data is copied!
         */
        void appendBase64EncodedData(const std::string_view &base64_encoded_data);

        /**
         * @brief copies the output of the inference model into a base64 encoded string
         * @return base64 encoded output of the model
         */
        [[nodiscard]] std::string getBase64EncodedData() const;

        /**
         * Copies a single objects of type T into the input tensor
         * @tparam T (float, uint*_t, int*_t)
         * @param data pointer to object
         */
        template<typename T>
        void appendToByteArray(const T &data);

    private:
        void inferInternal(std::span<int8_t> &&input, std::span<int8_t> &&output);

        void loadModel(const std::string &pathToModel);

        size_t getModelOutputSizeInBytes();

        size_t getModelInputSizeInBytes();

        std::vector<int8_t> output_buffer;
        std::vector<int8_t> input_buffer;
        std::unique_ptr<Ort::Session> session;
        std::vector<int64_t> input_shape;
        std::vector<int64_t> output_shape;
    };

    template<typename T>
    void ONNXAdapter::appendToByteArray(const T &data) {
        auto *ptr = reinterpret_cast<const int8_t *>(&data);
        input_buffer.insert(input_buffer.end(), ptr, ptr + sizeof(T));
    }
}// namespace NES::Runtime::Execution::Operators


#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_INFERMODEL_INFERENCEADAPTER_HPP_

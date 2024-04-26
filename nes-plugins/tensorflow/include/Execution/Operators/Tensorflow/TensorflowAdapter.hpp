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

#ifndef NES_PLUGINS_TENSORFLOW_INCLUDE_EXECUTION_OPERATORS_TENSORFLOW_TENSORFLOWADAPTER_HPP_
#define NES_PLUGINS_TENSORFLOW_INCLUDE_EXECUTION_OPERATORS_TENSORFLOW_TENSORFLOWADAPTER_HPP_

#include <any>
#include <memory>
#include <vector>

class TfLiteInterpreter;
class TfLiteTensor;

namespace NES::Runtime::Execution::Operators {

class TensorflowAdapter;
typedef std::shared_ptr<TensorflowAdapter> TensorflowAdapterPtr;

class TensorflowAdapter {
  public:
    static TensorflowAdapterPtr create();

    TensorflowAdapter() = default;

    ~TensorflowAdapter();

    /**
     * @brief Initialize tensorflow model
     * @param pathToModel file containing the serialized model
     */
    void initializeModel(std::string pathToModel);

    /**
     * @brief Add input for model inference
     * @param index: location of the input value
     * @param value: the input value
     */
    template<class T>
    void addModelInput(int index, T value) {
        ((T*) inputData)[index] = value;
    };

    /**
     * @brief runs the tensorflow model of a single tuple
     */
    void infer();

    /**
     * @brief accesses the ith field of the output
     * @param i index of the output value
     * @return float value
     */
    float getResultAt(int i);

  private:
    TfLiteInterpreter* interpreter{};
    TfLiteTensor* inputTensor;
    int tensorSize;
    void* inputData{};
    // TODO https://github.com/nebulastream/nebulastream/issues/3424
    // Right now we only support 32-bit floats as output.
    float* outputData{};
};

}// namespace NES::Runtime::Execution::Operators

#endif // NES_PLUGINS_TENSORFLOW_INCLUDE_EXECUTION_OPERATORS_TENSORFLOW_TENSORFLOWADAPTER_HPP_

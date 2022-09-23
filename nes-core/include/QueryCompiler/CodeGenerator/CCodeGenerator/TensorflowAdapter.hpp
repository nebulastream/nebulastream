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

#ifndef NES_TENSORFLOWADAPTER_HPP
#define NES_TENSORFLOWADAPTER_HPP

#include <memory>
#include <vector>
#include <tensorflow/lite/c/c_api.h>
#include <tensorflow/lite/c/c_api_experimental.h>
#include <tensorflow/lite/c/common.h>


namespace NES {

class TensorflowAdapter;
typedef std::shared_ptr<TensorflowAdapter> TensorflowAdapterPtr;

class TensorflowAdapter {
  public:
    static TensorflowAdapterPtr create();
    TensorflowAdapter();
    void infer(int n, ...);
    float getResultAt(int i);
    void initializeModel(std::string model);
    void pass() {}
  private:
    TfLiteInterpreter* interpreter;
    float* output{};
};

}// namespace NES
#endif//NES_TENSORFLOWADAPTER_HPP

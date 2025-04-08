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

#include "IREEAdapter.hpp"
#include "IREECompilerWrapper.hpp"
#include "IREERuntimeWrapper.hpp"
#include <fstream>
#include <iree/runtime/api.h>
#include <iree/compiler/loader.h>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators
{

std::shared_ptr<IREEAdapter> IREEAdapter::create() { return std::make_shared<IREEAdapter>(); }

void IREEAdapter::initializeModel(std::string modelPath)
{
    this->modelPath = modelPath;
    this->compilerWrapper = IREECompilerWrapper();
    const iree_const_byte_span_t compiledModel = compilerWrapper.compileModel(modelPath);

    this->runtimeWrapper = IREERuntimeWrapper();
    runtimeWrapper.setup(compiledModel);
    runtimeWrapper.setInputShape(compilerWrapper.getInputShape());
    runtimeWrapper.setNDim(compilerWrapper.getNDim());

    this->inputSize = compilerWrapper.getInputSize();
    this->inputData = malloc(inputSize);
}

void IREEAdapter::infer()
{
    this->outputData = runtimeWrapper.execute(compilerWrapper.getFunctionName(), inputData, inputSize, outputData);
}

float IREEAdapter::getResultAt(int i)
{
    return outputData[i];
}

IREEAdapter::~IREEAdapter()
{
    ireeCompilerGlobalShutdown();
    free(inputData);
    free(outputData);
}

}
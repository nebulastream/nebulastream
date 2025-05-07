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

#include "IREECompilerWrapper.hpp"
#include <Util/Logger/Logger.hpp>
#include <llvm/Support/CommandLine.h>
#include <mlir-c/BuiltinAttributes.h>
#include <mlir-c/IR.h>
#include <mlir-c/Support.h>

namespace NES::Runtime::Execution::Operators
{

iree_const_byte_span_t IREECompilerWrapper::compileModel(std::string)
{
    return {nullptr, 0};
}

std::string IREECompilerWrapper::getFunctionName()
{
    return this->functionName;
}

size_t IREECompilerWrapper::getInputSize()
{
    return this->inputSize;
}

std::vector<int> IREECompilerWrapper::getInputShape()
{
    return this->inputShape;
}

int IREECompilerWrapper::getNDim()
{
    return this->nDim;
}

}

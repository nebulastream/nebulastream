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

#include <Nautilus/Backends/Experimental/Vectorization/CUDA/CUDALoweringInterface.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Backends::CUDA {

CUDALoweringInterface::CUDALoweringInterface(const RegisterFrame& frame)
    : CPP::CPPLoweringInterface(frame)
{

}

std::unique_ptr<CodeGen::CodeGenerator> CUDALoweringInterface::lowerProxyCall(const std::shared_ptr<IR::Operations::ProxyCallOperation>& operation, RegisterFrame& frame) {
    if (operation->getFunctionSymbol() == "NES__Runtime__TupleBuffer__getBuffer") {
        auto code = std::make_unique<CodeGen::Segment>();
        auto identifier = operation->getIdentifier();
        auto var = getVariable(identifier);
        auto type = getType(operation->getStamp());
        frame.setValue(identifier, var);
        auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
        code->addToProlog(decl);
        auto tupleBufferVar = frame.getValue(TUPLE_BUFFER_IDENTIFIER);
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(fmt::format("{} = NES__CUDA__TupleBuffer__getBuffer({})", var, tupleBufferVar));
        code->add(stmt);
        return std::move(code);
    } else if (operation->getFunctionSymbol() == "NES__Runtime__TupleBuffer__getNumberOfTuples") {
        auto code = std::make_unique<CodeGen::Segment>();
        auto identifier = operation->getIdentifier();
        auto var = getVariable(identifier);
        auto type = getType(operation->getStamp());
        frame.setValue(identifier, var);
        auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
        code->addToProlog(decl);
        auto tupleBufferVar = frame.getValue(TUPLE_BUFFER_IDENTIFIER);
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(fmt::format("{} = NES__CUDA__TupleBuffer__getNumberOfTuples({})", var, tupleBufferVar));
        code->add(stmt);
        return std::move(code);
    }
    NES_NOT_IMPLEMENTED();
}

} // namespace NES::Nautilus::Backends::CUDA

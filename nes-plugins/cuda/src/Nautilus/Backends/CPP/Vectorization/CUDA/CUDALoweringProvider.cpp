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

#include <Nautilus/Backends/CPP/CPPLoweringContext.hpp>
#include <Nautilus/Backends/CPP/Vectorization/CUDA/CUDALoweringProvider.hpp>
#include <Nautilus/CodeGen/CPP/Statement.hpp>
#include <Nautilus/CodeGen/Segment.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Backends::CPP::Vectorization::CUDA {

CUDALoweringProvider::CUDALoweringProvider(const CUDALoweringProvider::RegisterFrame& registerFrame)
    : registerFrame(registerFrame) {}

std::unique_ptr<CodeGen::CodeGenerator>
CUDALoweringProvider::lowerProxyCall(const std::shared_ptr<IR::Operations::ProxyCallOperation>& operation, RegisterFrame& frame) {
    if (operation->getFunctionSymbol() == "NES__Runtime__TupleBuffer__getBuffer") {
        auto code = std::make_unique<CodeGen::Segment>();
        auto inputBufferVar = CPPLoweringContext::getVariable(operation->getInputArguments().at(0)->getIdentifier());
        auto identifier = operation->getIdentifier();
        auto var = CPPLoweringContext::getVariable(identifier);
        auto type = CPPLoweringContext::getType(operation->getStamp());
        frame.setValue(identifier, var);
        auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
        code->addToProlog(decl);
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(
            fmt::format("{} = NES__CUDA__TupleBuffer__getBuffer({})", var, inputBufferVar));
        code->add(stmt);
        return std::move(code);
    } else if (operation->getFunctionSymbol() == "NES__Runtime__TupleBuffer__getNumberOfTuples") {
        auto code = std::make_unique<CodeGen::Segment>();
        auto inputBufferVar = CPPLoweringContext::getVariable(operation->getInputArguments().at(0)->getIdentifier());
        auto identifier = operation->getIdentifier();
        auto var = CPPLoweringContext::getVariable(identifier);
        auto type = CPPLoweringContext::getType(operation->getStamp());
        frame.setValue(identifier, var);
        auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
        code->addToProlog(decl);
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(
            fmt::format("{} = NES__CUDA__TupleBuffer__getNumberOfTuples({})", var, inputBufferVar));
        code->add(stmt);
        return std::move(code);
    } else if (operation->getFunctionSymbol() == "NES__Runtime__TupleBuffer__getCreationTimestampInMS") {
        auto code = std::make_unique<CodeGen::Segment>();
        auto identifier = operation->getIdentifier();
        auto var = CPPLoweringContext::getVariable(identifier);
        auto type = CPPLoweringContext::getType(operation->getStamp());
        frame.setValue(identifier, var);
        auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
        code->addToProlog(decl);
        auto tupleBufferVar = frame.getValue(TUPLE_BUFFER_IDENTIFIER);
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(
            fmt::format("{} = NES__CUDA__TupleBuffer_getCreationTimestampInMS({})", var, tupleBufferVar));
        code->add(stmt);
        return std::move(code);
    } else if (operation->getFunctionSymbol() == "sum") {
        auto code = std::make_unique<CodeGen::Segment>();
        auto identifier = operation->getIdentifier();
        auto var = CPPLoweringContext::getVariable(identifier);
        auto type = CPPLoweringContext::getType(operation->getStamp());
        frame.setValue(identifier, var);
        auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
        code->addToProlog(decl);
        auto tupleBufferVar = frame.getValue(TUPLE_BUFFER_IDENTIFIER);
        auto tidVar = CPPLoweringContext::getVariable(operation->getInputArguments().at(1)->getIdentifier());
        auto offsetVar = CPPLoweringContext::getVariable(operation->getInputArguments().at(2)->getIdentifier());
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(
            fmt::format("{} = NES__CUDA__sum<uint64_t, 32, 1>({}, {}, {})", var, tupleBufferVar, tidVar, offsetVar));
        code->add(stmt);
        return std::move(code);
    } else if (operation->getFunctionSymbol() == "getSliceStore") {
        auto code = std::make_unique<CodeGen::Segment>();
        auto identifier = operation->getIdentifier();
        auto var = CPPLoweringContext::getVariable(identifier);
        auto type = CPPLoweringContext::getType(operation->getStamp());
        frame.setValue(identifier, var);
        auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
        code->addToProlog(decl);
        auto sliceStoreVar = frame.getValue(SLICE_STORE_IDENTIFIER);
        auto stmt =
            std::make_shared<CodeGen::CPP::Statement>(fmt::format("{} = NES__CUDA__getSliceStore({})", var, sliceStoreVar));
        code->add(stmt);
        return std::move(code);
    } else if (operation->getFunctionSymbol() == "setAsValidInMetadata") {
        auto tidVar = CPPLoweringContext::getVariable(operation->getInputArguments().at(0)->getIdentifier());
        auto code = std::make_unique<CodeGen::Segment>();
        auto tupleBufferVar = frame.getValue(TUPLE_BUFFER_IDENTIFIER);
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(
            fmt::format("NES__CUDA__setAsValidInMetadata({}, {})", tupleBufferVar, tidVar));
        code->add(stmt);
        return std::move(code);
    }
    NES_NOT_IMPLEMENTED();
}

std::unique_ptr<CodeGen::Segment> CUDALoweringProvider::lowerGraph(const std::shared_ptr<IR::IRGraph>& irGraph) {
    auto ctx = CPPLoweringContext(std::move(irGraph));

    auto functionOperation = irGraph->getRootOperation();
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    for (auto i = 0ull; i < functionBasicBlock->getArguments().size(); i++) {
        auto argument = functionBasicBlock->getArguments()[i];
        auto var = getVariable(argument->getIdentifier());
        registerFrame.setValue(argument->getIdentifier(), var);
    }

    auto _ = ctx.process(functionBasicBlock,
                         registerFrame);// TODO 4829: process with basic block argument should not be public, override instead
    functionBody->mergeProloguesToTopLevel();
    return std::move(functionBody);
}

std::string CUDALoweringProvider::getVariable(const std::string& id) { return CPPLoweringContext::getVariable(id); }
std::string CUDALoweringProvider::getType(const IR::Types::StampPtr& stamp) { return CPPLoweringContext::getType(stamp); }

CUDALoweringProvider::CUDALoweringProvider() = default;

}// namespace NES::Nautilus::Backends::CPP::Vectorization::CUDA

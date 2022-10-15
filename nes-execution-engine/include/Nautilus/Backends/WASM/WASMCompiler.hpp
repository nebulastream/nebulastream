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

#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_WASM_WASMCOMPILER_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_WASM_WASMCOMPILER_HPP_
#include <memory>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/AddressOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/CastOperation.hpp>
#include <Nautilus/IR/Operations/ConstBooleanOperation.hpp>
#include <Nautilus/IR/Operations/ConstFloatOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>

namespace NES::Nautilus::Backends::WASM {

class WASMCompiler {
  public:
    WASMCompiler();
    BinaryenModuleRef generateWASM(std::shared_ptr<IR::IRGraph> ir);
    void generateWASM(IR::BasicBlockPtr basicBlock, BinaryenModuleRef& module);
    void generateWASM(const IR::Operations::OperationPtr& operation, BinaryenModuleRef& module);
  private:
    std::shared_ptr<IR::IRGraph> ir;
};
}// namespace NES::Nautilus::Backends::WASM

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_WASM_WASMCOMPILER_HPP_
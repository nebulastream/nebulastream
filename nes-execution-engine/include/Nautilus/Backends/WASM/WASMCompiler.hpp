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
#include <binaryen-c.h>
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
#include <Nautilus/Util/Mapping.hpp>
#include <map>

namespace NES::Nautilus::Backends::WASM {

class WASMCompiler {
  public:
    WASMCompiler();
    BinaryenModuleRef lower(const std::shared_ptr<IR::IRGraph>& ir);

    using BinaryenExpressions = Mapping<std::string, BinaryenExpressionRef>;
    using RelooperBlocks = Mapping<std::string, RelooperBlockRef>;
    using BlockExpressions = Mapping<std::string, BinaryenExpressionRef>;
    using ExpressionsInBlocks = Mapping<std::string, std::vector<BinaryenExpressionRef>>;
  private:
    /**
     * Binaryen module which generates the final wasm binary code
     */
    BinaryenModuleRef wasm;
    /**
     * The Relooper instance used for creating structured CFGs
     */
    RelooperRef relooper;
    /**
     * Create RelooperBlock from BinaryenBlocks and store them
     */
    RelooperBlocks relooperBlocks;
    /**
     * Save variable s
     */
    Mapping<std::string, BinaryenType> localVariables;
    /**
     * Local variables mapped to BinaryenIndex
     */
     Mapping<std::string, BinaryenExpressionRef> localVarMapping;
    /**
     * Contains all expressions that have been already added. That way we prevent adding duplicate
     * expressions.
     */
    std::map<std::string, BinaryenExpressionRef> consumed;
    /**
     * Contains branches between blocks, which need to be added after all blocks are created
     */
    std::vector<std::tuple<std::string, std::string, BinaryenExpressionRef>> blockLinking;
    /**
     * Contains all expressions inside the current block
     */
    BinaryenExpressions currentExpressions;
    /**
     * Stores created blocks before we link them via branches
     */
    BlockExpressions blocks;
    /**
     * Current block over which we iterate
     */
    IR::BasicBlockPtr currentBlock;
    /**
     * Mapping of blocks and their contents
     */
    ExpressionsInBlocks blockExpressions;

    int index;
    int argIndex;

    void generateWASM(IR::BasicBlockPtr basicBlock, BinaryenExpressions& expressions);
    void generateWASM(const IR::Operations::OperationPtr& operation, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::FunctionOperation> funcOp);
    void generateWASM(std::shared_ptr<IR::Operations::ConstIntOperation> constIntOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::ConstFloatOperation> constFloatOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::ConstBooleanOperation> constBooleanOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::AddOperation> addOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::SubOperation> subOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::MulOperation> mulOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::DivOperation> divOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::StoreOperation> storeOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::LoadOperation> loadOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::AddressOperation> addressOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::IfOperation> ifOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::CompareOperation> compareOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::BranchOperation> branchOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::ReturnOperation> returnOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::ProxyCallOperation> proxyCallOp, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::OrOperation> yieldOperation, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::AndOperation> yieldOperation, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::NegateOperation> yieldOperation, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::CastOperation> castOperation, BinaryenExpressions& module);
    void generateWASM(std::shared_ptr<IR::Operations::LoopOperation> loopOp, BinaryenExpressions& module);
    BinaryenType getType(IR::Types::StampPtr stampPtr);
    BinaryenOp convertToInt32Comparison(IR::Operations::CompareOperation::Comparator comparisonType);
    BinaryenOp convertToInt64Comparison(IR::Operations::CompareOperation::Comparator comparisonType);
    BinaryenOp convertToFloat32Comparison(IR::Operations::CompareOperation::Comparator comparisonType);
    BinaryenOp convertToFloat64Comparison(IR::Operations::CompareOperation::Comparator comparisonType);
    void generateBasicBlock(IR::Operations::BasicBlockInvocation& blockInvocation,
                                             BinaryenExpressions blockArgs);
    void genBody(BinaryenExpressions expressions);
    BinaryenExpressionRef generateBody();
    void convertConstToLocal(std::string& key, BinaryenExpressions expressions);
};
}// namespace NES::Nautilus::Backends::WASM

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_WASM_WASMCOMPILER_HPP_
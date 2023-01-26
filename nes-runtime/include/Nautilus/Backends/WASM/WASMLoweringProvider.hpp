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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMCOMPILER_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMCOMPILER_HPP_

#include <Nautilus/Backends/WASM/Mapping.hpp>
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
#include <binaryen-c.h>
#include <map>
#include <memory>
#include <utility>

namespace NES::Nautilus::Backends::WASM {

class WASMLoweringProvider {
    using BinaryenExpressions = Mapping<std::string, BinaryenExpressionRef>;
    using RelooperBlocks = Mapping<std::string, RelooperBlockRef>;
    using BlockExpressions = Mapping<std::string, BinaryenExpressionRef>;
    using ProxyFunctions = Mapping<std::string, std::vector<BinaryenType>>;

  public:
    WASMLoweringProvider() {}
    /**
     * Lowers/translates Nautilus IR to WebAssembly binary code
     * @param ir Nautilus IR that needs to be lowered
     * @return WebAssembly binary and it's size
     */
    std::pair<size_t, char*> lower(const std::shared_ptr<IR::IRGraph>& ir);

  private:
    /**
     * Binaryen module which generates the final wasm binary code
     */
    BinaryenModuleRef wasm{};
    /**
     * The Relooper instance used for creating structured CFGs. We first generate code blocks, which
     * are not connected with each other. With the help of the Relooper algorithm we link these code blocks
     * accordingly and infer how they're connected, via if/else or a loop for example.
     * For more details: https://dl.acm.org/doi/abs/10.1145/2048147.2048224
     */
    RelooperRef relooper{};
    /**
     * Create RelooperBlock from BinaryenBlocks and store them
     */
    RelooperBlocks relooperBlocks;
    /**
     * Save locals (variables) generated during compilation of expressions.
     * The whole list of locals is needed in the last compilation step, when generating the execute function
     */
    Mapping<std::string, BinaryenType> localVariables;
    /**
     * Local variables mapped to BinaryenIndex.
     */
    Mapping<std::string, BinaryenExpressionRef> localVarMapping;
    /**
     * Contains all expressions that are already used in another expression. That way we prevent adding duplicate
     * expressions on the stack. For example, in
     * 0_0 = 7 :i64
     * 0_1 = 0_103 + 0_0 :i64
     * 0_0 would end up in consumed as 0_1 already "defines" it
     */
    std::unordered_map<std::string, BinaryenExpressionRef> consumed;
    /**
     * Holds information if blocks are connected with each other with a condition. This is the case in if/else and switch
     * branching
     */
    struct blockBranches {
        std::string currentBlock;
        std::string nextBlock;
        BinaryenExpressionRef condition;
        blockBranches(std::string cb, std::string nb, BinaryenExpressionRef con)
            : currentBlock(std::move(cb)), nextBlock(std::move(nb)), condition(con){}
    };
    /**
     * Contains branches between blocks, which need to be added after all blocks are created
     */
    std::vector<blockBranches> blockLinking;
    /**
     * Stores created blocks before we link them via branches
     */
    BlockExpressions blocks;
    /**
     * Current block over which we iterate
     */
    IR::BasicBlockPtr currentBlock;
    /**
     * Proxy functions used. Contains the proxy function name as key and a vector with the parameter &
     * return types as value => {param_1, ..., param_n, return}
     */
    ProxyFunctions proxyFunctions;
    /**
     * The name used to import/export WebAssembly memory
     */
    const char* memoryName = "memory";
    /**
     * Importing external names requires module and function names. This variables is used for the module name,
     * so all defined proxy functions use the same module name and are just separated by the function name
     */
    const char* proxyFunctionModule = "ProxyFunction";

    /**
     * Function arguments and locals share the same index space in Webassembly. When creating new locals, we need to track
     * the index.
     */
    int localVariablesIndex{};

    /**
     * We iterate over a given IR and depending on the expression, generate WebAssembly code
     * @param basicBlock the current basic block where we iterate over it's expressions
     * @param expressions contains the generated WebAssembly/binaryen expressions
     */
    void generateWASM(const IR::BasicBlockPtr& basicBlock, BinaryenExpressions& expressions);
    void generateWASM(const IR::Operations::OperationPtr& operation, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::FunctionOperation>& funcOp);
    void generateWASM(const std::shared_ptr<IR::Operations::ConstIntOperation>& constIntOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::ConstFloatOperation>& constFloatOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::ConstBooleanOperation>& constBooleanOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::AddOperation>& addOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::SubOperation>& subOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::MulOperation>& mulOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::DivOperation>& divOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::StoreOperation>& storeOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::LoadOperation>& loadOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::AddressOperation>& addressOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::IfOperation>& ifOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::CompareOperation>& compareOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::BranchOperation>& branchOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::ReturnOperation>& returnOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::ProxyCallOperation>& proxyCallOp, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::OrOperation>& yieldOperation, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::AndOperation>& yieldOperation, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::NegateOperation>& yieldOperation, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::CastOperation>& castOperation, BinaryenExpressions& module);
    void generateWASM(const std::shared_ptr<IR::Operations::LoopOperation>& loopOp, BinaryenExpressions& module);

    /**
     * Given a comparison operator with 32bit integer inputs, we need to generate the appropriate comparison operation in
     * WebAssembly
     * @param comparisonType a given comparison operation such as greater than
     * @return the Binaryen operation for the given {@param} comparisonType
     */
    static BinaryenOp convertToInt32Comparison(IR::Operations::CompareOperation::Comparator comparisonType);
    /**
     * Given a comparison operator with 64bit integer inputs, we need to generate the appropriate comparison operation in
     * WebAssembly
     * @param comparisonType a given comparison operation such as greater than
     * @return the Binaryen operation for the given {@param} comparisonType
     */
    static BinaryenOp convertToInt64Comparison(IR::Operations::CompareOperation::Comparator comparisonType);
    /**
     * Given a comparison operator with 32bit float inputs, we need to generate the appropriate comparison operation in
     * WebAssembly
     * @param comparisonType a given comparison operation such as greater than
     * @return the Binaryen operation for the given {@param} comparisonType
     */
    static BinaryenOp convertToFloat32Comparison(IR::Operations::CompareOperation::Comparator comparisonType);
    /**
     * Given a comparison operator with 64bit float inputs, we need to generate the appropriate comparison operation in
     * WebAssembly
     * @param comparisonType a given comparison operation such as greater than
     * @return the Binaryen operation for the given {@param} comparisonType
     */
    static BinaryenOp convertToFloat64Comparison(IR::Operations::CompareOperation::Comparator comparisonType);
    /**
     * This is called whenever visiting a new block
     * @param blockInvocation the currently visited block
     * @param blockArgs arguments/variables passed to the block
     */
    void generateBasicBlock(IR::Operations::BasicBlockInvocation& blockInvocation, BinaryenExpressions blockArgs);
    /**
     * Iterate over all generated expressions for a block and generate a Binaryen block
     * @param expressions all generated expressions
     */
    void generateBody(BinaryenExpressions expressions);
    /**
     * Iterate over all generated blocks, transform them into Relooper blocks and generate branches between the
     * blocks. Branches can be statements such as if/else, switch, loops
     * @return a complete function body for the execute function of the query
     */
    BinaryenExpressionRef generateCFG();
    /**
     * Do the conversion from IR types to Binaryen/WebAssembly types.
     * @param stampPtr IR type
     * @return Binaryen type
     */
    static BinaryenType getBinaryenType(const IR::Types::StampPtr& stampPtr);
};
}// namespace NES::Nautilus::Backends::WASM

#endif//NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMCOMPILER_HPP_
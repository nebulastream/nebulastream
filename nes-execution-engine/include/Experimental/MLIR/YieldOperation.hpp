

#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_YIELDOPERATION_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_YIELDOPERATION_HPP_

#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlockInvocation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
/**
 * @brief Terminator Operation(Op), must be last Op in BasicBlock(BB). Passes control flow from one BB to another.
 */
class YieldOperation : public Operation {
  public:
    explicit YieldOperation(std::vector<OperationPtr> arguments, BasicBlockPtr nextBlock);
    ~YieldOperation() override = default;
    std::vector<OperationPtr> getArguments();
    std::string toString() override;
    static bool classof(const Operation *Op);
  private:
    std::vector<OperationWPtr> arguments;
    BasicBlockPtr nextBlock;
};
}// namespace NES
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_YIELDOPERATION_HPP_

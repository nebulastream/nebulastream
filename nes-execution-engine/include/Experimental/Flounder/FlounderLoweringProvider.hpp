#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_FLOUNDER_FLOUNDERLOWERINGPROVIDER_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_FLOUNDER_FLOUNDERLOWERINGPROVIDER_HPP_
#include <Experimental/NESIR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Experimental/NESIR/Frame.hpp>
#include <Experimental/NESIR/NESIR.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/ConstIntOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/AndOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/OrOperation.hpp>
#include <Experimental/NESIR/Operations/Loop/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/ProxyCallOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>
#include <flounder/compiler.h>
#include <flounder/executable.h>
#include <flounder/program.h>
#include <set>
namespace NES::ExecutionEngine::Experimental::Flounder {

class FlounderLoweringProvider {
  public:
    FlounderLoweringProvider();
    std::unique_ptr<flounder::Executable> lower(std::shared_ptr<IR::NESIR> ir);
    flounder::Compiler compiler = flounder::Compiler{/*do not optimize*/ false,
                                                     /*collect the asm code to print later*/ true,
                                                     /*do not collect asm instruction offsets*/ true};

  private:
    using FlounderFrame = IR::Frame<std::string, flounder::Node*>;
    class LoweringContext {
      public:
        LoweringContext(std::shared_ptr<IR::NESIR> ir);
        std::unique_ptr<flounder::Executable> process(flounder::Compiler& compiler);
        void process(std::shared_ptr<IR::Operations::FunctionOperation>);
        void process(std::shared_ptr<IR::BasicBlock>, FlounderFrame& frame);
        void processInline(std::shared_ptr<IR::BasicBlock>, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::Operation>, FlounderFrame& frame);
        FlounderFrame processBlockInvocation(IR::Operations::BasicBlockInvocation&, FlounderFrame& frame);
        FlounderFrame processInlineBlockInvocation(IR::Operations::BasicBlockInvocation&, FlounderFrame& frame);
        flounder::VirtualRegisterIdentifierNode*
        createVreg(IR::Operations::OperationIdentifier id, IR::Types::StampPtr stamp, FlounderFrame& frame);

      private:
        flounder::Program program;
        std::shared_ptr<IR::NESIR> ir;
        std::set<std::string> activeBlocks;
        void process(std::shared_ptr<IR::Operations::AddOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::MulOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::IfOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::CompareOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::BranchOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::LoopOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::LoadOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::StoreOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::ProxyCallOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::OrOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::AndOperation> opt, FlounderFrame& frame);
    };
};

}// namespace NES::ExecutionEngine::Experimental::Flounder

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_FLOUNDER_FLOUNDERLOWERINGPROVIDER_HPP_

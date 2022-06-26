#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_PHASES_LOOPINFERENCEPHASE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_PHASES_LOOPINFERENCEPHASE_HPP_
#include <Experimental/NESIR/NESIR.hpp>
#include <optional>
#include <Experimental/NESIR/Operations/Loop/LoopInfo.hpp>
namespace NES::ExecutionEngine::Experimental::IR {
class LoopInferencePhase {

  public:
    std::shared_ptr<NESIR> apply(std::shared_ptr<NESIR> ir);

  private:
    class Context {
      public:
        Context(std::shared_ptr<NESIR> ir);
        std::shared_ptr<NESIR> process();
        void processBlock(BasicBlockPtr block);
        void processLoop(BasicBlockPtr block);
        std::optional<std::shared_ptr<Operations::CountedLoopInfo>> isCountedLoop(BasicBlockPtr block);
      private:
        std::shared_ptr<NESIR> ir;
    };
};
}// namespace NES::ExecutionEngine::Experimental::IR::Operations
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_PHASES_LOOPINFERENCEPHASE_HPP_

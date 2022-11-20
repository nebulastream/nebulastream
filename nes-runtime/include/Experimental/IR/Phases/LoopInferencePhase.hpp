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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_IR_PHASES_LOOPINFERENCEPHASE_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_IR_PHASES_LOOPINFERENCEPHASE_HPP_
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/Loop/LoopInfo.hpp>
#include <optional>
namespace NES::Nautilus::IR {
class LoopInferencePhase {

  public:
    std::shared_ptr<IRGraph> apply(std::shared_ptr<IRGraph> ir);

  private:
    class Context {
      public:
        Context(std::shared_ptr<IRGraph> ir);
        std::shared_ptr<IRGraph> process();
        void processBlock(BasicBlockPtr block);
        void processLoop(BasicBlockPtr block);
        std::optional<std::shared_ptr<Operations::CountedLoopInfo>> isCountedLoop(BasicBlockPtr block);

      private:
        std::shared_ptr<IRGraph> ir;
    };
};
}// namespace NES::Nautilus::IR
#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_IR_PHASES_LOOPINFERENCEPHASE_HPP_

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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_IRGRAPH_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_IRGRAPH_HPP_

#include <memory>
namespace NES::Nautilus::IR {

namespace Operations {
class FunctionOperation;
}

/**
 * @brief The IRGraph represents a fragment of nautilus ir.
 */
class IRGraph {
  public:
    /**
     * @brief An IR graph is created by applying the SSACreationPhase, and the TraceToIRConversionPhase to a query trace.
     *        Several phases can be applied to an existing IR graph. Each phase depends on all previous phases. Thus,
     *        phase 3(StructuredControlFlowPhase), depends on phase 2(LoopDetectionPhase) and phase 1(BrOnlyBlocksPhase).
     */
    enum AppliedPhases {
        TraceToIRConversionPhase = 0, 
        RemoveBrOnlyBlocksPhase = 1, 
        LoopDetectionPhase = 2, 
        StructuredControlFlowPhase = 3, 
        ValueScopingPhase = 4, 
        CountedLoopDetectionPhase = 5
    };
    IRGraph() = default;
    ~IRGraph() = default;
    std::shared_ptr<Operations::FunctionOperation> addRootOperation(std::shared_ptr<Operations::FunctionOperation> rootOperation);
    std::shared_ptr<Operations::FunctionOperation> getRootOperation();
    void setAppliedPhases(AppliedPhases appliedPhases);
    AppliedPhases getAppliedPhases();
    const std::string printAppliedPhases();
    std::string toString();

  private:
    std::shared_ptr<Operations::FunctionOperation> rootOperation;
    AppliedPhases appliedPhases;
};

}// namespace NES::Nautilus::IR

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_IRGRAPH_HPP_

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


#ifndef NES_RANDOMSAMPLEWITHOUTREPLACEMENTOPERATORHANDLER_HPP
#define NES_RANDOMSAMPLEWITHOUTREPLACEMENTOPERATORHANDLER_HPP

#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::ASP {
/**
 * @brief OperatorHandler for a SRSWoR synopsis
 */
class RandomSampleWithoutReplacementOperatorHandler : public Runtime::Execution::OperatorHandler {

  public:
    /**
     * @brief Getter for the pagedVectorRef
     * @return Returns the pointer to the pagedVector
     */
    void* getPagedVectorRef();

    /**
     * @brief Initializes the pagedVector
     * @param entrySize: Size of a single tuple/entry
     * @param pageSize: Size of a single page for the PagedVector
     */
    void setup(uint64_t entrySize, uint64_t pageSize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType terminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

  private:
    std::unique_ptr<Nautilus::Interface::PagedVector> pagedVector;


};
} // namespace NES::ASP


#endif//NES_RANDOMSAMPLEWITHOUTREPLACEMENTOPERATORHANDLER_HPP

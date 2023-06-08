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

#ifndef NES_EQUIWIDTH1DHISTOPERATORHANDLER_HPP
#define NES_EQUIWIDTH1DHISTOPERATORHANDLER_HPP

#include <Nautilus/Interface/Fixed2DArray/Fixed2DArray.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <memory>

namespace NES::ASP {

/**
 * @brief Operator handler for a 1-D equi-width histogram, which just stores the bins
 */
class EquiWidth1DHistOperatorHandler : public Runtime::Execution::OperatorHandler {
public:
    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager, uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType terminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Getter for the bins memRef
     * @return void*
     */
    void* getBinsRef();

    /**
     * @brief Initializes the histogram by creating new bins
     * @param entrySize
     * @param numberOfBins
     */
    void setup(uint64_t entrySize, uint64_t numberOfBins);

private:
    std::unique_ptr<Nautilus::Interface::Fixed2DArray> bins;
};
} // namespace NES::ASP

#endif //NES_EQUIWIDTH1DHISTOPERATORHANDLER_HPP

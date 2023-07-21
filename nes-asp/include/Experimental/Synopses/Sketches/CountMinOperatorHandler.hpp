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

#ifndef NES_COUNTMINOPERATORHANDLER_HPP
#define NES_COUNTMINOPERATORHANDLER_HPP

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArray.hpp>


namespace NES::ASP {

/**
 * @brief This class acts as the operator handler for the Count-Min sketch and stores the h3Seeds as well as the sketch array.
 */
class CountMinOperatorHandler : public Runtime::Execution::OperatorHandler {
  public:
    /**
     * @brief Constructor for a Count-Min operator handler object
     * @param numberOfKeyBits: Number of bits for the key
     */
    explicit CountMinOperatorHandler(const uint64_t numberOfKeyBits);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager, uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType terminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Allocates memory for the sketch array and creates the seeds for H3
     * @param numberOfRows: Number of rows for the sketch, the larger the less the probability for an error
     * @param numberOfCols: Number of columns for the sketch, the larger the less relativ error
     * @param entrySize: Datatype size of an entry
     */
    void setup(uint64_t entrySize, uint64_t numberOfRows, uint64_t numberOfCols);

    /**
     * @brief Getter for the sketch array
     * @return Void pointer to the sketch array
     */
    void* getSketchRef();

    /**
     * @brief Getter for the h3 seeds
     * @return Void pointer to the seeds
     */
    void* getH3SeedsRef();

private:
    std::unique_ptr<Nautilus::Interface::Fixed2DArray> sketchArray;
    std::vector<uint64_t> h3Seeds;
    const uint64_t numberOfKeyBits;

};
} // namespace NES::ASP

#endif //NES_COUNTMINOPERATORHANDLER_HPP

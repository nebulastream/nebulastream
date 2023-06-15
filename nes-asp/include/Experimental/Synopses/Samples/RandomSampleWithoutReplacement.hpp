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

#ifndef NES_RANDOMSAMPLEWITHOUTREPLACEMENT_HPP
#define NES_RANDOMSAMPLEWITHOUTREPLACEMENT_HPP

#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/TupleBuffer.hpp>


namespace NES::ASP{

/**
 * @brief Implementation of a Simple Random Sample Without Replacement, as explained
 * in https://www.statisticshowto.com/sampling-with-replacement-without/
 */
class RandomSampleWithoutReplacement : public AbstractSynopsis {

  public:
    /**
     * @brief Constructor for a SampleRandomWithReplacement
     * @param aggregationConfig: Configuration for the aggregation
     * @param sampleSize: Size of the tuples in the sample
     * @param entrySize: Size of a single entry/tuple
     */
    explicit RandomSampleWithoutReplacement(Parsing::SynopsisAggregationConfig& aggregationConfig,
                                            size_t sampleSize, uint64_t entrySize);

    /**
     * @brief Initializes the sample by calling the setup method of the operator handler
     */
    void setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext& ctx) override;

    /**
     * @brief Adds the record to this sample
     * @param handlerIndex: Index for the operator handler
     * @param ctx: The RuntimeExecutionContext
     * @param record: The record that should be processed.
     * @param pState: State that stores the local state
     */
    void addToSynopsis(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx, Nautilus::Record record,
                       Runtime::Execution::Operators::OperatorState *pState) override;

    /**
     * @brief Once we have finished building our sample, we can ask for an approximate
     * @param ctx
     * @param bufferManager
     * @param keyValues
     * @return Record(s) with the approximation
     */
    std::vector<Runtime::TupleBuffer> getApproximate(uint64_t handlerIndex,
                                                     Runtime::Execution::ExecutionContext& ctx,
                                                     std::vector<Nautilus::Value<>>& keyValues,
                                                     Runtime::BufferManagerPtr bufferManager) override;
    /**
     * @brief For now this does not store any local state and therefore returns False #3743
     * @param handlerIndex:Index for the operator handler
     * @param op: Operator for which to store the local state
     * @param ctx: The RuntimeExecutionContext
     * @param buffer: RecordBuffer for this state
     * @return False
     */
    bool storeLocalOperatorState(uint64_t handlerIndex, const Runtime::Execution::Operators::Operator* op,
                                 Runtime::Execution::ExecutionContext &ctx,
                                 Runtime::Execution::RecordBuffer buffer) override;

    /**
     * @brief Deconstructor
     */
    virtual ~RandomSampleWithoutReplacement() = default;


  private:
    /**
     * @brief Calculates the scaling factor from the current number of samples and stored records
     * @param listRef
     * @return Scaling factor
     */
    double getScalingFactor(Nautilus::Interface::PagedVectorRef& pagedVecRef);

    /**
     * @brief Multiplies the approximatedValue with the scalingFactor
     * @param approximatedValue: Current approximated value
     * @param scalingFactor: Value to scale the approximatedValue by
     * @return Multiplied approximated value
     */
    Nautilus::Value<>
    multiplyWithScalingFactor(Nautilus::Value<> approximatedValue, Nautilus::Value<Nautilus::Double> scalingFactor);

    const size_t sampleSize;
    const size_t entrySize;
};
} // namespace NES::ASP

#endif//NES_RANDOMSAMPLEWITHOUTREPLACEMENT_HPP

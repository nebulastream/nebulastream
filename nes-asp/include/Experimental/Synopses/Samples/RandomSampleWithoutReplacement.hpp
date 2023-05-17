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
#include <Runtime/TupleBuffer.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/Interface/Stack/ListRef.hpp>


namespace NES::ASP{

/**
 * @brief Implementation of a Simple Random Sample Without Replacement, as explained
 * in https://www.statisticshowto.com/sampling-with-replacement-without/
 */
class RandomSampleWithoutReplacement : public AbstractSynopsis {

  public:
    /**
     * @brief Constructor for a SampleRandomWithReplacement
     * @param aggregationConfig
     * @param sampleSize
     */
    explicit RandomSampleWithoutReplacement(Parsing::SynopsisAggregationConfig& aggregationConfig,
                                            size_t sampleSize);

    /**
     * @brief Initializes the sample by calling
     */
    void setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext& ctx) override;

    /**
     * @brief Adds the record to this sample
     * @param record
     */
    void addToSynopsis(uint64_t handlerIndex, Runtime::Execution::ExecutionContext& ctx, Nautilus::Record record) override;

    /**
     * @brief Once we have finished building our sample, we can ask for an approximate
     * @param ctx
     * @param bufferManager
     * @return Record(s) with the approximation
     */
    std::vector<Runtime::TupleBuffer> getApproximate(uint64_t handlerIndex,
                                                     Runtime::Execution::ExecutionContext& ctx,
                                                     Runtime::BufferManagerPtr bufferManager) override;

    /**
     * @brief Deconstructor
     */
    virtual ~RandomSampleWithoutReplacement() = default;


  private:
    /**
     * @brief Calculates the scaling factor from the current number of samples and stored records
     * @param stackRef
     * @return Scaling factor
     */
    double getScalingFactor(Nautilus::Interface::ListRef stackRef);

    /**
     * @brief Multiplies the approximatedValue with the scalingFactor
     * @param approximatedValue
     * @param scalingFactor
     * @return Multiplied approximated value
     */
    Nautilus::Value<>
    multiplyWithScalingFactor(Nautilus::Value<> approximatedValue, Nautilus::Value<Nautilus::Double> scalingFactor);

    const size_t sampleSize;
    const size_t recordSize;
};
} // namespace NES::ASP

#endif//NES_RANDOMSAMPLEWITHOUTREPLACEMENT_HPP

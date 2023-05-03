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

#ifndef NES_SIMPLERANDOMSAMPLEWITHOUTREPLACEMENT_HPP
#define NES_SIMPLERANDOMSAMPLEWITHOUTREPLACEMENT_HPP

#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Nautilus/Interface/Stack/StackRef.hpp>
#include <Nautilus/Interface/Stack/Stack.hpp>

namespace NES::ASP{

/**
 * @brief Implementation of a Simple Random Sample Without Replacement, as explained
 * in https://www.statisticshowto.com/sampling-with-replacement-without/
 */
class SimpleRandomSampleWithoutReplacement : public AbstractSynopsis {

  public:
    /**
     * @brief Constructor for a SampleRandomWithReplacement
     * @param aggregationConfig
     * @param sampleSize
     */
    explicit SimpleRandomSampleWithoutReplacement(Parsing::SynopsisAggregationConfig& aggregationConfig,
                                                  size_t sampleSize);

    /**
     * @brief Adds the record to this sample
     * @param record
     */
    void addToSynopsis(Nautilus::Record record) override;

    /**
     * @brief Initializes this synopsis
     */
    void initialize() override;

    /**
     * @brief Once we have finished building our sample, we can ask for an approximate
     * @return Record(s) with the approximation
     */
    std::vector<Runtime::TupleBuffer> getApproximate(Runtime::BufferManagerPtr bufferManager) override;

    /**
     * @brief Deconstructor
     */
    virtual ~SimpleRandomSampleWithoutReplacement() = default;


  private:
    /**
     * @brief Calculates the scaling factor from the current number of samples and stored records
     * @param stackRef
     * @return Scaling factor
     */
    double getScalingFactor(Nautilus::Interface::StackRef stackRef);

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
    std::unique_ptr<Nautilus::Interface::Stack> stack;
};
} // namespace NES::ASP

#endif//NES_SIMPLERANDOMSAMPLEWITHOUTREPLACEMENT_HPP

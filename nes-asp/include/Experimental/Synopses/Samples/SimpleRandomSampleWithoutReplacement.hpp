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

<<<<<<< HEAD:nes-asp/include/Experimental/Synopses/Samples/SimpleRandomSampleWithoutReplacement.hpp
#include <Experimental/Synopses/AbstractSynopsis.hpp>
=======
#include <Synopses/AbstractSynopsis.hpp>
>>>>>>> b77760a39f ([3620] almost done with it. Next step is to add a scaling factor and check if the values in the csv file are correct. Afterwards, add a scaling factor to the Sampling for SUM and COUNT, followed by creating a draft PR so that Ankit and Philipp can take a look.):nes-approx/include/Synopses/Samples/SampleRandomWithoutReplacement.hpp
#include <Runtime/TupleBuffer.hpp>

namespace NES::ASP{

/**
 * @brief Implementation of a Simple Random Sample Without Replacement, as explained in https://www.statisticshowto.com/sampling-with-replacement-without/
 */
class SimpleRandomSampleWithoutReplacement : public AbstractSynopsis {

  public:
    /**
     * @brief Constructor for a SampleRandomWithReplacement
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
     * @return Scaling factor
     */
    double getScalingFactor();

    /**
     * @brief Multiplies the approximatedValue with the scalingFactor
     * @param approximatedValue
     * @param scalingFactor
     * @return Multiplied approximated value
     */
    Nautilus::Value<>
    multiplyWithScalingFactor(Nautilus::Value<> approximatedValue, Nautilus::Value<Nautilus::Double> scalingFactor);

    const size_t sampleSize;
    std::vector<Runtime::TupleBuffer> storedRecords;
    uint64_t numberOfTuples;
};
} // namespace NES::ASP

#endif//NES_SIMPLERANDOMSAMPLEWITHOUTREPLACEMENT_HPP

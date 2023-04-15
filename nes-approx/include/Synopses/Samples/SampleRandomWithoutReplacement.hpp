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

#ifndef NES_SAMPLERANDOMWITHOUTREPLACEMENT_HPP
#define NES_SAMPLERANDOMWITHOUTREPLACEMENT_HPP

#include <Synopses/AbstractSynopsis.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::ASP{

/**
 * @brief Implementation of a  Simple Random Sample With Replacement
 */
class SampleRandomWithoutReplacement : public AbstractSynopsis {

  public:
    /**
     * @brief Constructor for a SampleRandomWithReplacement
     * @param sampleSize
     */
    explicit SampleRandomWithoutReplacement(size_t sampleSize);

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
    virtual ~SampleRandomWithoutReplacement() = default;


  private:
    size_t sampleSize;
    std::vector<Runtime::TupleBuffer> storedRecords;
    uint64_t numberOfTuples;

};
} // namespace NES::ASP

#endif//NES_SAMPLERANDOMWITHOUTREPLACEMENT_HPP

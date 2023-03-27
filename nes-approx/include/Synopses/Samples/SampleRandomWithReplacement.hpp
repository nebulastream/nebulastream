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

#ifndef NES_SAMPLERANDOMWITHREPLACEMENT_HPP
#define NES_SAMPLERANDOMWITHREPLACEMENT_HPP

#include <Synopses/AbstractSynopsis.hpp>
#include <Synopses/SynopsisArguments.hpp>

namespace NES::ASP{

class SampleRandomWithReplacement : public AbstractSynopsis {

  public:
    void addToSynopsis(Nautilus::Record record) override;

    std::vector<Nautilus::Record> getApproximate() override;

    virtual ~SampleRandomWithReplacement() = default;

    SampleRandomWithReplacement(Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction,
                                size_t sampleSize)
        : AbstractSynopsis(aggregationFunction), sampleSize(sampleSize) {}


  private:
    size_t sampleSize;
    std::vector<Nautilus::Record> storedRecords;

};

SynopsisArguments
SRSWR(size_t sampleSize, std::string fieldName,
                        Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction);

} // namespace NES::ASP

#endif//NES_SAMPLERANDOMWITHREPLACEMENT_HPP

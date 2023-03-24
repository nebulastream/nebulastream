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

#ifndef NES_SYNOPSESARGUMENTS_HPP
#define NES_SYNOPSESARGUMENTS_HPP

#include <cstddef>
#include <stdint.h>
#include <Execution/Aggregation/AggregationFunction.hpp>

namespace NES::ASP {

class SynopsesArguments {

  public:
    enum class Type : uint8_t {
        SRSWR,  // Simple Random Sampling With Replacement
        SRSWoR, // Simple Random Sampling Without Replacement
        Poisson, // Poisson sampling
        Stratified, // Stratified Sampling
        ResSamp,    // Reservoir Sampling

        EqWdHist, // Equi-Width Histogram,
        EqDpHist, // Equi-Depth Histogram,
        vOptHist, // v-Optimal Histograms

        HaarWave, // Haar Wavelet

        CM,         // Count-Min Sketch
        ECM,        // Exponential Count-Min Sketch
        HLL,        // HyperLogLog
    };

    static SynopsesArguments createArguments(Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction,
                                             std::string fieldName, Type type, size_t width, size_t height = 1, size_t windowSize = 1);

    static std::vector<SynopsesArguments> parseArgumentsFromYamlFile(const std::string& fileName);



    size_t getWidth() const;

    size_t getHeight() const;

    size_t getWindowSize() const;

    Type getType() const;

    const std::string& getFieldName() const;

    Runtime::Execution::Aggregation::AggregationFunctionPtr getAggregationFunction() const;

  private:
    SynopsesArguments(Type type, size_t width, size_t height, size_t windowSize, std::string fieldName,
                      Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction);

  private:
    Type type;
    size_t width;
    size_t height;
    size_t windowSize;
    std::string fieldNameAggregation;
    std::vector<std::string> fieldNameKeys; // TODO add here multiple keys for each synopsis
    Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction;
};

} // namespace NES::ASP

#endif//NES_SYNOPSESARGUMENTS_HPP

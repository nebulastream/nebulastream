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

#ifndef NES_SYNOPSISARGUMENTS_HPP
#define NES_SYNOPSISARGUMENTS_HPP

#include <cstddef>
#include <stdint.h>
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES::ASP {

class SynopsisArguments {

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

    static SynopsisArguments createArguments(Type type, size_t width, size_t height = 1,
                                             size_t windowSize = 1);

    static SynopsisArguments createArgumentsFromYamlNode(const Yaml::Node& synopsisArgumentsNode);


    size_t getWidth() const;

    size_t getHeight() const;

    size_t getWindowSize() const;

    Type getType() const;

  private:
    SynopsisArguments(Type type, size_t width, size_t height, size_t windowSize);

  private:
    Type type;
    size_t width;
    size_t height;
    size_t windowSize;

};

} // namespace NES::ASP

#endif//NES_SYNOPSISARGUMENTS_HPP

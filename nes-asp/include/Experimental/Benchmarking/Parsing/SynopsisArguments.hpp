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

    /**
     * @brief Default constructor
     */
    SynopsisArguments() = default;

    /**
     * @brief Factory method for creating a synopsis argument
     * @param type
     * @param width
     * @param height
     * @param windowSize
     * @return SynopsisArguments
     */
    static SynopsisArguments createArguments(Type type, size_t width, size_t height = 1,
                                             size_t windowSize = 1);

    /**
     * @brief Factory method for creating a synopsis argument from a yaml node
     * @param synopsisArgumentsNode
     * @return SynopsisArguments
     */
    static SynopsisArguments createArgumentsFromYamlNode(Yaml::Node& synopsisArgumentsNode);

    /**
     * @brief Creates a header for the output csv file from this SynopsisArguments
     * @return Header as a string with comma separated values
     */
    std::string getHeaderAsCsv();

    /**
     * @brief Creates a csv string for all values from this SynopsisArguments
     * @return Header as a string with comma separated values
     */
    std::string getValuesAsCsv();

    /**
     * @brief Getter for the width of a synopsis
     * @return Synopsis width
     */
    size_t getWidth() const;

    /**
     * @brief Getter for the height of a synopsis
     * @return Synopsis height
     */
    size_t getHeight() const;

    /**
     * @brief Getter for the window size of a synopsis
     * @return Synopsis window size
     */
    size_t getWindowSize() const;

    /**
     * @brief Getter for the type of a synopsis
     * @return Synopsis type
     */
    Type getType() const;

    /**
     * @brief Creates a string representation
     * @return String representation
     */
    std::string toString();

  private:
    /**
     * @brief Private constructor for a SynopsisArguments object. For creating an object, the factory method should be used
     * @param type
     * @param width
     * @param height
     * @param windowSize
     */
    SynopsisArguments(Type type, size_t width, size_t height, size_t windowSize);

    Type type;
    size_t width;
    size_t height;
    size_t windowSize;

};

} // namespace NES::ASP

#endif//NES_SYNOPSISARGUMENTS_HPP

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

#ifndef NES_SYNOPSISCONFIGURATION_HPP
#define NES_SYNOPSISCONFIGURATION_HPP

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/yaml/Yaml.hpp>
#include <cstddef>
#include <stdint.h>

namespace NES::ASP {

class SynopsisConfiguration;
using SynopsisConfigurationPtr = std::shared_ptr<SynopsisConfiguration>;

class SynopsisConfiguration : public Configurations::BaseConfiguration {

  public:
    enum class Synopsis_Type : uint8_t {
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
        NONE        // NONE
    };

    Configurations::EnumOption<Synopsis_Type> type = {Configurations::SYNOPSIS_CONFIG_TYPE,
                                                      Synopsis_Type::NONE, "Type of synopsis."};
    Configurations::SizeTOption width = {Configurations::SYNOPSIS_CONFIG_WIDTH, 1, "Width of the synopsis."};
    Configurations::SizeTOption height = {Configurations::SYNOPSIS_CONFIG_HEIGHT, 1, "Height of the synopsis."};
    Configurations::SizeTOption windowSize = {Configurations::SYNOPSIS_CONFIG_WINDOWSIZE, 1, "WindowSize of the synopsis."};

    /**
     * @brief Factory method for creating a synopsis argument
     * @param type
     * @param width
     * @param height
     * @param windowSize
     * @return SynopsisArguments
     */
    static SynopsisConfigurationPtr create(Synopsis_Type type, size_t width, size_t height = 1,
                                                         size_t windowSize = 1);

    /**
     * @brief Factory method for creating a synopsis argument from a yaml node
     * @param synopsisArgumentsNode
     * @return SynopsisConfiguration
     */
    static SynopsisConfigurationPtr createArgumentsFromYamlNode(Yaml::Node& synopsisArgumentsNode);

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
     * @brief Creates a string representation
     * @return String representation
     */
    std::string toString() override
        ;

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&type,
                &width,
                &height,
                &windowSize};
    }

};


} // namespace NES::ASP

#endif//NES_SYNOPSISCONFIGURATION_HPP

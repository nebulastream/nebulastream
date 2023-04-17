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

#ifndef NES_YAMLAGGREGATION_HPP
#define NES_YAMLAGGREGATION_HPP

#include <API/Schema.hpp>
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>

namespace NES::ASP::Benchmarking {

using AggregationValuePtr = std::unique_ptr<Runtime::Execution::Aggregation::AggregationValue>;

enum class AGGREGATION_TYPE : uint8_t { NONE, MIN, MAX, SUM, AVERAGE, COUNT };
class YamlAggregation {


  public:
    /**
     * @brief Default constructor
     */
    YamlAggregation() = default;

    YamlAggregation(const YamlAggregation& other);
    YamlAggregation& operator=(const YamlAggregation& other);

    /**
     * @brief Custom constructor
     * @param type
     * @param fieldNameAggregation
     * @param inputFile
     * @param inputSchema
     */
    YamlAggregation(const AGGREGATION_TYPE& type,
                    const std::string& fieldNameAggregation,
                    const std::string& fieldNameApproximate,
                    const std::string& timestampFieldName,
                    const std::string& inputFile,
                    const SchemaPtr& inputSchema,
                    const SchemaPtr& outputSchema);

    /**
     * @brief Creates an YamlAggregation object from a yaml node
     * @param aggregationNode
     * @param data is needed for providing an absolute path to the input file
     * @return YamlAggregation
     */
    static YamlAggregation createAggregationFromYamlNode(Yaml::Node& aggregationNode,
                                                         const std::filesystem::path& data);

    /**
     * @brief Creates a string representation
     * @return String representation
     */
    std::string toString();

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
     * @brief Creates an aggregation function from the current parameters
     * @return AggregationFunction
     */
    Runtime::Execution::Aggregation::AggregationFunctionPtr createAggregationFunction();

    /**
     * @brief Creates an aggregation value from the current parameters
     * @return AggregationValue
     */
    AggregationValuePtr createAggregationValue();


  private:
    /**
     * @brief Creates an aggregation value from the current parameters for a min aggregation function
     * @return AggregationValue
     */
    AggregationValuePtr createAggregationValueMin();

    /**
     * @brief Creates an aggregation value from the current parameters for a max aggregation function
     * @return AggregationValue
     */
    AggregationValuePtr createAggregationValueMax();

    /**
     * @brief Creates an aggregation value from the current parameters for a count aggregation function
     * @return AggregationValue
     */
    AggregationValuePtr createAggregationValueCount();

    /**
     * @brief Creates an aggregation value from the current parameters for an average aggregation function
     * @return AggregationValue
     */
    AggregationValuePtr createAggregationValueAverage();

    /**
     * @brief Creates an aggregation value from the current parameters for a sum aggregation function
     * @return AggregationValue
     */
    AggregationValuePtr createAggregationValueSum();

  public:
    AGGREGATION_TYPE type;
    std::string fieldNameAggregation;
    std::string fieldNameApproximate;
    std::string timeStampFieldName;
    std::string inputFile;
    SchemaPtr inputSchema;
    SchemaPtr outputSchema;
};
} // namespace NES::ASP::Benchmarking


#endif//NES_YAMLAGGREGATION_HPP

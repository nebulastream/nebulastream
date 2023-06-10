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

#ifndef NES_SYNOPSISAGGREGATIONCONFIG_HPP
#define NES_SYNOPSISAGGREGATIONCONFIG_HPP

#include <API/Schema.hpp>
#include <Configurations/BaseConfiguration.hpp>
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>

namespace NES::ASP::Parsing {

using AggregationValuePtr = std::unique_ptr<Runtime::Execution::Aggregation::AggregationValue>;
enum class Aggregation_Type : uint8_t { NONE, MIN, MAX, SUM, AVERAGE, COUNT };

/**
 * @brief This class encapsulates the necessary stuff for parsing the aggregation from a yaml file.
 */
class SynopsisAggregationConfig {
  public:
    /**
     * @brief Default constructor
     */
    SynopsisAggregationConfig() = default;

    /**
     * @brief Copy constructor
     * @param other
     */
    SynopsisAggregationConfig(const SynopsisAggregationConfig& other);

    /**
     * @brief Assignment operator
     * @param other
     * @return
     */
    SynopsisAggregationConfig& operator=(const SynopsisAggregationConfig& other);

    /**
     * @brief Creates an SynopsisAggregationConfig object from a yaml node
     * @param aggregationNode
     * @param data is needed for providing an absolute path to the input file
     * @return SynopsisAggregationConfig and input file
     */
    static std::pair<SynopsisAggregationConfig, std::string> createAggregationFromYamlNode(Yaml::Node& aggregationNode,
                                                                                           const std::filesystem::path& data);

    /**
     * @brief Creates an aggregation config for a synopsis
     * @param type
     * @param fieldNameAggregation
     * @param fieldNameApproximate
     * @param timestampFieldName
     * @param inputSchema
     * @param outputSchema
     * @return SynopsisAggregationConfig
     */
    static SynopsisAggregationConfig create(const Aggregation_Type& type,
                                            const std::string& fieldNameKey,
                                            const std::string& fieldNameAggregation,
                                            const std::string& fieldNameApproximate,
                                            const std::string& timestampFieldName,
                                            const SchemaPtr& inputSchema,
                                            const SchemaPtr& outputSchema);


    /**
     * @brief Creates a string representation
     * @return String representation
     */
    const std::string toString() const;

    /**
     * @brief Creates a header for the output csv file from this SynopsisArguments
     * @return Header as a string with comma separated values
     */
    const std::string getHeaderAsCsv() const;

    /**
     * @brief Creates a csv string for all values from this SynopsisArguments
     * @return Header as a string with comma separated values
     */
    const std::string getValuesAsCsv() const;

    /**
     * @brief Creates an aggregation function from the current parameters
     * @return AggregationFunction
     */
    Runtime::Execution::Aggregation::AggregationFunctionPtr createAggregationFunction();

    /**
     * @brief Gets the physical type of the input
     * @return PhysicalTypePtr
     */
    PhysicalTypePtr getInputType() const;

    /**
     * @brief Gets the physical type of the final output
     * @return PhysicalTypePtr
     */
    PhysicalTypePtr getFinalType() const;

    /**
     * @brief Gets a ReadFieldExpression for reading the aggregation value
     * @return Shared pointer of type ReadFieldExpression
     */
    std::shared_ptr<Runtime::Execution::Expressions::ReadFieldExpression> getReadFieldAggregationExpression() const;

    /**
     * @brief Gets a ReadFieldExpression for reading the key value
     * @return Shared pointer of type ReadFieldExpression
     */
    std::shared_ptr<Runtime::Execution::Expressions::ReadFieldExpression> getReadFieldKeyExpression() const;




  private:

    /**
     * @brief Custom constructor
     * @param type
     * @param fieldNameAggregation
     * @param fieldNameApproximate
     * @param timestampFieldName
     * @param inputSchema
     * @param outputSchema
     */
    explicit SynopsisAggregationConfig(const Aggregation_Type& type,
                                       const std::string& fieldNameKey,
                                       const std::string& fieldNameAggregation,
                                       const std::string& fieldNameApproximate,
                                       const std::string& timestampFieldName,
                                       const SchemaPtr& inputSchema,
                                       const SchemaPtr& outputSchema);

  public:
    Aggregation_Type type;
    std::string fieldNameKey;
    std::string fieldNameAggregation;
    std::string fieldNameApproximate;
    std::string timeStampFieldName;
    SchemaPtr inputSchema;
    SchemaPtr outputSchema;
};
} // namespace NES::ASP::Parsing


#endif//NES_SYNOPSISAGGREGATIONCONFIG_HPP

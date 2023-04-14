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

#ifndef NES_ABSTRACTSYNOPSIS_HPP
#define NES_ABSTRACTSYNOPSIS_HPP

#include <Benchmarking/Parsing/YamlAggregation.hpp>
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Benchmarking/Parsing/SynopsisArguments.hpp>

namespace NES::ASP {

class AbstractSynopsis;
using AbstractSynopsesPtr = std::shared_ptr<AbstractSynopsis>;

constexpr auto GENERATOR_SEED_DEFAULT = 42;

/**
 * @brief The synopsis does not take care of any window semantics. This is done in the AbstractSynopsesWindow class, which will be
 * added in issue #3628
 */
class AbstractSynopsis {

  public:
    /**
     * @brief This is the first step of receiving an approximation. This method adds the record to the underlying synopsis
     * @param record
     */
    virtual void addToSynopsis(Nautilus::Record record) = 0;

    /**
     * @brief Once all records have been inserted, we can ask for an approximation
     * @return Record that stores the approximation
     */
    virtual std::vector<Runtime::TupleBuffer> getApproximate(Runtime::BufferManagerPtr bufferManager) = 0;

    /**
     * @brief Initializes the synopsis. This means that the synopsis should create a state in which a new approximation
     * can be done. Couple examples are:
     *  - removing all drawn samples for a sampling algorithm
     *  - resetting and writing zeros to the 2D array for a Count-min sketch
     */
    virtual void initialize() = 0;

    /**
     * @brief Creates the synopsis from the SynopsisArguments
     * @param arguments
     * @return AbstractSynopsesPtr
     */
    static AbstractSynopsesPtr create(SynopsisArguments arguments);

    /**
     * @brief Sets the aggregation function for this synopsis
     * @param aggregationFunction
     */
    void setAggregationFunction(const Runtime::Execution::Aggregation::AggregationFunctionPtr& aggregationFunction);

    /**
     * @brief Sets the aggregation value for this synopsis
     */
    void setAggregationValue(Benchmarking::AggregationValuePtr aggregationValue);

    /**
     * @brief Sets the fieldname for this aggregation
     * @param fieldNameAggregation
     */
    void setFieldNameAggregation(const std::string& fieldNameAggregation);

    /**
     * @brief Sets the fieldname of the output
     * @param fieldNameOutput
     */
    void setFieldNameApproximate(const std::string& fieldNameOutput);

    /**
     * @brief Sets the outputSchema
     * @param outputSchema
     */
    void setOutputSchema(const SchemaPtr& outputSchema);

    /**
     * @brief virtual deconstructor
     */
    virtual ~AbstractSynopsis() = default;

  protected:
    Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction;
    Benchmarking::AggregationValuePtr aggregationValue;
    std::string fieldNameAggregation;
    std::string fieldNameApproximate;
    SchemaPtr outputSchema;

};
} // namespace NES::ASP

#endif//NES_ABSTRACTSYNOPSIS_HPP

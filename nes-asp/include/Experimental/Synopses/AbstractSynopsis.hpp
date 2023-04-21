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

#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <Experimental/Parsing/SynopsisConfiguration.hpp>
#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Nautilus/Interface/Record.hpp>

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
     * @brief Creating an AbstractSynopsis
     * @param aggregationConfig
     * @param bufferManager
     */
    explicit AbstractSynopsis(Parsing::SynopsisAggregationConfig& aggregationConfig);

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
     * @param aggregationConfig
     * @return AbstractSynopsesPtr
     */
    static AbstractSynopsesPtr create(Parsing::SynopsisConfiguration& arguments,
                                      Parsing::SynopsisAggregationConfig& aggregationConfig);

    /**
     * @brief Sets the bufferManager
     * @param bufferManager
     */
    void setBufferManager(const Runtime::BufferManagerPtr& bufferManager);

    /**
     * @brief virtual deconstructor
     */
    virtual ~AbstractSynopsis() = default;

  protected:
    Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction;
    Parsing::AggregationValuePtr aggregationValue;
    std::string fieldNameAggregation;
    std::string fieldNameApproximate;
    SchemaPtr inputSchema;
    SchemaPtr outputSchema;
    Runtime::BufferManagerPtr bufferManager;

};
} // namespace NES::ASP

#endif//NES_ABSTRACTSYNOPSIS_HPP

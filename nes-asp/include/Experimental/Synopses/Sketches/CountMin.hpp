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

#ifndef NES_COUNTMIN_HPP
#define NES_COUNTMIN_HPP

#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArrayRef.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>


namespace NES::ASP {
/**
 * @brief This class implements a Count-Min sketch as explained in http://dimacs.rutgers.edu/~graham/pubs/papers/cmencyc.pdf
 */
class CountMin : public AbstractSynopsis {
  public:
    /**
     * @brief This class acts as a simple storage container for the sketch array and the h3Seeds
     */
    class LocalCountMinState : public Runtime::Execution::Operators::OperatorState {
      public:
        LocalCountMinState(const Nautilus::Interface::Fixed2DArrayRef &sketchArray,
                           const Nautilus::Value<Nautilus::MemRef> &h3SeedsMemRef) : sketchArray(sketchArray),
                                                                                     h3SeedsMemRef(h3SeedsMemRef) {}


        Nautilus::Interface::Fixed2DArrayRef sketchArray;
        Nautilus::Value<Nautilus::MemRef> h3SeedsMemRef;
    };

    /**
     * @brief Constructor for a count-min sketch
     * @param aggregationConfig: Config related to the aggregation
     * @param numberOfRows: Number of rows for the sketch, the larger the less the probability for an error
     * @param numberOfCols: Number of columns for the sketch, the larger the less relativ error
     * @param entrySize: Datatype size of an entry
     */
    CountMin(Parsing::SynopsisAggregationConfig &aggregationConfig,
             const uint64_t numberOfRows, const uint64_t numberOfCols, const uint64_t entrySize);

    /**
     * @brief Adds the record to the sketch by selecting a column for each row and executing the
     * @param handlerIndex: Index for the operator handler
     * @param ctx: The RuntimeExecutionContext
     * @param record: Input record
     * @param pState: State that contains the sketch array and the h3 seeds
     */
    void addToSynopsis(const uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx, Nautilus::Record record,
                       const Runtime::Execution::Operators::OperatorState *pState) override;

    /**
     * @brief Retrieves the approximated value by iterating over each row and combining the values of all rows
     * @param handlerIndex: Index for the operator handler
     * @param ctx: Execution context that stores the bins
     * @param keys: Keys for which to approximate
     * @param outputRecord: Record that should store the approximation
     */
    void getApproximateRecord(const uint64_t handlerIndex, Runtime::Execution::ExecutionContext& ctx, const Nautilus::Value<>& key,
                             Nautilus::Record& outputRecord) override;

    /**
     * @brief Sets the count-min sketch up, by calling the setup method of the operator handler, as well as resetting the
     * values in the cells with the aggregation function
     * @param handlerIndex: Index for the operator handler
     * @param ctx: Execution context that stores the bins
     */
    void setup(const uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) override;

    /**
     * @brief Allows the count-min to store a memRef to the sketch array and a memref to the h3 seed
     * @param handlerIndex: Index for the operator handler
     * @param op: Synopses operator that should contain the local state
     * @param ctx: Execution context that stores the bins
     * @param buffer: Current record buffer
     */
    bool storeLocalOperatorState(const uint64_t handlerIndex, const Runtime::Execution::Operators::Operator* op,
                                 Runtime::Execution::ExecutionContext &ctx) override;

  private:
    const uint64_t numberOfRows;
    const uint64_t numberOfCols;
    const uint64_t entrySize;
    std::unique_ptr<Nautilus::Interface::HashFunction> h3HashFunction;
    Runtime::Execution::Aggregation::AggregationFunctionPtr combineRowsApproximate;
};
} // namespace NES::ASP

#endif //NES_COUNTMIN_HPP

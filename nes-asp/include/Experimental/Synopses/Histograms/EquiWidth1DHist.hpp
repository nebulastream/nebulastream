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

#ifndef NES_EQUIWIDTH1DHIST_HPP
#define NES_EQUIWIDTH1DHIST_HPP

#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArrayRef.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>

namespace NES::ASP {

/**
 * @brief Implementation of a 1-D equi-width histogram
 */
class EquiWidth1DHist : public AbstractSynopsis {
public:
    /**
     * @brief This class acts as a simple storage container for the local state, which contains the memref to the bins
     */
    class LocalBinsOperatorState : public Runtime::Execution::Operators::OperatorState {
      public:
        explicit LocalBinsOperatorState(const Nautilus::Interface::Fixed2DArrayRef& binsRef) : bins(binsRef) {}
        Nautilus::Interface::Fixed2DArrayRef bins;
    };

    /**
     * @brief Constructor for an equi-width one-dimensional histogram
     * @param aggregationConfig: Config related to the aggregation
     * @param entrySize: Datatype size of an entry
     * @param minValue: Minimum value for this histogram
     * @param maxValue: Maximum value for this histogram
     * @param numberOfBins: Number of bins for this histogram
     * @param lowerBinBoundString: Name of the lower bound field, which stores the lower bound of the bin corresponding to a key query
     * @param upperBinBoundString: Name of the upper bound field, which stores the upper bound of the bin corresponding to a key query
     */
    EquiWidth1DHist(Parsing::SynopsisAggregationConfig& aggregationConfig, const uint64_t entrySize,
                    const int64_t minValue, const int64_t maxValue, const uint64_t numberOfBins,
                    const std::string& lowerBinBoundString, const std::string& upperBinBoundString);

    /**
     * @brief Adds the record to the histogram, by first calculating the position and then calling the aggregation function
     * @param handlerIndex: Index for the operator handler
     * @param ctx: The RuntimeExecutionContext
     * @param record: Input record
     * @param pState: State that contains the memRef to the bins
     */
    void addToSynopsis(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx, Nautilus::Record record,
                       Runtime::Execution::Operators::OperatorState *pState) override;

    /**
     * @brief Calculates the position for each key and then the lower operation on all bins and writes each bin into a record
     * @param handlerIndex: Index for the operator handler
     * @param ctx: Execution context that stores the bins
     * @param bufferManager: Buffermanager to allocate return buffers
     * @param keys: Keys for which to approximate
     * @return Vector of TupleBuffers, which contain the approximation for the keys
     */
    std::vector<Runtime::TupleBuffer> getApproximate(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx,
                                                     std::vector<Nautilus::Value<>>& keys,
                                                     Runtime::BufferManagerPtr bufferManager) override;

    /**
     * @brief Sets the histogram up, by calling the setup method of the operator handler, as well as resetting the
     * values in the bins with the aggregation function
     * @param handlerIndex: Index for the operator handler
     * @param ctx: Execution context that stores the bins
     */
    void setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) override;

    /**
     * @brief Allows the histogram to store something in the state of the synopsis operator, for example a memref to bins
     * @param handlerIndex: Index for the operator handler
     * @param op: Synopses operator that should contain the local state
     * @param ctx: Execution context that stores the bins
     * @param buffer: Current record buffer
     */
    bool storeLocalOperatorState(uint64_t handlerIndex, const Runtime::Execution::Operators::Operator* op,
                                 Runtime::Execution::ExecutionContext &ctx,
                                 Runtime::Execution::RecordBuffer buffer) override;

private:
    const int64_t minValue;
    const int64_t maxValue;
    const uint64_t numberOfBins;
    const uint64_t binWidth;
    const uint64_t entrySize;

    const std::string lowerBinBoundString;
    const std::string upperBinBoundString;
};
} // namespace NES::ASP

#endif //NES_EQUIWIDTH1DHIST_HPP

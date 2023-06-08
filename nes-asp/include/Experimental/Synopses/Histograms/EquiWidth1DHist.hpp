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
class EquiWidth1DHist : public AbstractSynopsis {

public:

    class LocalBinsOperatorState : public Runtime::Execution::Operators::OperatorState {
      public:
        explicit LocalBinsOperatorState(const Nautilus::Interface::Fixed2DArrayRef& binsRef) : bins(binsRef) {}

        Nautilus::Interface::Fixed2DArrayRef bins;
    };

    EquiWidth1DHist(Parsing::SynopsisAggregationConfig& aggregationConfig, const uint64_t entrySize,
                    const int64_t minValue, const int64_t maxValue, const uint64_t numberOfBins,
                    const std::string& lowerBinBoundString, const std::string& upperBinBoundString,
                    std::unique_ptr<Runtime::Execution::Expressions::ReadFieldExpression> readBinDimension);

    void addToSynopsis(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx, Nautilus::Record record,
                       Runtime::Execution::Operators::OperatorState *pState) override;

    std::vector<Runtime::TupleBuffer> getApproximate(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx,
                                                     Runtime::BufferManagerPtr bufferManager) override;

    void setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) override;

    void storeLocalOperatorState(uint64_t handlerIndex, const Runtime::Execution::Operators::Operator* op,
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
    std::unique_ptr<Runtime::Execution::Expressions::ReadFieldExpression> readBinDimension;
};
} // namespace NES::ASP

#endif //NES_EQUIWIDTH1DHIST_HPP

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
class CountMin : public AbstractSynopsis {
  public:

    class LocalCountMinState : public Runtime::Execution::Operators::OperatorState {
      public:
        LocalCountMinState(const Nautilus::Interface::Fixed2DArrayRef &sketchArray,
                           const Nautilus::Value<Nautilus::MemRef> &h3SeedsMemRef) : sketchArray(sketchArray),
                                                                                     h3SeedsMemRef(h3SeedsMemRef) {}


        Nautilus::Interface::Fixed2DArrayRef sketchArray;
        Nautilus::Value<Nautilus::MemRef> h3SeedsMemRef;
    };

    CountMin(Parsing::SynopsisAggregationConfig &aggregationConfig,
             const uint64_t numberOfRows, const uint64_t numberOfCols, const uint64_t entrySize,
             const uint64_t keySizeInB, const std::string &keyFieldNameString);

    void addToSynopsis(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx, Nautilus::Record record,
                       Runtime::Execution::Operators::OperatorState *pState) override;

    std::vector<Runtime::TupleBuffer> getApproximate(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx,
                                                     std::vector<Nautilus::Value<>>& keyValues,
                                                     Runtime::BufferManagerPtr bufferManager) override;

    void setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) override;

    bool storeLocalOperatorState(uint64_t handlerIndex, const Runtime::Execution::Operators::Operator* op,
                                 Runtime::Execution::ExecutionContext &ctx,
                                 Runtime::Execution::RecordBuffer buffer) override;


  private:
    const uint64_t numberOfRows;
    const uint64_t numberOfCols;
    const uint64_t entrySize;

    const uint64_t keySizeInB;
    const std::string keyFieldNameString;

    std::unique_ptr<Nautilus::Interface::HashFunction> h3HashFunction;
    Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunctionMergeRows;
};
} // namespace NES::ASP

#endif //NES_COUNTMIN_HPP

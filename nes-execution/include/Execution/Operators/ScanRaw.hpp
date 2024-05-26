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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_SCANRAW_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_SCANRAW_HPP_
#include <DataParser/Parser.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Operator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief
 */
class ScanRaw : public Operator {
  public:
    ScanRaw(SchemaPtr schema,
            std::unique_ptr<NES::DataParser::Parser>&& parser,
            std::vector<Nautilus::Record::RecordFieldIdentifier> projections = {});

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

  private:
    SchemaPtr schema;
    const std::vector<Nautilus::Record::RecordFieldIdentifier> projections;
    std::unique_ptr<NES::DataParser::Parser> parser;
    std::vector<Value<MemRef>> tupleData;
    Value<MemRef> parserRef;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_SCANRAW_HPP_

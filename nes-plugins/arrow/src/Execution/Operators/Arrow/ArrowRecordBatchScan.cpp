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

#include <Execution/Operators/Arrow/ArrowFieldReader.hpp>
#include <Execution/Operators/Arrow/ArrowRecordBatchScan.hpp>
#include <Execution/Operators/Arrow/RecordBufferWrapper.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <arrow/api.h>
#include <utility>

namespace NES::Runtime::Execution::Operators {

uint64_t getBatchSize(void* ptr) {
    auto wrapper = (RecordBufferWrapper*) ptr;
    return wrapper->batch->num_rows();
}

ArrowRecordBatchScan::ArrowRecordBatchScan(const std::vector<std::shared_ptr<AbstractArrowFieldReader>>& readers)
    : readers(std::move(readers)) {}

ArrowRecordBatchScan::ArrowRecordBatchScan(const NES::SchemaPtr& schema)
    : ArrowRecordBatchScan(createArrowFieldReaderFromSchema(schema)) {}

void ArrowRecordBatchScan::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // initialize global state variables to keep track of the watermark ts and the origin id
    ctx.setWatermarkTs(recordBuffer.getWatermarkTs());
    ctx.setOrigin(recordBuffer.getOriginId());
    ctx.setSequenceNumber(recordBuffer.getSequenceNr());
    // call open on all child operators
    child->open(ctx, recordBuffer);

    // iterate over records in buffer
    auto recordBatch = recordBuffer.getBuffer();

    std::vector<Value<>> columns;
    for (const auto& reader : readers) {
        columns.emplace_back(reader->getColumn(recordBatch));
    }
    auto numberOfRecords = FunctionCall("getBatchSize", getBatchSize, recordBatch);
    for (Value<UInt64> i = 0_u64; i < numberOfRecords; i = i + 1_u64) {
        Record record;
        for (auto index = 0_u64; index < readers.size(); index++) {
            auto value = readers[index]->getValue(columns[index].as<MemRef>(), i);
            record.write(readers[index]->fieldName, value);
        }
        child->execute(ctx, record);
    }
}

}// namespace NES::Runtime::Execution::Operators

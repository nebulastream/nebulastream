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

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Experimental/Synopses/Samples/RandomSampleWithoutReplacement.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::ASP {

AbstractSynopsesPtr AbstractSynopsis::create(Parsing::SynopsisConfiguration& arguments,
                                             Parsing::SynopsisAggregationConfig& aggregationConfig) {

    // TODO For now this is okay, but later on we want to have a separate factory class, see issue #3734
    if (arguments.type.getValue() == Parsing::Synopsis_Type::SRSWoR) {
        auto entrySize = aggregationConfig.inputSchema->getSchemaSizeInBytes();
        auto pageSize = Nautilus::Interface::PagedVector::PAGE_SIZE;
        return std::make_shared<RandomSampleWithoutReplacement>(aggregationConfig, arguments.width.getValue(),
                                                                entrySize, pageSize);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

std::vector<Runtime::TupleBuffer> AbstractSynopsis::getApproximateForKeys(const uint64_t handlerIndex,
                                                                          Runtime::Execution::ExecutionContext& ctx,
                                                                          const std::vector<Nautilus::Value<>>& keys,
                                                                          Runtime::BufferManagerPtr bufferManager) {
    using namespace Runtime::Execution;

    std::vector<Runtime::TupleBuffer> retTupleBuffers;

    auto buffer = bufferManager->getBufferBlocking();
    auto maxRecordsPerBuffer = bufferManager->getBufferSize() / outputSchema->getSchemaSizeInBytes();
    auto memoryProviderOutput = MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(),
                                                                                     outputSchema);
    Nautilus::Value<Nautilus::UInt64> recordIndex((uint64_t) 0);
    for (auto& key : keys) {
        // Retrieving the approximation for the record
        Nautilus::Record record;
        getApproximateRecord(handlerIndex, ctx, key, record);

        // Writing the values to the buffer
        auto recordBuffer = RecordBuffer(Nautilus::Value<Nautilus::MemRef>((int8_t*) std::addressof(buffer)));
        auto bufferAddress = recordBuffer.getBuffer();
        memoryProviderOutput->write(recordIndex, bufferAddress, record);
        recordIndex = recordIndex + 1;
        recordBuffer.setNumRecords(recordIndex);

        if (recordIndex >= maxRecordsPerBuffer) {
            retTupleBuffers.emplace_back(buffer);
            buffer = bufferManager->getBufferBlocking();
            recordIndex = (uint64_t) 0;
        }
    }

    if (recordIndex > 0) {
        retTupleBuffers.emplace_back(buffer);
    }

    return retTupleBuffers;
}

AbstractSynopsis::AbstractSynopsis(Parsing::SynopsisAggregationConfig& aggregationConfig)
    : readKeyExpression(aggregationConfig.getReadFieldKeyExpression()),
    aggregationFunction(aggregationConfig.createAggregationFunction()), aggregationType(aggregationConfig.type),
    fieldNameKey(aggregationConfig.fieldNameKey), fieldNameAggregation(aggregationConfig.fieldNameAggregation),
    fieldNameApproximate(aggregationConfig.fieldNameApproximate), inputSchema(aggregationConfig.inputSchema),
    outputSchema(aggregationConfig.outputSchema) {}

} // namespace NES::ASP
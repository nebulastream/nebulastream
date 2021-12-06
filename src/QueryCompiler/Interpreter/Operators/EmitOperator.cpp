/*

     Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
//
// Created by pgrulich on 27.08.21.
//

#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <QueryCompiler/Interpreter/Operators/EmitOperator.hpp>
#include <QueryCompiler/Interpreter/Operators/OperatorContext.hpp>
#include <QueryCompiler/Interpreter/Record.hpp>
#include <QueryCompiler/Interpreter/RecordBuffer.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt32.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt64.hpp>
#include <QueryCompiler/Interpreter/Values/NesMemoryAddress.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <memory>
namespace NES::QueryCompilation {

EmitOperator::EmitOperator(SchemaPtr outputSchema) : ExecutableOperator(nullptr), outputSchema(outputSchema) {}

void EmitOperator::execute(RecordPtr , ExecutionContextPtr ) const {
   // auto localState = ctx->getThreadLocalOperator<EmitOperatorState>(shared_from_this());
   // auto physicalFieldFactory = DefaultPhysicalTypeFactory();
   // int64_t fieldOffset = 0;
    //for (uint64_t i = 0; i < outputSchema->fields.size(); i++) {
        //auto field = outputSchema->fields[i];
        //auto value = record->read(field->getName());
        //auto physicalType = physicalFieldFactory.getPhysicalType(field->getDataType());
        //auto offset = (localState->currentIndex * localState->recordSize) + fieldOffset;
        //auto buf = localState->buffer->getBufferAddress();
        //value->write(std::dynamic_pointer_cast<NesMemoryAddress>(buf + offset));
        //fieldOffset = fieldOffset + physicalType->size();
    //}

    //NES_DEBUG("Emit Record: " << record);
    //std::cout << record << std::endl;
}
void EmitOperator::open(ExecutionContextPtr ctx) const {
    Operator::open(ctx);
    auto state = std::make_shared<EmitOperatorState>();
    auto buffer = ctx->allocateTupleBuffer();
    auto schema = Schema::create(Schema::ROW_LAYOUT);
    state->buffer = std::make_shared<RecordBuffer>(buffer, schema);
    state->recordSize = outputSchema->getSchemaSizeInBytes();
    state->currentIndex = std::make_shared<NesInt32>(0);

    auto thisPtr = shared_from_this();
    auto ops = std::dynamic_pointer_cast<OperatorState>(state);
    ctx->setThreadLocalOperator(thisPtr, ops);
}
void EmitOperator::close(ExecutionContextPtr ctx) const { Operator::close(ctx); }

}// namespace NES::QueryCompilation
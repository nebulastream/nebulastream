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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ScanRaw.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Text/RawStringView.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

ScanRaw::ScanRaw(SchemaPtr schema,
                 std::unique_ptr<NES::DataParser::Parser>&& parser,
                 std::vector<Record::RecordFieldIdentifier> projections)
    : schema(std::move(schema)), projections(std::move(projections)), parser(std::move(parser)),
      parserRef(std::make_shared<MemRef>(reinterpret_cast<int8_t*>(this->parser.get()))) {
    auto rawOffsets = this->parser->getFieldOffsets();
    std::transform(rawOffsets.begin(), rawOffsets.end(), std::back_inserter(tupleData), [](void* ptr) {
        return Value<MemRef>(static_cast<int8_t*>(ptr));
    });
}

void* parseProxy(void* parserPtr, void* begin, void* end) {
    auto parser = static_cast<DataParser::Parser*>(parserPtr);
    // Nautilus does not have a concept of const Memref, but the memory segment is never modified.
    return const_cast<char*>(parser->parse({static_cast<char*>(begin), static_cast<char*>(end)}).data());
}

Nautilus::Value<> load(const PhysicalTypePtr& type, Value<MemRef> offset) {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return offset.load<Boolean>();
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return offset.load<Int8>();
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return offset.load<Int16>();
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return offset.load<Int32>();
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return offset.load<Int64>();
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return offset.load<UInt8>();
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return offset.load<UInt16>();
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return offset.load<UInt32>();
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return offset.load<UInt64>();
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return offset.load<Nautilus::Float>();
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return offset.load<Nautilus::Double>();
            };
            case BasicPhysicalType::NativeType::TEXT: {
                NES_ERROR("Physical Type: {} is currently not supported", type->toString());
                NES_NOT_IMPLEMENTED();
            };
            default: {
                NES_ERROR("Physical Type: {} is currently not supported", type->toString());
                NES_NOT_IMPLEMENTED();
            };
        }
    } else if (type->isArrayType()) {
        NES_ERROR("Physical Type: array type {} is currently not supported", type->toString());
        NES_NOT_IMPLEMENTED();
    } else {
        NES_ERROR("Physical Type: type {} is currently not supported", type->toString());
        NES_NOT_IMPLEMENTED();
    }
}

void ScanRaw::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // initialize global state variables to keep track of the watermark ts and the origin id
    ctx.setWatermarkTs(recordBuffer.getWatermarkTs());
    ctx.setOrigin(recordBuffer.getOriginId());
    ctx.setCurrentTs(recordBuffer.getCreatingTs());
    ctx.setSequenceNumber(recordBuffer.getSequenceNr());
    ctx.setChunkNumber(recordBuffer.getChunkNr());
    ctx.setLastChunk(recordBuffer.isLastChunk());
    ctx.setCurrentStatisticId(recordBuffer.getStatisticId());
    // call open on all child operators
    child->open(ctx, recordBuffer);

    // iterate over records in buffer
    auto begin = recordBuffer.getBuffer();
    auto end = Value<MemRef>(begin->add(recordBuffer.getNumRecords().getValue()));
    while (begin != end) {
        begin = FunctionCall("parseProxy", parseProxy, parserRef, begin, end);
        Record record;
        for (size_t index = 0; const auto& field : schema->fields) {
            DefaultPhysicalTypeFactory typeFactory;
            Value<> a = load(typeFactory.getPhysicalType(field->getDataType()), tupleData[index]);
            record.write(field->getName(), a);
            index++;
        }

        child->execute(ctx, record);
    }
}

}// namespace NES::Runtime::Execution::Operators

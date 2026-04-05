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

#include <ArrowFileSink.hpp>

#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>

namespace NES
{

namespace
{
constexpr uint64_t align8(const uint64_t value)
{
    return (value + 7) & ~uint64_t{7};
}

std::shared_ptr<arrow::DataType> nesTypeToArrowType(const DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::INT8:    return arrow::int8();
        case DataType::Type::INT16:   return arrow::int16();
        case DataType::Type::INT32:   return arrow::int32();
        case DataType::Type::INT64:   return arrow::int64();
        case DataType::Type::UINT8:   return arrow::uint8();
        case DataType::Type::UINT16:  return arrow::uint16();
        case DataType::Type::UINT32:  return arrow::uint32();
        case DataType::Type::UINT64:  return arrow::uint64();
        case DataType::Type::FLOAT32: return arrow::float32();
        case DataType::Type::FLOAT64: return arrow::float64();
        case DataType::Type::BOOLEAN: return arrow::boolean();
        case DataType::Type::CHAR:    return arrow::uint8();
        case DataType::Type::VARSIZED: return arrow::utf8();
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Cannot map NES UNDEFINED type to Arrow type.");
    }
    std::unreachable();
}

std::shared_ptr<arrow::Schema> nesSchemaToArrowSchema(const Schema& schema)
{
    arrow::FieldVector arrowFields;
    arrowFields.reserve(schema.getNumberOfFields());
    for (const auto& field : schema)
    {
        arrowFields.push_back(arrow::field(field.name, nesTypeToArrowType(field.dataType.type), field.dataType.nullable));
    }
    return arrow::schema(std::move(arrowFields));
}
}

ArrowFileSink::ArrowFileSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , filePath(sinkDescriptor.getFromConfig(ConfigParametersArrowFile::FILE_PATH))
    , nesSchema(sinkDescriptor.getSchema())
{
}

void ArrowFileSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up Arrow file sink: {}", *this);

    arrowSchema = nesSchemaToArrowSchema(*nesSchema);

    auto fileResult = arrow::io::FileOutputStream::Open(filePath);
    INVARIANT(fileResult.ok(), "Failed to open Arrow output file '{}': {}", filePath, fileResult.status().ToString());
    file = *fileResult;

    auto writerResult = arrow::ipc::MakeStreamWriter(file, arrowSchema);
    INVARIANT(writerResult.ok(), "Failed to create Arrow IPC stream writer: {}", writerResult.status().ToString());
    writer = *writerResult;

    NES_INFO("Arrow file sink opened '{}'", filePath);
}

void ArrowFileSink::stop(PipelineExecutionContext&)
{
    if (writer)
    {
        auto status = writer->Close();
        if (!status.ok())
        {
            NES_WARNING("ArrowFileSink: error closing writer: {}", status.ToString());
        }
        writer.reset();
    }
    if (file)
    {
        auto status = file->Close();
        if (!status.ok())
        {
            NES_WARNING("ArrowFileSink: error closing file: {}", status.ToString());
        }
        file.reset();
    }
    NES_INFO("Arrow file sink closed '{}'", filePath);
}

std::shared_ptr<arrow::RecordBatch> ArrowFileSink::wrapBufferAsRecordBatch(const TupleBuffer& buffer) const
{
    const int64_t numRows = static_cast<int64_t>(buffer.getNumberOfTuples());
    if (numRows == 0)
    {
        return arrow::RecordBatch::Make(arrowSchema, 0, std::vector<std::shared_ptr<arrow::Array>>{});
    }

    const uint64_t dataSizePerRecord = std::accumulate(
        nesSchema->begin(), nesSchema->end(), uint64_t{0},
        [](uint64_t acc, const Schema::Field& f)
        {
            if (f.dataType.type == DataType::Type::BOOLEAN) return acc;
            return acc + (f.dataType.type == DataType::Type::VARSIZED ? sizeof(int32_t) : f.dataType.getSizeInBytesWithoutNull());
        });
    const auto numFields = nesSchema->getNumberOfFields();
    const uint64_t numBoolColumns = std::count_if(nesSchema->begin(), nesSchema->end(),
        [](const Schema::Field& f) { return f.dataType.type == DataType::Type::BOOLEAN; });
    const auto bufferSize = buffer.getBufferSize();
    const double bitmapOverheadPerRecord = static_cast<double>(numFields + numBoolColumns) / 8.0;
    uint64_t cap = static_cast<uint64_t>(
        static_cast<double>(bufferSize) / (static_cast<double>(dataSizePerRecord) + bitmapOverheadPerRecord));

    /// Shrink capacity to account for 8-byte alignment padding on each column region.
    auto computeAlignedSize = [&](uint64_t c) -> uint64_t
    {
        uint64_t size = numFields * align8((c + 7) / 8);
        for (const auto& field : *nesSchema)
        {
            if (field.dataType.type == DataType::Type::BOOLEAN)
            {
                size += align8((c + 7) / 8);
            }
            else if (field.dataType.type == DataType::Type::VARSIZED)
            {
                size += align8(static_cast<uint64_t>(c + 1) * sizeof(int32_t));
            }
            else
            {
                size += align8(c * field.dataType.getSizeInBytesWithoutNull());
            }
        }
        return size;
    };
    while (computeAlignedSize(cap) > bufferSize && cap > 1)
    {
        --cap;
    }

    const uint64_t bitmapBytesPerColumn = align8((cap + 7) / 8);

    auto* rawBuffer = buffer.getAvailableMemoryArea<uint8_t>().data();
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(numFields);

    uint32_t numVarSizedCols = 0;
    for (const auto& field : *nesSchema)
    {
        if (field.dataType.type == DataType::Type::VARSIZED)
        {
            ++numVarSizedCols;
        }
    }

    uint64_t bitmapOffset = 0;
    uint64_t dataOffset = numFields * bitmapBytesPerColumn;
    uint32_t nextChildSlot = 0;
    for (const auto& field : *nesSchema)
    {
        const auto arrowType = nesTypeToArrowType(field.dataType.type);

        std::shared_ptr<arrow::Buffer> nullBitmapBuf;
        if (field.dataType.nullable)
        {
            nullBitmapBuf = arrow::Buffer::Wrap(rawBuffer + bitmapOffset, bitmapBytesPerColumn);
        }

        if (field.dataType.type == DataType::Type::VARSIZED)
        {
            const uint32_t childSlot = nextChildSlot++;
            const uint64_t offsetsBufSize = static_cast<uint64_t>(cap + 1) * sizeof(int32_t);
            auto offsetsBuf = arrow::Buffer::Wrap(rawBuffer + dataOffset, offsetsBufSize);

            const auto totalChildren = buffer.getNumberOfChildBuffers();
            uint64_t totalDataBytes = 0;
            uint32_t childCount = 0;
            for (uint32_t idx = childSlot; idx < totalChildren; idx += numVarSizedCols)
            {
                totalDataBytes += buffer.loadChildBuffer(VariableSizedAccess::Index{idx}).getNumberOfTuples();
                ++childCount;
            }

            std::shared_ptr<arrow::Buffer> dataBuf;
            if (childCount <= 1 && totalChildren > childSlot)
            {
                auto childBuf = buffer.loadChildBuffer(VariableSizedAccess::Index{childSlot});
                dataBuf = arrow::Buffer::Wrap(childBuf.getAvailableMemoryArea<uint8_t>().data(), totalDataBytes);
            }
            else if (childCount > 1)
            {
                auto allocResult = arrow::AllocateBuffer(static_cast<int64_t>(totalDataBytes));
                INVARIANT(allocResult.ok(), "Failed to allocate Arrow buffer for VARSIZED data: {}", allocResult.status().ToString());
                auto arrowBuf = std::move(*allocResult);
                auto* dest = arrowBuf->mutable_data();
                uint64_t pos = 0;
                for (uint32_t idx = childSlot; idx < totalChildren; idx += numVarSizedCols)
                {
                    auto childBuf = buffer.loadChildBuffer(VariableSizedAccess::Index{idx});
                    const auto usedBytes = childBuf.getNumberOfTuples();
                    std::memcpy(dest + pos, childBuf.getAvailableMemoryArea<uint8_t>().data(), usedBytes);
                    pos += usedBytes;
                }
                dataBuf = std::move(arrowBuf);
            }
            else
            {
                dataBuf = arrow::Buffer::Wrap(rawBuffer, 0);
            }

            auto arrayData = arrow::ArrayData::Make(arrowType, numRows, {nullBitmapBuf, offsetsBuf, dataBuf});
            arrays.push_back(arrow::MakeArray(arrayData));
            dataOffset += align8(offsetsBufSize);
        }
        else if (field.dataType.type == DataType::Type::BOOLEAN)
        {
            /// BOOLEAN: data is already bit-packed in the buffer — wrap directly as Arrow array.
            const uint64_t dataBufSize = align8((cap + 7) / 8);
            auto dataBuf = arrow::Buffer::Wrap(rawBuffer + dataOffset, dataBufSize);
            auto arrayData = arrow::ArrayData::Make(arrowType, numRows, {nullBitmapBuf, dataBuf});
            arrays.push_back(arrow::MakeArray(arrayData));
            dataOffset += dataBufSize;
        }
        else
        {
            const uint32_t fieldSize = field.dataType.getSizeInBytesWithoutNull();
            const uint64_t dataBufSize = cap * fieldSize;
            auto dataBuf = arrow::Buffer::Wrap(rawBuffer + dataOffset, dataBufSize);
            auto arrayData = arrow::ArrayData::Make(arrowType, numRows, {nullBitmapBuf, dataBuf});
            arrays.push_back(arrow::MakeArray(arrayData));
            dataOffset += align8(dataBufSize);
        }

        bitmapOffset += bitmapBytesPerColumn;
    }

    return arrow::RecordBatch::Make(arrowSchema, numRows, std::move(arrays));
}

void ArrowFileSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in ArrowFileSink.");

    auto batch = wrapBufferAsRecordBatch(inputTupleBuffer);
    std::lock_guard lock(writerMutex);
    auto status = writer->WriteRecordBatch(*batch);
    INVARIANT(status.ok(), "ArrowFileSink: failed to write RecordBatch: {}", status.ToString());
}

DescriptorConfig::Config ArrowFileSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersArrowFile>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterArrowFileSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return ArrowFileSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterArrowFileSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<ArrowFileSink>(
        std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}

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

#include <ArrowFileSource.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/ArrowBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>

namespace NES
{

ArrowFileSource::ArrowFileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersArrowFileSource::FILE_PATH))
    , nesSchema(sourceDescriptor.getLogicalSource().getSchema())
{
}

void ArrowFileSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);

    auto fileResult = arrow::io::ReadableFile::Open(filePath);
    INVARIANT(fileResult.ok(), "Failed to open Arrow input file '{}': {}", filePath, fileResult.status().ToString());
    file = *fileResult;

    auto readerResult = arrow::ipc::RecordBatchStreamReader::Open(file);
    INVARIANT(readerResult.ok(), "Failed to create Arrow IPC stream reader for '{}': {}", filePath, readerResult.status().ToString());
    reader = *readerResult;

    /// Obtain the ArrowBufferRef layout from LowerSchemaProvider — this is the same layout
    /// the scan operator uses, so source and scan are guaranteed to agree on offsets.
    auto bufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufferProvider->getBufferSize(), *nesSchema, "Arrow", {});
    arrowBufRef = std::dynamic_pointer_cast<const ArrowBufferRef>(bufRef);
    INVARIANT(arrowBufRef, "LowerSchemaProvider did not return an ArrowBufferRef for Arrow format");

    exhausted = false;
    currentBatch = nullptr;
    currentRowInBatch = 0;

    NES_INFO("ArrowFileSource opened '{}' (capacity={}, fields={})", filePath, arrowBufRef->getCapacity(), nesSchema->getNumberOfFields());
}

void ArrowFileSource::close()
{
    reader.reset();
    if (file)
    {
        auto status = file->Close();
        if (!status.ok())
        {
            NES_WARNING("ArrowFileSource: error closing file: {}", status.ToString());
        }
        file.reset();
    }
    currentBatch.reset();
    arrowBufRef.reset();
    NES_INFO("ArrowFileSource closed '{}'", filePath);
}

Source::FillTupleBufferResult ArrowFileSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    if (exhausted || stopToken.stop_requested())
    {
        return FillTupleBufferResult::eos();
    }

    const auto& fields = arrowBufRef->getFields();
    const auto numFields = fields.size();
    const auto capacity = arrowBufRef->getCapacity();

    auto* rawBuffer = tupleBuffer.getAvailableMemoryArea<uint8_t>().data();

    /// Track cumulative string bytes per VARSIZED column across chunks
    std::vector<int32_t> varSizedCumulativeBytes(arrowBufRef->getNumVarSizedColumns(), 0);

    uint64_t totalRowsWritten = 0;

    while (totalRowsWritten < capacity)
    {
        if (stopToken.stop_requested())
        {
            break;
        }

        /// Need a new batch?
        if (!currentBatch || currentRowInBatch >= currentBatch->num_rows())
        {
            auto status = reader->ReadNext(&currentBatch);
            INVARIANT(status.ok(), "Error reading Arrow batch: {}", status.ToString());
            if (!currentBatch)
            {
                exhausted = true;
                break;
            }
            currentRowInBatch = 0;
        }

        const int64_t remainingInBatch = currentBatch->num_rows() - currentRowInBatch;
        const uint64_t remainingInBuffer = capacity - totalRowsWritten;
        const uint64_t rowsToWrite = std::min(static_cast<uint64_t>(remainingInBatch), remainingInBuffer);

        /// Write each column
        uint32_t varColIdx = 0;
        for (size_t col = 0; col < numFields; ++col)
        {
            const auto& field = fields[col];
            const auto& arrowArray = currentBatch->column(static_cast<int>(col));
            auto* bitmapDst = rawBuffer + field.bitmapOffset;

            /// Copy validity bitmap
            if (field.type.nullable && arrowArray->null_bitmap_data())
            {
                for (uint64_t r = 0; r < rowsToWrite; ++r)
                {
                    const int64_t srcBitIdx = currentRowInBatch + static_cast<int64_t>(r);
                    const uint64_t dstBitIdx = totalRowsWritten + r;
                    if (arrow::bit_util::GetBit(arrowArray->null_bitmap_data(), srcBitIdx + arrowArray->offset()))
                    {
                        arrow::bit_util::SetBit(bitmapDst, dstBitIdx);
                    }
                    else
                    {
                        arrow::bit_util::ClearBit(bitmapDst, dstBitIdx);
                    }
                }
            }
            else if (field.type.nullable)
            {
                /// No null bitmap in Arrow means all valid — set bits to 1
                for (uint64_t r = 0; r < rowsToWrite; ++r)
                {
                    arrow::bit_util::SetBit(bitmapDst, totalRowsWritten + r);
                }
            }

            if (field.type.type == DataType::Type::VARSIZED)
            {
                const auto* stringArray = static_cast<const arrow::StringArray*>(arrowArray.get());
                auto* offsetsDst = reinterpret_cast<int32_t*>(rawBuffer + field.dataOffset);

                /// Arrow stores string data contiguously — get the byte range for this row slice
                const int32_t baseOffset = stringArray->value_offset(currentRowInBatch);
                const int32_t endOffset = stringArray->value_offset(currentRowInBatch + static_cast<int64_t>(rowsToWrite));
                const int32_t totalBytes = endOffset - baseOffset;

                /// Write globally-cumulative offsets directly from Arrow's offset array
                for (uint64_t r = 0; r <= rowsToWrite; ++r)
                {
                    offsetsDst[totalRowsWritten + r] = varSizedCumulativeBytes[varColIdx]
                        + stringArray->value_offset(currentRowInBatch + static_cast<int64_t>(r)) - baseOffset;
                }

                /// Single memcpy from Arrow values buffer into child buffer
                const auto neededSize = totalBytes > 0 ? static_cast<uint64_t>(totalBytes) : uint64_t{1};
                TupleBuffer childBuffer = [&]()
                {
                    if (bufferProvider->getBufferSize() >= neededSize)
                    {
                        if (auto buf = bufferProvider->getBufferNoBlocking(); buf.has_value())
                        {
                            return buf.value();
                        }
                    }
                    auto unpooled = bufferProvider->getUnpooledBuffer(neededSize);
                    INVARIANT(unpooled.has_value(), "Cannot allocate child buffer of size {}", neededSize);
                    return unpooled.value();
                }();

                if (totalBytes > 0)
                {
                    std::memcpy(childBuffer.getAvailableMemoryArea<uint8_t>().data(),
                                stringArray->raw_data() + baseOffset, totalBytes);
                }
                childBuffer.setNumberOfTuples(totalBytes);
                [[maybe_unused]] auto childIdx = tupleBuffer.storeChildBuffer(childBuffer);

                varSizedCumulativeBytes[varColIdx] += totalBytes;
                ++varColIdx;
            }
            else if (field.type.type == DataType::Type::BOOLEAN)
            {
                /// BOOLEAN: Both Arrow and ArrowBufferRef use packed bits (1 bit per value).
                /// Copy bits directly from the Arrow array to the buffer's data region.
                auto* bitmapDst = rawBuffer + field.dataOffset;
                const auto* boolArray = static_cast<const arrow::BooleanArray*>(arrowArray.get());
                for (uint64_t r = 0; r < rowsToWrite; ++r)
                {
                    const auto srcRow = currentRowInBatch + static_cast<int64_t>(r);
                    const auto dstBit = totalRowsWritten + r;
                    if (boolArray->Value(srcRow))
                    {
                        arrow::bit_util::SetBit(bitmapDst, dstBit);
                    }
                    else
                    {
                        arrow::bit_util::ClearBit(bitmapDst, dstBit);
                    }
                }
            }
            else
            {
                /// Fixed-width: memcpy from Arrow array raw data
                const auto* srcData = arrowArray->data()->buffers[1]->data();
                auto* dataDst = rawBuffer + field.dataOffset;
                const uint32_t fieldSize = field.dataTypeSize;
                std::memcpy(
                    dataDst + totalRowsWritten * fieldSize,
                    srcData + (currentRowInBatch + arrowArray->offset()) * fieldSize,
                    rowsToWrite * fieldSize);
            }
        }

        totalRowsWritten += rowsToWrite;
        currentRowInBatch += static_cast<int64_t>(rowsToWrite);
    }

    if (totalRowsWritten == 0)
    {
        return FillTupleBufferResult::eos();
    }

    /// For ARROW parser, withBytes stores the record count (consumed by SourceThread::setNumberOfTuples)
    return FillTupleBufferResult::withBytes(totalRowsWritten);
}

std::ostream& ArrowFileSource::toString(std::ostream& str) const
{
    str << "ArrowFileSource(" << filePath << ")";
    return str;
}

DescriptorConfig::Config ArrowFileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersArrowFileSource>(std::move(config), NAME);
}

SourceValidationRegistryReturnType
///NOLINTNEXTLINE(performance-unnecessary-value-param)
RegisterArrowFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return ArrowFileSource::validateAndFormat(sourceConfig.config);
}

///NOLINTNEXTLINE(performance-unnecessary-value-param)
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterArrowFileSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<ArrowFileSource>(sourceRegistryArguments.sourceDescriptor);
}
}

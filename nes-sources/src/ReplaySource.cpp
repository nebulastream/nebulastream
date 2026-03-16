/*
    Licensed under the Apache License, Version 2.0 (the "License");
*/

#include <Sources/ReplaySource.hpp>

#include <cstdint>
#include <cstring>
#include <ios>
#include <iosfwd>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <ReplayStoreReader.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include "Runtime/AbstractBufferProvider.hpp"

namespace NES
{

ReplaySource::ReplaySource(const SourceDescriptor& sourceDescriptor)
    : filePath(
          sourceDescriptor.getConfig().contains("file_path") ? std::get<std::string>(sourceDescriptor.getConfig().at("file_path"))
                                                             : std::string())
    , schema(*sourceDescriptor.getLogicalSource().getSchema())
{
}

ReplaySource::~ReplaySource() = default;

void ReplaySource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_DEBUG("ReplaySource: opening {}", filePath);
    reader = std::make_unique<StoreManager::ReplayStoreReader>(filePath);
    reader->open();
    reader->verifySchema(schema);
    NES_DEBUG("ReplaySource: dataStartOffset={}", reader->getDataStartOffset());
}

void ReplaySource::close()
{
    if (reader)
    {
        reader->close();
        reader.reset();
    }
}

Source::FillTupleBufferResult ReplaySource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    const auto startPos = reader->getPosition();
    NES_DEBUG("ReplaySource: fill called at pos {}", static_cast<long long>(startPos));
    if (reader->isEof())
    {
        return FillTupleBufferResult::eos();
    }

    // Build RowLayout using current buffer size and the logical source schema
    auto rowLayout = LowerSchemaProvider::lowerSchema(tupleBuffer.getBufferSize(), schema, MemoryLayoutType::ROW_LAYOUT);
    const auto capacity = rowLayout->getCapacity();

    // Delegate row reading to the ReplayStoreReader
    char* dest = tupleBuffer.getAvailableMemoryArea<char>().data();
    const uint32_t tupleSize = rowLayout->getTupleSize();
    const uint64_t tuplesWritten = reader->readRows(dest, capacity, tupleSize, schema);

    // Calculate row width for bytes tracking
    uint32_t rowWidthBytes = 0;
    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        auto type = schema.getFieldAt(i).dataType;
        if (type.isType(DataType::Type::VARSIZED))
        {
            rowWidthBytes += sizeof(uint32_t);
        }
        else
        {
            rowWidthBytes += type.getSizeInBytes();
        }
    }

    const size_t bytesRead = static_cast<size_t>(tuplesWritten) * static_cast<size_t>(rowWidthBytes);
    tupleBuffer.setNumberOfTuples(tuplesWritten);

    bool reachedEnd = false;
    if (reader->isEof())
    {
        reader->clearErrors();
        if (startPos != std::streampos(-1))
        {
            reader->seekTo(static_cast<std::streamoff>(startPos) + static_cast<std::streamoff>(bytesRead));
        }
        reachedEnd = true;
        tupleBuffer.setLastChunk(true);
    }
    else
    {
        if (reader->peek() == std::char_traits<char>::eof())
        {
            reachedEnd = true;
            tupleBuffer.setLastChunk(true);
        }
    }

    NES_DEBUG("ReplaySource: read {} bytes ({} tuples), eof={}", bytesRead, tuplesWritten, reachedEnd);

    if (tuplesWritten == 0 && reachedEnd)
    {
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(tuplesWritten);
}

uint32_t ReplaySource::getRowWidthBytes() const
{
    uint32_t rowWidth = 0;
    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        auto type = schema.getFieldAt(i).dataType;
        if (type.isType(DataType::Type::VARSIZED))
        {
            rowWidth += sizeof(uint32_t);
        }
        else
        {
            rowWidth += type.getSizeInBytes();
        }
    }
    return rowWidth;
}

Schema ReplaySource::readSchemaFromFile(const std::string& filePath)
{
    return StoreManager::ReplayStoreReader::readSchemaFromFile(filePath);
}

DescriptorConfig::Config ReplaySource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    if (config.find("file_path") == config.end())
    {
        throw InvalidConfigParameter("ReplaySource requires 'file_path'");
    }
    DescriptorConfig::Config validated;
    validated.emplace("file_path", DescriptorConfig::ConfigType(config.at("file_path")));
    validated.emplace("number_of_buffers_in_local_pool", DescriptorConfig::ConfigType(static_cast<int64_t>(-1)));
    validated.emplace("max_inflight_buffers", DescriptorConfig::ConfigType(SourceDescriptor::INVALID_MAX_INFLIGHT_BUFFERS));
    return validated;
}

std::ostream& ReplaySource::toString(std::ostream& str) const
{
    str << fmt::format("ReplaySource(filePath: {}, bytesRead: {})", filePath, reader ? reader->getTotalBytesRead() : 0);
    return str;
}

SourceValidationRegistryReturnType RegisterReplaySourceValidation(SourceValidationRegistryArguments args)
{
    return ReplaySource::validateAndFormat(std::move(args.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterReplaySource(SourceRegistryArguments args)
{
    return std::make_unique<ReplaySource>(args.sourceDescriptor);
}
}

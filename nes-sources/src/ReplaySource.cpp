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

#include <Sources/ReplaySource.hpp>

#include <cstdint>
#include <cstring>
#include <iosfwd>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <ReplayStoreReader.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <StoreRegistry.hpp>

namespace NES
{

ReplaySource::ReplaySource(const SourceDescriptor& sourceDescriptor)
    : filePath(
          sourceDescriptor.getConfig().contains("file_path") ? std::get<std::string>(sourceDescriptor.getConfig().at("file_path"))
                                                             : std::string())
    , storeName(
          sourceDescriptor.getConfig().contains("store_name") ? std::get<std::string>(sourceDescriptor.getConfig().at("store_name"))
                                                              : std::string())
    , schema(*sourceDescriptor.getLogicalSource().getSchema())
{
}

ReplaySource::~ReplaySource() = default;

void ReplaySource::open(std::shared_ptr<AbstractBufferProvider>)
{
    if (!storeName.empty())
    {
        auto registeredStore = StoreManager::StoreRegistry::instance().getStore(storeName);
        if (registeredStore.has_value())
        {
            store = registeredStore.value();
            NES_DEBUG("ReplaySource: using registered store '{}'", storeName);
            return;
        }
    }

    NES_DEBUG("ReplaySource: opening file {}", filePath);
    reader = std::make_unique<StoreManager::ReplayStoreReader>(filePath);
    reader->open();
    reader->verifySchema(schema);
    NES_DEBUG("ReplaySource: dataStartOffset={}", reader->getDataStartOffset());
}

void ReplaySource::close()
{
    if (store.has_value())
    {
        store.reset();
    }
    if (reader)
    {
        reader->close();
        reader.reset();
    }
}

Source::FillTupleBufferResult ReplaySource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    PRECONDITION(store.has_value(), "Replay source requires underlying store type!");
    if (!store->hasMore())
    {
        return FillTupleBufferResult::eos();
    }
    const uint64_t tuplesWritten = store->read(tupleBuffer, schema);
    if (tuplesWritten == 0)
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
            rowWidth += type.getSizeInBytesWithNull();
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
    if (config.find("file_path") == config.end() && config.find("store_name") == config.end())
    {
        throw InvalidConfigParameter("ReplaySource requires 'file_path' or 'store_name'");
    }
    DescriptorConfig::Config validated;
    if (static_cast<unsigned int>(config.contains("file_path")) != 0U)
    {
        validated.emplace("file_path", DescriptorConfig::ConfigType(config.at("file_path")));
    }
    if (static_cast<unsigned int>(config.contains("store_name")) != 0U)
    {
        validated.emplace("store_name", DescriptorConfig::ConfigType(config.at("store_name")));
    }
    validated.emplace("number_of_buffers_in_local_pool", DescriptorConfig::ConfigType(static_cast<int64_t>(-1)));
    validated.emplace("max_inflight_buffers", DescriptorConfig::ConfigType(SourceDescriptor::INVALID_MAX_INFLIGHT_BUFFERS));
    return validated;
}

std::ostream& ReplaySource::toString(std::ostream& str) const
{
    if (store.has_value())
    {
        str << fmt::format("ReplaySource(store: {})", storeName);
    }
    else
    {
        str << fmt::format("ReplaySource(filePath: {}, bytesRead: {})", filePath, reader ? reader->getTotalBytesRead() : 0);
    }
    return str;
}

SourceValidationRegistryReturnType RegisterReplaySourceValidation(SourceValidationRegistryArguments args)
{
    return ReplaySource::validateAndFormat(std::move(args.config));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterReplaySource(SourceRegistryArguments args)
{
    return std::make_unique<ReplaySource>(args.sourceDescriptor);
}
}

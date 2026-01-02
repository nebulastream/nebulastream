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

#include <MatrixSink.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>

#include <curl/curl.h>
#include <curl/easy.h>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

std::string
getFieldValueAsString(std::span<const std::byte>& tuple, const TupleBuffer& tupleBuffer, const MatrixSink::fieldProperties properties)
{
    /// Get datatype, size and offset of field
    const DataType dataType = properties.dataType;
    const size_t offset = properties.offset;


    if (dataType.type == DataType::Type::VARSIZED)
    {
        const VariableSizedAccess variableSizedAccess{*std::bit_cast<const uint64_t*>(&tuple[offset])};
        return MemoryLayout::readVarSizedDataAsString(tupleBuffer, variableSizedAccess);
    }
    return dataType.formattedBytesToString(&tuple[offset]);
}

MatrixSink::fieldProperties getOffsetOfFieldName(const std::string& fieldName, std::shared_ptr<const Schema> schema)
{
    size_t currentOffset = 0;
    for (size_t i = 0; i < schema->getNumberOfFields(); i++)
    {
        if (fieldName == schema->getFieldAt(i).name)
        {
            return {currentOffset, schema->getFieldAt(i).dataType};
        }
        currentOffset += schema->getFieldAt(i).dataType.getSizeInBytes();
    }
    /// If the field name does not match with a single field in the schema, return npos
    return {std::string::npos, {}};
}

MatrixSink::MatrixSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , curl(nullptr)
    , messageFieldDelimiter(sinkDescriptor.getFromConfig(ConfigParametersMatrix::MESSAGE_FIELD_DELIMITER))
    , schema(sinkDescriptor.getSchema())
{
    url = fmt::format(
        "http://{}:{}/{}",
        sinkDescriptor.getFromConfig(ConfigParametersMatrix::IPADDRESS),
        sinkDescriptor.getFromConfig(ConfigParametersMatrix::PORT),
        sinkDescriptor.getFromConfig(ConfigParametersMatrix::ENDPOINT));

    /// Scan the message string, remove field names and save their indexes
    std::string messageString = sinkDescriptor.getFromConfig(ConfigParametersMatrix::MESSAGE);
    std::string_view tempStringView(messageString);
    size_t fieldStartIndex = tempStringView.find(messageFieldDelimiter);

    while (fieldStartIndex != std::string::npos)
    {
        const size_t fieldEndIndex = tempStringView.find(messageFieldDelimiter, fieldStartIndex + 1);
        if (fieldEndIndex == std::string::npos)
        {
            throw std::invalid_argument(
                fmt::format("Matrix sink: Given message template does not delimit all mentioned fields properly: {}", messageString));
        }
        const size_t nameLength = fieldEndIndex - fieldStartIndex - 1;
        const std::string fieldName(tempStringView.substr(fieldStartIndex + 1, nameLength));
        const fieldProperties offsetInSchema = getOffsetOfFieldName(fieldName, schema);

        /// If we cannot find the field name in the schema, we just ignore it
        if (offsetInSchema.offset != std::string::npos)
        {
            /// Insert the desired value offset for this position in the message string
            messageToBufferOffsets[fieldStartIndex] = offsetInSchema;
            /// Remove the delimited fieldname from the string
            std::string tmpString;
            tmpString += tempStringView.substr(0, fieldStartIndex);
            tmpString += tempStringView.substr(fieldEndIndex + 1);
            messageString = std::move(tmpString);
            tempStringView = std::string_view(messageString);
            fieldStartIndex = tempStringView.find(messageFieldDelimiter);
        }
        else
        {
            fieldStartIndex = tempStringView.find(messageFieldDelimiter, fieldEndIndex + 1);
        }
    }
    message = messageString;
}

std::ostream& MatrixSink::toString(std::ostream& str) const
{
    str << fmt::format("MatrixSink(url: {}, message: {})", url, message);
    return str;
}

void MatrixSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up matrix sink: {}", *this);
    curl = curl_easy_init();
    PRECONDITION(curl, "curl_easy_init failed");
    isOpen = true;
}

void MatrixSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in MatrixSink.");
    PRECONDITION(isOpen, "Sink was not opened");
    const size_t tupleSize = schema->getSizeOfSchemaInBytes();

    /// Shamelessly copied from CSVFormat.csv
    const auto buffer = inputTupleBuffer.getAvailableMemoryArea<>().subspan(0, tupleSize * inputTupleBuffer.getNumberOfTuples());
    for (uint64_t i = 0; i < inputTupleBuffer.getNumberOfTuples(); i++)
    {
        std::string payload = message;
        size_t offsetCorrection = 0;
        auto tuple = buffer.subspan(tupleSize * i, tupleSize);
        /// Insert corresponding tuple fields at the defined offsets
        for (const auto& [messageOffset, fieldProperties] : messageToBufferOffsets)
        {
            std::string fieldValue = getFieldValueAsString(tuple, inputTupleBuffer, fieldProperties);
            const size_t totalOffset = messageOffset + offsetCorrection;
            if (totalOffset < payload.length())
            {
                payload.insert(totalOffset, fieldValue);
            }
            else
            {
                payload += fieldValue;
            }
            offsetCorrection += fieldValue.length() - 1;
        }

        /// Only if there actually are tuples in the buffer, we should send them.
        /// Prepare headers
        const std::string messageJson = "{\"message\": \"" + payload + "\"}";
        curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");

        /// Setup the url, headers and message
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, messageJson.c_str());

        curl_easy_perform(curl);
        curl_slist_free_all(headers);
    }
}

void MatrixSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Closing matrix sink.");
    curl_easy_cleanup(curl);
}

DescriptorConfig::Config MatrixSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMatrix>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterMatrixSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return MatrixSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterMatrixSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<MatrixSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
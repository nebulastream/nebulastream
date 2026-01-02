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

#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <curl/curl.h>

#include <folly/Synchronized.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <DataTypes/Schema.hpp>

namespace NES
{

/// Sink that creates a customizable message out of query results and transmits it to a matrix bot.
/// Needs the message and the ip address and port of the matrix bot.
class MatrixSink final : public Sink
{
public:
    struct fieldProperties
    {
        size_t offset;
        DataType dataType;
    };

    static constexpr std::string_view NAME = "Matrix";
    explicit MatrixSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~MatrixSink() override = default;

    MatrixSink(const MatrixSink&) = delete;
    MatrixSink& operator=(const MatrixSink&) = delete;
    MatrixSink(MatrixSink&&) = delete;
    MatrixSink& operator=(MatrixSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;


private:
    std::string url;
    std::string message;
    CURL* curl;
    char messageFieldDelimiter;
    bool isOpen = false;
    /// Maps offsets in the message string to the field offsets of the field to insert into the message
    std::unordered_map<size_t, fieldProperties> messageToBufferOffsets;
    std::shared_ptr<const Schema> schema;
};

struct ConfigParametersMatrix
{
    static inline const DescriptorConfig::ConfigParameter<std::string> IPADDRESS{
        "ip_address",
        "localhost",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(IPADDRESS, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> PORT{
        "port",
        "3000",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PORT, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> ENDPOINT{
        "endpoint",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ENDPOINT, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> MESSAGE{
        "message",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MESSAGE, config); }};

    static inline const DescriptorConfig::ConfigParameter<char> MESSAGE_FIELD_DELIMITER{
        "message_field_delimiter",
        '|',
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MESSAGE_FIELD_DELIMITER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, IPADDRESS, PORT, ENDPOINT, MESSAGE, MESSAGE_FIELD_DELIMITER);
};

}

FMT_OSTREAM(NES::MatrixSink);
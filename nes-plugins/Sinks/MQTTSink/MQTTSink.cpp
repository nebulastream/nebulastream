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

#include <MQTTSink.hpp>

#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <fmt/format.h>
#include <mqtt/async_client.h>
#include <mqtt/exception.h>
#include <mqtt/message.h>
#include "Sequencing/Sequencer.hpp"

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

struct OOPolicy
{
    virtual ~OOPolicy() = default;
    virtual bool isNext(TupleBuffer buffer) = 0;
    virtual std::optional<TupleBuffer> advanceAndDoNext(const TupleBuffer& buffer) = 0;
};

struct Allow final : OOPolicy
{
    bool isNext(TupleBuffer) override { return true; }

    std::optional<TupleBuffer> advanceAndDoNext(const TupleBuffer&) override { return {}; }
};

struct Enforce final : OOPolicy
{
    Sequencer<TupleBuffer> sequencer;

    bool isNext(TupleBuffer buffer) override
    {
        return sequencer.isNext(SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()), std::move(buffer))
            .has_value();
    }

    std::optional<TupleBuffer> advanceAndDoNext(const TupleBuffer& buffer) override
    {
        return sequencer.advanceAndGetNext(SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()));
    }
};

struct Drop final : OOPolicy
{
    folly::Synchronized<SequenceData> lastMutex{SequenceData(INVALID_SEQ_NUMBER, INVALID_CHUNK_NUMBER, true)};

    bool isNext(TupleBuffer buffer) override
    {
        auto last = lastMutex.wlock();
        if (*last < SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()))
        {
            *last = SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk());
            return true;
        }
        return false;
    }

    std::optional<TupleBuffer> advanceAndDoNext(const TupleBuffer&) override { return {}; }
};

MQTTSink::MQTTSink(const SinkDescriptor& sinkDescriptor)
    : Sink()
    , serverUri(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::SERVER_URI))
    , clientId(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::CLIENT_ID))
    , topic(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::TOPIC))
    , qos(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::QOS))
{
    switch (const auto inputFormat = sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::INPUT_FORMAT))
    {
        case InputFormat::CSV:
            formatter = std::make_unique<CSVFormat>(*sinkDescriptor.getSchema());
            break;
        case InputFormat::JSON:
            formatter = std::make_unique<JSONFormat>(*sinkDescriptor.getSchema());
            break;
        default:
            throw UnknownSinkFormat(fmt::format("Sink format: {} not supported.", magic_enum::enum_name(inputFormat)));
    }
    switch (auto oopolicy = sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::OUT_OF_ORDER_POLICY))
    {
        case OutOfOrderPolicy::ALLOW:
            this->policy = std::make_unique<Allow>();
            break;
        case OutOfOrderPolicy::ENFORCE:
            this->policy = std::make_unique<Enforce>();
            break;
        case OutOfOrderPolicy::DROP:
            this->policy = std::make_unique<Drop>();
            break;
        default:
            throw UnknownSinkFormat(fmt::format("Out of Order policy: {} not supported.", magic_enum::enum_name(oopolicy)));
    }
}

MQTTSink::~MQTTSink() = default;

std::ostream& MQTTSink::toString(std::ostream& str) const
{
    str << fmt::format("MQTTSink(serverURI: {}, clientId: {}, topic: {}, qos: {})", serverUri, clientId, topic, qos);
    return str;
}

void MQTTSink::start(PipelineExecutionContext&)
{
    client = std::make_unique<mqtt::async_client>(serverUri, clientId);

    try
    {
        const auto connectOptions = mqtt::connect_options_builder().automatic_reconnect(true).clean_session(true).finalize();

        client->connect(connectOptions)->wait();
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSink(e.what());
    }
}

void MQTTSink::stop(PipelineExecutionContext&)
{
    try
    {
        client->disconnect()->wait();
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSink("When closing mqtt sink: {}", e.what());
    }
}

void MQTTSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    if (policy->isNext(inputBuffer))
    {
        auto nextBuffer = std::make_optional(inputBuffer);
        while (nextBuffer)
        {
            if (inputBuffer.getNumberOfTuples() != 0)
            {
                const std::string fBuf = formatter->getFormattedBuffer(*nextBuffer);
                const mqtt::message_ptr message = mqtt::make_message(topic, fBuf);
                message->set_qos(qos);
                try
                {
                    client->publish(message)->wait();
                }
                catch (...)
                {
                    throw wrapExternalException();
                }
            }

            nextBuffer = policy->advanceAndDoNext(*nextBuffer);
        }
    }
}

DescriptorConfig::Config MQTTSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMQTTSink>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterMQTTSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return MQTTSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterMQTTSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<MQTTSink>(sinkRegistryArguments.sinkDescriptor);
}

}

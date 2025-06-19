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
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/Sequencer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <fmt/format.h>
#include <mqtt/async_client.h>
#include <mqtt/exception.h>
#include <mqtt/message.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

struct OOPolicy
{
    virtual ~OOPolicy() = default;
    virtual bool isNext(SequenceData sequence, std::string& message) = 0;
    virtual std::optional<std::string> advanceAndDoNext(SequenceData) = 0;
};

struct Allow final : OOPolicy
{
    bool isNext(SequenceData, std::string&) override { return true; }
    std::optional<std::string> advanceAndDoNext(SequenceData) override { return {}; }
};

struct Enforce final : OOPolicy
{
    Sequencer<std::string> sequencer;

    bool isNext(SequenceData sequence, std::string& message) override
    {
        if (auto isNextMessage = sequencer.isNext(sequence, std::move(message)); isNextMessage.has_value())
        {
            message = std::move(isNextMessage.value());
            return true;
        }
        return false;
    }

    std::optional<std::string> advanceAndDoNext(SequenceData sequence) override { return sequencer.advanceAndGetNext(sequence); }
};

struct Drop final : OOPolicy
{
    folly::Synchronized<SequenceData> lastMutex{SequenceData(INVALID_SEQ_NUMBER, INVALID_CHUNK_NUMBER, true)};

    bool isNext(SequenceData sequence, std::string&) override
    {
        auto last = lastMutex.wlock();
        if (*last < sequence)
        {
            *last = sequence;
            return true;
        }
        return false;
    }

    std::optional<std::string> advanceAndDoNext(SequenceData) override { return {}; }
};

Writer::Writer(std::unique_ptr<OOPolicy> policy, std::string server_uri, std::string client_id, std::string topic, int32_t qos)
    : policy(std::move(policy)), serverUri(std::move(server_uri)), clientId(std::move(client_id)), topic(std::move(topic)), qos(qos)
{
}
Writer::~Writer() = default;

MQTTSink::MQTTSink(const SinkDescriptor& sinkDescriptor)
    : Sink()
    , writer(
          [&]() -> std::unique_ptr<OOPolicy>
          {
              switch (sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::OUT_OF_ORDER_POLICY))
              {
                  case OutOfOrderPolicy::ALLOW:
                      return std::make_unique<Allow>();
                  case OutOfOrderPolicy::ENFORCE:
                      return std::make_unique<Enforce>();
                  case OutOfOrderPolicy::DROP:
                      return std::make_unique<Drop>();
              }
              std::unreachable();
          }(),
          sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::SERVER_URI),
          sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::CLIENT_ID),
          sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::TOPIC),
          sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::QOS))
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
    terminationFuture = writer.terminationPromise.get_future().share();
}

MQTTSink::~MQTTSink() = default;

std::ostream& MQTTSink::toString(std::ostream& str) const
{
    str << fmt::format(
        "MQTTSink(serverURI: {}, clientId: {}, topic: {}, qos: {})", writer.serverUri, writer.clientId, writer.topic, writer.qos);
    return str;
}

void MQTTSink::start(PipelineExecutionContext&)
{
    dispatcher = std::jthread(
        [](std::stop_token token, std::reference_wrapper<Writer> writerRef) mutable
        {
            auto& writer = writerRef.get();
            try
            {
                auto client = std::make_unique<mqtt::async_client>(writer.serverUri, writer.clientId);
                client->connect()->wait();
                std::pair<SequenceData, std::string> data;
                while (!token.stop_requested())
                {
                    if (!writer.dataQueue.tryReadUntil(std::chrono::system_clock::now() + std::chrono::milliseconds(10), data))
                    {
                        continue;
                    }
                    auto& [sequence, messageBody] = data;

                    if (writer.policy->isNext(sequence, messageBody))
                    {
                        auto nextBuffer = std::make_optional(std::move(messageBody));
                        while (nextBuffer)
                        {
                            if (!nextBuffer.value().empty())
                            {
                                const mqtt::message_ptr message = mqtt::make_message(writer.topic, std::move(nextBuffer.value()));
                                message->set_qos(writer.qos);
                                client->publish(message);
                            }

                            nextBuffer = writer.policy->advanceAndDoNext(sequence);
                        }
                    }
                }
            }
            catch (...)
            {
                writer.terminationPromise.set_exception(std::current_exception());
                return;
            }
            writer.terminationPromise.set_value();
        },
        std::reference_wrapper(writer));
}

void MQTTSink::stop(PipelineExecutionContext&)
{
    dispatcher.request_stop();
}

void MQTTSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    auto sequence = SequenceData(inputBuffer.getSequenceNumber(), inputBuffer.getChunkNumber(), inputBuffer.isLastChunk());
    if (inputBuffer.getNumberOfTuples() == 0)
    {
        writer.dataQueue.write(sequence, std::string());
    }
    else
    {
        writer.dataQueue.write(sequence, formatter->getFormattedBuffer(inputBuffer));
    }

    if (terminationFuture.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
    {
        try
        {
            terminationFuture.get();
        }
        catch (...)
        {
           throw wrapExternalException();
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

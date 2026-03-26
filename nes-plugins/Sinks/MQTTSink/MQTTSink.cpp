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
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <mqtt/exception.h>
#include <mqtt/message.h>

#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
    {

        MQTTSink::MQTTSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
            : Sink(std::move(backpressureController))
            , serverUri(sinkDescriptor.getFromConfig(ConfigParametersMQTT::SERVER_URI))
            , clientId(sinkDescriptor.getFromConfig(ConfigParametersMQTT::CLIENT_ID))
            , topic(sinkDescriptor.getFromConfig(ConfigParametersMQTT::TOPIC))
            , qos(sinkDescriptor.getFromConfig(ConfigParametersMQTT::QOS))
            , connected(false)
            , client(nullptr)
        {
        }

        MQTTSink::~MQTTSink() = default;

        std::ostream& MQTTSink::toString(std::ostream& os) const
        {
            return os << fmt::format("MQTTSink(serverURI: {}, clientId: {}, topic: {}, qos: {})", serverUri, clientId, topic, qos);
        }

        void MQTTSink::start(PipelineExecutionContext&)
        {
            NES_DEBUG("Setting up MQTT sink: {}", *this);

            try
            {
                client = std::make_unique<mqtt::async_client>(serverUri, clientId);
                client->connect()->wait();
                connected = true;
            }
            catch (...)
            {
                throw wrapExternalException();
            }
        }

        void MQTTSink::stop(PipelineExecutionContext&)
        {
            NES_INFO("Stopping MQTT sink: {}", *this);

            if (!client || !connected)
            {
                return;
            }

            try
            {
                client->disconnect()->wait();
                connected = false;
            }
            catch (...)
            {
                throw wrapExternalException();
            }
        }

        void MQTTSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
        {
            PRECONDITION(inputBuffer, "Invalid input buffer in MQTTSink.");
            PRECONDITION(client != nullptr, "MQTTSink client is not initialized. Did you forget to call start()?");
            PRECONDITION(connected, "MQTTSink client is not connected.");

            try
            {
                BufferIterator iterator{inputBuffer};

                std::optional<BufferIterator::BufferElement> element = iterator.getNextElement();
                while (element.has_value())
                {
                    const auto payloadView = element.value().buffer.getAvailableMemoryArea<char>();
                    const std::string payload{payloadView.data(), element.value().contentLength};
                    NES_INFO("Buffer element contentLength: {}", element.value().contentLength);

                    if (!payload.empty())
                    {
                        mqtt::message_ptr message = mqtt::make_message(topic, payload);
                        message->set_qos(qos);
                        NES_INFO("MQTT payload length: {}", payload.size());
                        NES_INFO("MQTT payload raw: [{}]", payload);
                        client->publish(message)->wait();
                    }

                    element = iterator.getNextElement();
                }
            }
            catch (...)
            {
                throw wrapExternalException();
            }
        }

        DescriptorConfig::Config MQTTSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
        {
            return DescriptorConfig::validateAndFormat<ConfigParametersMQTT>(std::move(config), NAME);
        }

        SinkValidationRegistryReturnType RegisterMQTTSinkValidation(SinkValidationRegistryArguments sinkConfig)
        {
            return MQTTSink::validateAndFormat(std::move(sinkConfig.config));
        }

        SinkRegistryReturnType RegisterMQTTSink(SinkRegistryArguments sinkRegistryArguments)
        {
            return std::make_unique<MQTTSink>(
                std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
        }

    }
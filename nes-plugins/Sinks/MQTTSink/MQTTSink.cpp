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

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES::Sinks
{

MQTTSink::MQTTSink(const SinkDescriptor& sinkDescriptor)
    : Sink()
    , serverUri(sinkDescriptor.getFromConfig(ConfigParametersMQTT::SERVER_URI))
    , clientId(sinkDescriptor.getFromConfig(ConfigParametersMQTT::CLIENT_ID))
    , topic(sinkDescriptor.getFromConfig(ConfigParametersMQTT::TOPIC))
    , qos(sinkDescriptor.getFromConfig(ConfigParametersMQTT::QOS))
{
    switch (const auto inputFormat = sinkDescriptor.getFromConfig(ConfigParametersMQTT::INPUT_FORMAT))
    {
        case Configurations::InputFormat::CSV:
            formatter = std::make_unique<CSVFormat>(sinkDescriptor.schema);
            break;
        case Configurations::InputFormat::JSON:
            formatter = std::make_unique<JSONFormat>(sinkDescriptor.schema);
            break;
        default:
            throw UnknownSinkFormat(fmt::format("Sink format: {} not supported.", magic_enum::enum_name(inputFormat)));
    }
}

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

void MQTTSink::execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    if (inputBuffer.getNumberOfTuples() == 0)
    {
        return;
    }

    const std::string fBuf = formatter->getFormattedBuffer(inputBuffer);
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

Configurations::DescriptorConfig::Config MQTTSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersMQTT>(std::move(config), NAME);
}

SinkValidationRegistryReturnType SinkValidationGeneratedRegistrar::RegisterMQTTSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return MQTTSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterMQTTSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<MQTTSink>(sinkRegistryArguments.sinkDescriptor);
}

}

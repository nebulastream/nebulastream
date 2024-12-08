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

#include "MQTTSource.hpp"

#include <memory>
#include <ostream>

#include <Configurations/Descriptor.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::Sources
{


MQTTSource::MQTTSource(const SourceDescriptor& sourceDescriptor)
    : serverURI(sourceDescriptor.getFromConfig(ConfigParametersMQTT::SERVER_URI))
    , clientId(sourceDescriptor.getFromConfig(ConfigParametersMQTT::CLIENT_ID))
    , userName(sourceDescriptor.getFromConfig(ConfigParametersMQTT::USER_NAME))
    , topic(sourceDescriptor.getFromConfig(ConfigParametersMQTT::TOPIC))
{
}

std::ostream& MQTTSource::toString(std::ostream& str) const
{
    str << "\nMQTTSource(";
    str << "\n  serverURI: " << serverURI;
    str << "\n  clientId: " << clientId;
    str << "\n  topic: " << topic;
    str << ")\n";
    return str;
}

void MQTTSource::open()
{
    mqttClient = std::make_unique<mqtt::async_client>(serverURI, clientId);

    try
    {
        auto connOpts = mqtt::connect_options_builder().user_name(userName).automatic_reconnect(true).clean_session(true).finalize();
        auto token = mqttClient->connect(connOpts);

        auto response = token->get_connect_response();

        if (response.is_session_present())
        {
            mqttClient->subscribe(topic, 1)->wait();
        }
    }
    catch (const mqtt::exception& e)
    {
    }
}

size_t MQTTSource::fillTupleBuffer(NES::Memory::TupleBuffer& /*tupleBuffer*/)
{
    return 0;
}

void MQTTSource::close()
{
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
MQTTSource::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersMQTT>(std::move(config), NAME);
}

namespace SourceValidationGeneratedRegistrar
{
std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
RegisterMQTTSourceValidation(std::unordered_map<std::string, std::string>&& sourceConfig)
{
    return MQTTSource::validateAndFormat(std::move(sourceConfig));
}
}

namespace SourceGeneratedRegistrar
{
std::unique_ptr<Source> RegisterMQTTSource(const SourceDescriptor& sourceDescriptor)
{
    return std::make_unique<MQTTSource>(sourceDescriptor);
}
}

}

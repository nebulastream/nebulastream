/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <KafkaConnectorConfiguration.hpp>

namespace NES
{
    /**
     * KafkaConnectorConfiguration constructor
     *
     * @param enum CONF_TYPE
     */
    KafkaConnectorConfiguration::KafkaConnectorConfiguration(KafkaConnectorConfiguration::CONF_TYPE type)
    {
        if (type == SOURCE)
        {
            this->connectorConfig = std::make_shared<KafkaConsumerConfiguration>();
        }
        else
        {
            this->connectorConfig = std::make_shared<KafkaSinkConfiguration>();
        }
    }


    /**
     * Set the configuration property
     *
     * @param const std::string& key
     * @param const std::string& value
     * @return void
     */
    void KafkaConnectorConfiguration::setProperty(const std::string & key, const std::string & value)
    {
        this->connectorConfig->put(key, value);
    }


    /**
     * Get the configuration object
     *
     * @return KafkaProperties
     */
    KafkaProperties KafkaConnectorConfiguration::getConfiguration()
    {
        return *this->connectorConfig;
    }

    /**
     * Return configuration as a string
     *
     * @return
     */
    std::string KafkaConnectorConfiguration::toString()
    {
        return this->connectorConfig->toString();
    }
}


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

#ifndef NES_KAFKACONNECTORCONFIGURATION_HPP
#define NES_KAFKACONNECTORCONFIGURATION_HPP

#include <kafka/KafkaConsumer.h>
#include <kafka/KafkaProducer.h>

// FIXME: Clash of syslog.h and NES logger. This is dirty workaround.
#undef 	LOG_EMERG		/* system is unusable */
#undef 	LOG_ALERT		/* action must be taken immediately */
#undef 	LOG_CRIT		/* critical conditions */
#undef 	LOG_ERR			/* error conditions */
#undef 	LOG_WARNING		/* warning conditions */
#undef 	LOG_NOTICE		/* normal but significant condition */
#undef 	LOG_INFO		/* informational */
#undef 	LOG_DEBUG		/* debug-level messages */

using KafkaSinkConfiguration     = NES_KAFKA::ProducerConfig;
using KafkaConsumerConfiguration = NES_KAFKA::ConsumerConfig;
using KafkaProperties            = NES_KAFKA::Properties;
using KafkaPropertiesPtr         = std::shared_ptr<NES_KAFKA::Properties>;

namespace NES
{
    class KafkaConnectorConfiguration
    {
        public:

            /**
             * Marks the type of configuration (source or sink configuration)
             */
            enum CONF_TYPE {SOURCE, SINK};


            /**
             * KafkaConnectorConfiguration constructor
             *
             * @param enum CONF_TYPE
             */
            KafkaConnectorConfiguration(CONF_TYPE);


            /**
             * Set the configuration property
             *
             * @param const std::string& key
             * @param const std::string& value
             * @return void
             */
            void setProperty(const std::string&, const std::string&);


            /**
             * Get the configuration object
             *
             * @return KafkaProperties
             */
            KafkaProperties getConfiguration();


            /**
             * Return configuration as a string
             *
             * @return
             */
            std::string toString();


        private:
            /**
             * Connector configuration
             */
            KafkaPropertiesPtr connectorConfig;
    };
}

#endif //NES_KAFKACONNECTORCONFIGURATION_HPP

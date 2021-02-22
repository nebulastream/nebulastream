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

#include <gtest/gtest.h>

#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>

#include <Util/Logger.hpp>
#include <KafkaConnectorConfiguration.hpp>
#include <Sources/KafkaSourcee.hpp>

#undef 	LOG_EMERG		/* system is unusable */
#undef 	LOG_ALERT		/* action must be taken immediately */
#undef 	LOG_CRIT		/* critical conditions */
#undef 	LOG_ERR			/* error conditions */
#undef 	LOG_WARNING		/* warning conditions */
#undef 	LOG_NOTICE		/* normal but significant condition */
#undef 	LOG_INFO		/* informational */
#undef 	LOG_DEBUG		/* debug-level messages */

namespace NES
{
    class KafkaUnitTest: public testing::Test
    {
        public:
            void SetUp()
            {
                NES::setupLogging("KafkaUnitTest.log", NES::LOG_DEBUG);
                NES_INFO("Setup KafkaUnitTest test class");
            }

            void TearDown()
            {
                NES_DEBUG("Tear down KafkaUnitTest");
            }

        protected:
            SchemaPtr schema;
            NodeEngine::QueryManagerPtr queryManager;
            NodeEngine::BufferManagerPtr bufferManager;
            NodeEngine::NodeEnginePtr nodeEngine{nullptr};
    };


    /**
     * Tests creation of kafka source connector configuration
     */
    TEST_F(KafkaUnitTest, KafkaSourceConnectorInit)
    {
        // connector configuration
        KafkaConnectorConfiguration* connectorConfiguration = new KafkaConnectorConfiguration(KafkaConnectorConfiguration::SOURCE);
        connectorConfiguration->setProperty("bootstrap.servers", "localhost:29092");

        // conf string
        std::string confString = "bootstrap.servers=localhost:29092";

        NES_DEBUG("Coniguration string: " + connectorConfiguration->toString());
        EXPECT_EQ(confString, connectorConfiguration->toString());
    }


    /**
     * Tests creation of a kafka source
     */
    TEST_F(KafkaUnitTest, KafkaSourceInit)
    {
        NES_INFO("KafkaUnitTest: Start NodeEngine");
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        this->nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
        NES_INFO("KafkaUnitTest: NodeEngine started");

        NES_INFO("KafkaUnitTest: Create source dependencies");
        this->schema = Schema::create()->addField("number", UINT16);
        this->bufferManager = nodeEngine->getBufferManager();
        this->queryManager = nodeEngine->getQueryManager();

        Topics topics = {"nes"};
        KafkaConnectorConfiguration* conf = new KafkaConnectorConfiguration(KafkaConnectorConfiguration::SOURCE);
        conf->setProperty("bootstrap.servers", "localhost:29092");
        NES_INFO("KafkaUnitTest: Source dependencies created");

        KafkaSource* kafkaSource = new KafkaSource(
                this->schema,
                this->bufferManager,
                this->queryManager,
                0,
                *conf,
                topics
        );

        SUCCEED();
    }


}


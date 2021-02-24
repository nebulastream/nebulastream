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

#include <Util/Logger.hpp>
#include <Sources/SourceCreator.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>

namespace NES
{
    /**
     * KafkaUnitTest tests kafka sources and sinks
     */
    class KafkaUnitTest : public testing::Test {
    public:

        /**
         * SetUp the test
         */
        void SetUp()
        {
            // create log file
            NES::setupLogging("KafkaUnitTest.log", NES::LOG_DEBUG);

            // start node engine
            NES_INFO("KafkaUnitTest: Start NodeEngine");
            PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
            this->nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
            NES_INFO("KafkaUnitTest: NodeEngine started");

            // create schema, buffer manager and query manager
            NES_INFO("KafkaUnitTest: Create Schema, Buffer Manager and Query Manager");
            this->schema = Schema::create()
                    ->addField("user_id", UINT16)
                    ->addField("page_id", UINT16)
                    ->addField("campaign_id", UINT16)
                    ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                    ->addField("event_type", DataTypeFactory::createFixedChar(9))
                    ->addField("current_ms", UINT64)
                    ->addField("ip", INT32);

            this->bufferManager = nodeEngine->getBufferManager();
            this->queryManager = nodeEngine->getQueryManager();
            NES_INFO("KafkaUnitTest: Created Schema, Buffer Manager and Query Manager");


            NES_INFO("Setup KafkaUnitTest test class");
        }

        /**
         * TearDown the test
         */
        void TearDown() {
            NES_DEBUG("Tear down KafkaUnitTest");
        }

    protected:
        SchemaPtr schema;
        NodeEngine::QueryManagerPtr queryManager;
        NodeEngine::BufferManagerPtr bufferManager;
        NodeEngine::NodeEnginePtr nodeEngine;
    };


    /**
     * Tests creation of kafka source connector configuration
     */
    TEST_F(KafkaUnitTest, KafkaSourceConnectorInit) {
        // connector configuration
        KafkaConnectorConfiguration *connectorConfiguration = new KafkaConnectorConfiguration(
                KafkaConnectorConfiguration::SOURCE);
        connectorConfiguration->setProperty("bootstrap.servers", "localhost:29092");

        // conf string
        std::string confString = "bootstrap.servers=localhost:29092";


        // check that confString string matches given configuration
        NES_DEBUG("Coniguration string: " + connectorConfiguration->toString());
        EXPECT_EQ(confString, connectorConfiguration->toString());
    }


    /**
     * Tests creation of a kafka source
     */
    TEST_F(KafkaUnitTest, KafkaSourceReceiveData)
    {
        // create source configuration
        NES_INFO("KafkaUnitTest: Create source configuration");
        Topics topics = {"nes"};

        KafkaConnectorConfiguration *conf = new KafkaConnectorConfiguration(KafkaConnectorConfiguration::SOURCE);
        conf->setProperty("bootstrap.servers", "localhost:29092");
        conf->setProperty("auto.offset.reset", "beginning");
        NES_INFO("KafkaUnitTest: Source configuration created");

        // create kafka source and receive data
        auto kafkaSource = createKafkaSource(this->schema, this->bufferManager, this->queryManager, 0, *conf, topics);
        auto buffer = kafkaSource->receiveData();

        // get buffer content as string
        std::string bufferContent = UtilityFunctions::printTupleBufferAsCSV(buffer.value(), schema);

        // get content of the file
        std::ifstream file("test_data/ysb-tuples-100-campaign-100.csv");
        std::stringstream fileContent;
        fileContent << file.rdbuf();

        // check that content is the same
        EXPECT_EQ(bufferContent, fileContent.str());
    }
}


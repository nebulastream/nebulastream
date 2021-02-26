#include <gtest/gtest.h>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <KafkaConnectorConfiguration.hpp>
#include <boost/algorithm/string.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Sinks/Mediums/NewKafkaSink.hpp>

#include <Util/Logger.hpp>
#include <fstream>


namespace NES {
    using NodeEngine::TupleBuffer;

    class KafkaSinkUnitTest : public testing::Test {
    public:
        /* Called before any test in this class is started. */
        static void SetUpTestCase() {
            NES::setupLogging("KafkaTest.log", NES::LOG_DEBUG);
            NES_INFO("Setup KafkaTest class.");
        }
        /* Called before a single test is started. */
        void SetUp () override
        {
            std::cout<<"Set up KafkaTest test case."<< std::endl;
            NES_INFO("Set up KafkaTest test case.");
        }

        /* Called after a single test is executed. */
        void TearDown() override {
            std::cout << "Tear down KafkaTest test case." << std::endl;
            NES_INFO("Tear down KafkaTest test case.");
        }

        /* Called after all tests of this class are executed. */
        static void TearDownTestCase() {
            std::cout << "Tear down KafkaTest class." << std::endl;
            NES_INFO("Tear down KafkaTest class.");
        }
    protected:
        SchemaPtr schema;
        NodeEngine::QueryManagerPtr queryManager;
        NodeEngine::BufferManagerPtr bufferManager;
        NodeEngine::NodeEnginePtr nodeEngine{nullptr};
        std::ifstream input;

    };

    /**
 * Tests creation of kafka source connector configuration
 */
    TEST_F(KafkaSinkUnitTest, KafkaSinkConnectorInit)
    {
        // connector configuration
        KafkaConnectorConfiguration* connectorConfiguration = new KafkaConnectorConfiguration(KafkaConnectorConfiguration::SINK);
        connectorConfiguration->setProperty("bootstrap.servers", "localhost:29092");

        // conf string
        std::string confString = "bootstrap.servers=localhost:29092";

        NES_DEBUG("Coniguration string: " + connectorConfiguration->toString());
        EXPECT_EQ(confString, connectorConfiguration->toString());
    }

    TEST_F(KafkaSinkUnitTest, KafkaSinkCreate)
    {
        uint64_t generated_tuples_this_pass;
        bool write_result;
        std::string path_to_csv_file = "/home/djordjije/Desktop/nebulastream/tests/test_data/ysb-tuples-100-campaign-100.csv";
        char* path = realpath(path_to_csv_file.c_str(), NULL);
        NES_DEBUG("KafkaSinkTest: Opening path " << path);
        input.open(path);
        ASSERT_TRUE(input.is_open());


        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
//        bufferManager = std::make_shared<NodeEngine::BufferManager>(1024, 1024);

        TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking(); // empty tuple buffer
        NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId()); // worker context

        auto schema = Schema::create()
                ->addField("user_id", DataTypeFactory::createUInt64())
                ->addField("page_id", DataTypeFactory::createUInt64())
                ->addField("campaign_id", DataTypeFactory::createUInt64())
                ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                ->addField("event_type", DataTypeFactory::createFixedChar(9))
                ->addField("current_ms", DataTypeFactory::createUInt64())
                ->addField("ip", DataTypeFactory::createUInt64());

        uint64_t tupleSize = schema->getSchemaSizeInBytes();
        NES_DEBUG("Schema in bytes: " << tupleSize);
        auto buf = buffer;
        KafkaConnectorConfiguration* connectorConfiguration = new KafkaConnectorConfiguration(KafkaConnectorConfiguration::SINK);
        connectorConfiguration->setProperty("bootstrap.servers", "localhost:29092");

        const DataSinkPtr kafkaSink = createKafkaSink(schema,0,nodeEngine,{"bar"},{{1,2}}, *connectorConfiguration);

        generated_tuples_this_pass = buf.getBufferSize() / tupleSize;
        uint64_t tupCnt = 0;
        uint64_t totalTupCnt = 0;
        std::vector<PhysicalTypePtr> physicalTypes;
        DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
        for (auto field : schema->fields) {
            auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
            physicalTypes.push_back(physicalField);
        }
        for (std::string line; std::getline(input, line);) {
            NES_DEBUG("KafkaSink line=" << totalTupCnt << " val=" << line);
            std::vector<std::string> tokens;
            boost::algorithm::split(tokens, line, boost::is_any_of(","));
            uint64_t offset = 0;
            for (uint64_t j = 0; j < schema->getSize(); j++) {
                auto field = physicalTypes[j];
                uint64_t fieldSize = field->size();

                if (field->isBasicType()) {
                    auto basicPhysicalField = std::dynamic_pointer_cast<BasicPhysicalType>(field);
                    /*
                     * TODO: this requires proper MIN / MAX size checks, numeric_limits<T>-like
                     * TODO: this requires underflow/overflow checks
                     * TODO: our types need their own sto/strto methods
                     */
                    if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_64) {
                        uint64_t val = std::stoull(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_64) {
                        int64_t val = std::stoll(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_32) {
                        uint32_t val = std::stoul(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_32) {
                        int32_t val = std::stol(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16) {
                        uint16_t val = std::stol(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_16) {
                        int16_t val = std::stol(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16) {
                        uint8_t val = std::stoi(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_8) {
                        int8_t val = std::stoi(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_8) {
                        int8_t val = std::stoi(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::DOUBLE) {
                        double val = std::stod(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::FLOAT) {
                        float val = std::stof(tokens[j].c_str());
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::BOOLEAN) {
                        bool val = (strcasecmp(tokens[j].c_str(), "true") == 0 || atoi(tokens[j].c_str()) != 0);
                        memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                    }
                } else {
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, tokens[j].c_str(), fieldSize);
                }

                offset += fieldSize;
                std::cout<< "Offset: ";
                std::cout<<offset;
                std::cout<<" field size: ";
                std::cout<<fieldSize<<std::endl;

            }
            tupCnt++;
            totalTupCnt++;
            if(generated_tuples_this_pass==tupCnt)
            {
                buf.setNumberOfTuples(tupCnt);
                NES_DEBUG("KafkaSinkTest buffer content EXPECTED: \n" << UtilityFunctions::printTupleBufferAsCSV(buf, schema));
                write_result = kafkaSink->writeData(buffer,wctx);
                offset = 0;
                tupCnt = 0;
            }
        }
        // if the reading of the csv file finished and the buffer was not completely full, spill it now to Kafka Cluster
        if(tupCnt < generated_tuples_this_pass)
        {
            buf.setNumberOfTuples(tupCnt);
            NES_DEBUG("KafkaSinkTest buffer content EXPECTED: \n" << UtilityFunctions::printTupleBufferAsCSV(buf, schema));
            write_result = kafkaSink->writeData(buffer,wctx);
        }
        EXPECT_TRUE(write_result);

        SUCCEED();
    }
}

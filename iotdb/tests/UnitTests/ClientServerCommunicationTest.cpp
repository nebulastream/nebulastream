#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include <gtest/gtest.h>
#include <zmq.hpp>

#ifndef TESTING
#define TESTING
#endif // TESTING
#include "../../iotClient.cpp"
#include "../../iotServer.cpp"

/* ------------------------------------------------------------------------- */
/* - Client Server Communication ------------------------------------------- */
class ClientServerCommunicationTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup ClientServerTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp()
    {
        std::cout << "Setup ClientServerTest test case." << std::endl;
        client_queries_id = std::make_shared<size_t>(0);
        client_queries = std::make_shared<IotServer::client_queries_t>();

        client_queries->insert(IotServer::client_query_t(
            (*client_queries_id)++,
            std::tuple<std::string, std::string>(std::string("query_0.cpp"), std::string("Content\nFile 0"))));
        client_queries->insert(IotServer::client_query_t(
            (*client_queries_id)++,
            std::tuple<std::string, std::string>(std::string("query_1.cpp"), std::string("Content\nFile 1"))));
    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Setup ClientServerTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ClientServerTest test class." << std::endl; }

    const static uint16_t port = MASTER_PORT;
    std::shared_ptr<size_t> client_queries_id;
    std::shared_ptr<IotServer::client_queries_t> client_queries;
};

/* Receive 'ADD_QEUERY'-command. */
TEST_F(ClientServerCommunicationTest, add_query)
{

    auto test_file_path = std::string(TEST_DATA_DIRECTORY).append("/ClientServerCommunication_TestQuery.cpp");
    std::cout << "Path of the test file: " << test_file_path << std::endl;

    auto request = IotClient::add_query_request(test_file_path);
    EXPECT_NE(request.get(), nullptr);

    auto reply = IotServer::add_query_request(request, client_queries, client_queries_id);
    EXPECT_NE(reply.get(), nullptr);

    auto result = IotClient::add_query_reply(reply);
    EXPECT_EQ(result, 2);

    std::ifstream ifs(test_file_path.c_str());
    std::string file_content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    EXPECT_TRUE(std::get<0>((*client_queries)[2]) == std::string("ClientServerCommunication_TestQuery.cpp"));
    EXPECT_TRUE(std::get<1>((*client_queries)[2]) == file_content);
}

/* Receive 'REMOVE_QUERY'-command, query id is present. */
TEST_F(ClientServerCommunicationTest, remove_query_existing)
{
    auto request = IotClient::remove_query_request(1);
    EXPECT_NE(request.get(), nullptr);

    auto reply = IotServer::remove_query_request(request, client_queries);
    EXPECT_NE(reply.get(), nullptr);

    auto result = IotClient::remove_query_reply(reply);
    EXPECT_EQ(result, true);
}

/* Receive 'REMOVE_QUERY'-command, query id is not present. */
TEST_F(ClientServerCommunicationTest, remove_query_not_existing)
{
    auto request = IotClient::remove_query_request(3);
    EXPECT_NE(request.get(), nullptr);

    auto reply = IotServer::remove_query_request(request, client_queries);
    EXPECT_NE(reply.get(), nullptr);

    auto result = IotClient::remove_query_reply(reply);
    EXPECT_EQ(result, false);
}

/* Receive 'LIST_QUERIES'-command. */
TEST_F(ClientServerCommunicationTest, list_queries)
{
    auto request = IotClient::list_queries_request();
    EXPECT_NE(request.get(), nullptr);

    auto reply = IotServer::list_queries_request(request, client_queries);
    EXPECT_NE(reply.get(), nullptr);

    auto result = IotClient::list_queries_reply(reply);
    EXPECT_TRUE(result[0].first == 0);
    EXPECT_TRUE(result[0].second == std::string("query_0.cpp"));
    EXPECT_TRUE(result[1].first == 1);
    EXPECT_TRUE(result[1].second == std::string("query_1.cpp"));
}

#include <iostream>
#include <thread>

#include "gtest/gtest.h"

#ifndef TESTING
#define TESTING
#endif // TESTING
#include "../../iotClient.cpp"
#include "../../iotServer.cpp"

/* ------------------------------------------------------------------------- */
/* - Client Server Communication ------------------------------------------- */
class IotClientServerTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup ClientServerTest test class." << std::endl;
    client_queries_id = 0;
    client_queries[client_queries_id++] =
        std::tuple<std::string, std::string>(std::string("query_1.cpp"), std::string("Content\nFile 1"));
    client_queries[client_queries_id++] =
        std::tuple<std::string, std::string>(std::string("query_2.cpp"), std::string("Content\nFile 2"));
    server_thread = std::thread(&IotServer::receive_reply, &client_queries, &client_queries_id);
  }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup ClientServerTest test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup ClientServerTest test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() {
    std::cout << "Tear down ClientServerTest test class." << std::endl;
    server_thread.join();
  }

private:
  static size_t client_queries_id;
  static IotServer::client_queries_t client_queries;
  static std::thread server_thread;
};

/* Receive 'ADD_QEUERY'-command. */

/* Receive 'REMOVE_QUERY'-command, query id is present. */

/* Receive 'REMOVE_QUERY'-command, query id is not present. */

/* Receive 'LIST_QUERIES'-command. */

/* Receive unknown command. */

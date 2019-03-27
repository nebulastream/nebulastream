//
// blocking_tcp_echo_server.cpp
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2011 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <cstdlib>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <thread>
#include <NodeEngine/json.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <string>
#include <boost/lexical_cast.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <CodeGen/QueryExecutionPlan.hpp>
#include <QEPs/CompiledYSBTestQueryExecutionPlan.hpp>


using boost::asio::ip::tcp;
using JSON = nlohmann::json;
using namespace iotdb;
using namespace std;
const int max_length = 1024*10;
const size_t serverPort = 5555;
const size_t clientPort = 6666;
const std::string clientPortStr = "6666";

enum NODE_COMMANDS {
	START_QUERY = 1,
	STOP_QUERY = 2,
	DEPLOY_QEP = 3 };

typedef boost::shared_ptr<tcp::socket> socket_ptr;

std::map<std::string, NodePropertiesPtr> nodes;
void session(socket_ptr sock)
{
  try
  {
      char data[max_length];
      boost::system::error_code error;
      size_t length = sock->read_some(boost::asio::buffer(data), error);
      assert(length < max_length);
      if (error)
        throw boost::system::system_error(error); // Some other error.g

      NodePropertiesPtr ptr = std::make_shared<NodeProperties>();
      ptr->load(data);
      IOTDB_DEBUG("IOTBROKER: sending replay")
      char reply[14];
      IOTDB_DEBUG("IOTBROKER: Host= " << ptr->getHostname() << " try to register")
      if ( nodes.find(ptr->getHostname()) == nodes.end() ) {
    	  IOTDB_DEBUG("IOTBROKER: registering node")
		  nodes[ptr->getHostname()] = ptr;
		  memcpy(reply, "REG_COMPLETED", 13);
	  } else {
		  IOTDB_DEBUG("IOTBROKER: Node already registered")
		  memcpy(reply, "ALREADY_REG", 11);

	  }
  	boost::asio::write(*sock, boost::asio::buffer(reply, sizeof(reply)));
  	IOTDB_DEBUG("IOTBROKER: registration process completed")
  }
  catch (std::exception& e)
  {
	  IOTDB_ERROR("IOTBROKER: Exception in registration: " << e.what())
  }
}

void server()
{
  boost::asio::io_service io_service;
  tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), serverPort));
  for (;;)
  {
    socket_ptr sock(new tcp::socket(io_service));
    a.accept(*sock);
    boost::thread t(boost::bind(session, sock));
  }
}

void save_qep(const QueryExecutionPlan* s, const char * filename){
    // make an archive
    std::ofstream ofs(filename);
    boost::archive::text_oarchive oa(ofs);
    oa << s;
}
std::string generateTestQEP()
{
    Schema schema = Schema::create()
        .addField("campaign_id", 16)
        .addField("event_type", 9)
        .addField("current_ms", 8)
        .addField("id", 4);

    std::string filename = "/home/zeuchste/git/IoTDB/iotdb/build/tests/testQep.txt";
    CompiledYSBTestQueryExecutionPlanPtr q(new CompiledYSBTestQueryExecutionPlan());

    DataSourcePtr src = createYSBSource(100,10, /*pregen*/ false);
    q->addDataSource(src);
//    DataSourcePtr zmq_src = createZmqSource(schema, "127.0.0.1", 55555);
//    q->addDataSource(zmq_src);

    WindowPtr window = createTestWindow(10);
    window->setup();
    q->addWindow(window);

    DataSinkPtr sink = createYSBPrintSink();
    q->addDataSink(sink);
//    DataSinkPtr zmq_sink = createZmqSink(schema, "127.0.0.1", 55555);
//    q->addDataSink(zmq_sink);

    save_qep(q.get(), filename.c_str());

    return filename;
}


char* readFile(std::string fileName, size_t& size)
{
    FILE *f = fopen(fileName.c_str(), "rb");
    fseek(f, 0, SEEK_END);
    size = ftell(f);
    fseek(f, 0, SEEK_SET);
    char* data = (char *)malloc(size + 1);
    fread(data, size, 1, f);
    fclose(f);
    data[size] = 0;
    return data;
}

void sendCommandToNodes(std::string command)
{
	for(auto& node : nodes)
	{
		boost::asio::io_service io_service;
		tcp::resolver resolver(io_service);
		IOTDB_DEBUG("IOTBROKER: resolve address for " << node.first)
		tcp::resolver::query query(tcp::v4(), node.first, clientPortStr.c_str(), boost::asio::ip::resolver_query_base::numeric_service);
		tcp::resolver::iterator iterator = resolver.resolve(query);
		tcp::socket s(io_service);
		s.connect(*iterator);
		IOTDB_DEBUG("IOTBROKER: connected to " << node.first << ":" << clientPort << " successfully")


	    if(command == "3")
	    {
	        IOTDB_DEBUG("IOTBROKER: sending QEP")
	        size_t fileSize = 0;
	        std::string fileName = generateTestQEP();
//	        std::string fileName = "/home/zeuchste/git/IoTDB/iotdb/build/tests/demofile.txt";
	        char* data = readFile(fileName, fileSize);
	        IOTDB_DEBUG("IOTBROKER: send QEP serialized data:" << data)

	        char* sendBuffer = new char[fileSize + 1];
	        char cmdChar = command[0];
            memcpy(sendBuffer, &cmdChar, 1);
            memcpy(&sendBuffer[1], data, fileSize);

	        boost::asio::write(s, boost::asio::buffer(sendBuffer, fileSize + 32));
	        delete sendBuffer;
	        delete data;
	    }
	    else
	    {
	        IOTDB_DEBUG("IOTBROKER: send command " << command << " size=" << sizeof(command))
            boost::asio::write(s, boost::asio::buffer(command.c_str(), sizeof(command)));
	    }

        IOTDB_DEBUG("IOTBROKER: send command completed")

	}
}

void setupLogging()
{
	 // create PatternLayout
	log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

	// create FileAppender
	LOG4CXX_DECODE_CHAR(fileName, "iotbroker.log");
	log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

	// create ConsoleAppender
	log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

	// set log level
	//logger->setLevel(log4cxx::Level::getTrace());
	logger->setLevel(log4cxx::Level::getDebug());
//	logger->setLevel(log4cxx::Level::getInfo());
//	logger->setLevel(log4cxx::Level::getWarn());
	//logger->setLevel(log4cxx::Level::getError());
//	logger->setLevel(log4cxx::Level::getFatal());

	// add appenders and other will inherit the settings
	logger->addAppender(file);
	logger->addAppender(console);
}

int main(int argc, char* argv[])
{
	setupLogging();
  try
  {
    if (argc != 1)
    {
      std::cerr << "no args required";
      return 1;
    }

    boost::thread t(server);
    IOTDB_DEBUG("IOTBROKER: server started")

    std::string command = "0";

	while(command != "4"){
		std::cout << "Please enter node command:";
		std::cout << "1 = START_QUERY";
		std::cout << "2 = STOP_QUERY";
		std::cout << "3 = DEPLOY_QEP";
		std::cout << "4 = QUIT"  <<std::endl;
		cin >> command; // input the length

		IOTDB_DEBUG("IOTBROKER: User entered command " << command)
		sendCommandToNodes(command);
	}

  }
  catch (std::exception& e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}

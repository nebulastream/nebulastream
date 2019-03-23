//
// blocking_tcp_echo_client.cpp
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2011 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <boost/asio.hpp>
#include <NodeEngine/NodeEngine.hpp>

using boost::asio::ip::tcp;
using namespace iotdb;
using namespace std; // For strlen.

enum { max_length = 1024 };

boost::asio::io_service io_service;


void registerNodeInFog(string host, string port)
{
	tcp::resolver resolver(io_service);
	tcp::resolver::query query(tcp::v4(), host, port);
	tcp::resolver::iterator iterator = resolver.resolve(query);
	tcp::socket s(io_service);
	s.connect(*iterator);
	cout << "connected to " << host << ":" << port << " successfully";

	NodeEnginePtr node = std::make_shared<NodeEngine>(1);
	JSON props = node->getNodeProperties();
//	node->printNodeProperties();
//	node->printMetric();


//	cout << "write data size=" << props.dump().size() << endl;
	boost::asio::write(s, boost::asio::buffer(props.dump(), props.dump().size()));
//	std::this_thread::sleep_for(std::chrono::seconds(1));
//
	char reply[14];
	size_t reply_length = boost::asio::read(s, boost::asio::buffer(reply, max_length));
	std::cout << "Reply is: ";
	std::cout.write(reply, reply_length);
	std::cout << "\n";
}

void setupLogging()
{
	 // create PatternLayout
	log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

	// create FileAppender
	LOG4CXX_DECODE_CHAR(fileName, "iotdb.log");
	log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

	// create ConsoleAppender
	log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

	// set log level
	//logger->setLevel(log4cxx::Level::getTrace());
//	logger->setLevel(log4cxx::Level::getDebug());
	logger->setLevel(log4cxx::Level::getInfo());
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
    if (argc != 3)
    {
      std::cerr << "Usage: blocking_tcp_echo_client <host> <port>\n";
      return 1;
    }
    std::string host = argv[1];
    std::string port = argv[2];

    registerNodeInFog(host, port);

  }
  catch (std::exception& e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}

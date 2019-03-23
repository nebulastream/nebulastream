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
#include <boost/thread.hpp>

#include <NodeEngine/NodeEngine.hpp>
enum NODE_COMMANDS {
	START_QUERY = 1,
	STOP_QUERY = 2,
	DEPLOY_QEP = 3 };

using boost::asio::ip::tcp;
using namespace iotdb;
using namespace std; // For strlen.

const size_t serverPort = 5555;
const size_t clientPort = 6666;
enum { max_length = 1024*10 };

boost::asio::io_service io_service;
typedef boost::shared_ptr<tcp::socket> socket_ptr;


bool registerNodeInFog(string host, string port)
{
	tcp::resolver resolver(io_service);
	tcp::resolver::query query(tcp::v4(), host, port);
	tcp::resolver::iterator iterator = resolver.resolve(query);
	tcp::socket s(io_service);
	s.connect(*iterator);
	cout << "connected to " << host << ":" << port << " successfully";

	NodeEnginePtr node = std::make_shared<NodeEngine>();
	JSON props = node->getNodeProperties();

	boost::asio::write(s, boost::asio::buffer(props.dump(), props.dump().size()));
//
	char reply[14];
	boost::asio::read(s, boost::asio::buffer(reply, max_length));
	std::cout << "Reply is: " << reply << std::endl;
}

void commandProcess(socket_ptr sock)
{
	char data[max_length];
	boost::system::error_code error;
	size_t length = sock->read_some(boost::asio::buffer(data), error);
	assert(length < max_length);
	if (error)
		throw boost::system::system_error(error);

	//first char identifies cmd
	if(data[0] == START_QUERY)
	{
		std::cout << "starting query" << std::endl;
	}
	else if(data[1] == STOP_QUERY)
	{
		std::cout << "stop query" << std::endl;
	}
	else if(data[2] == DEPLOY_QEP)
	{
		std::cout << "deploy query" << std::endl;
	}

}
void listen(boost::asio::io_service& io_service, short port){
	tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), port));
	  for (;;)
	  {
	    socket_ptr sock(new tcp::socket(io_service));
	    a.accept(*sock);
	    boost::thread t(boost::bind(commandProcess, sock));
	  }
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

    bool successReg = registerNodeInFog(host, port);

    if(successReg)
    {
        std::cout << "waiting for commands" << std::endl;
        boost::asio::io_service io_service;
        listen(io_service, clientPort);
    }
    else
    {
    	return -1;
    }

  }
  catch (std::exception& e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}

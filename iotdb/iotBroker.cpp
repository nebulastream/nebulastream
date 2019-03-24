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
      std::cout << "sending replay" << std::endl;
      char reply[14];
      std::cout << "Host= " << ptr->getHostname() << " try to register"<< std::endl;
      if ( nodes.find(ptr->getHostname()) == nodes.end() ) {
		  std::cout << "registering node" << std::endl;
		  nodes[ptr->getHostname()] = ptr;
		  memcpy(reply, "REG_COMPLETED", 13);
	  } else {
		  std::cout << "Node already registered" << std::endl;
		  memcpy(reply, "ALREADY_REG", 11);

	  }
  	boost::asio::write(*sock, boost::asio::buffer(reply, sizeof(reply)));
	std::cout << "process complete" << std::endl;
  }
  catch (std::exception& e)
  {
    std::cerr << "Exception in thread: " << e.what() << "\n";
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

void sendCommandToNodes(std::string command)
{
	for(auto& node : nodes)
	{
		boost::asio::io_service io_service;
		tcp::resolver resolver(io_service);
		cout << "resolve address for " << node.first << std::endl;
		tcp::resolver::query query(tcp::v4(), node.first, clientPortStr.c_str(), boost::asio::ip::resolver_query_base::numeric_service);
		tcp::resolver::iterator iterator = resolver.resolve(query);
		tcp::socket s(io_service);
		s.connect(*iterator);
		cout << "connected to " << node.first << ":" << clientPort << " successfully" << std::endl;

		cout << "send command " << command << " size=" << sizeof(command) << std::endl;
	  	boost::asio::write(s, boost::asio::buffer(command.c_str(), sizeof(command)));
	}
}


int main(int argc, char* argv[])
{
  try
  {
    if (argc != 1)
    {
      std::cerr << "no args required";
      return 1;
    }

    boost::thread t(server);
    std::cout << "server started" << std::endl;

    std::string command = "0";


	while(command != "4"){
		std::cout << "Please enter node command:";
		std::cout << "1 = START_QUERY";
		std::cout << "2 = STOP_QUERY";
		std::cout << "3 = DEPLOY_QEP";
		std::cout << "4 = QUIT"  <<std::endl;
		cin >> command; // input the length

		std::cout << "Exec Command, " << command << std::endl;
		sendCommandToNodes(command);
	}

  }
  catch (std::exception& e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}

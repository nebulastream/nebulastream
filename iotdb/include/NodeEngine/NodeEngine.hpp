#ifndef _IOT_NODE_H
#define _IOT_NODE_H

#include <iostream>
#include <string>
#include <zmq.hpp>
#include <vector>
#include <pthread.h>
#include <unistd.h>
#include "json.hpp"
#include "NodeProperties.hpp"

namespace iotdb{
using JSON = nlohmann::json;

class NodeEngine {
public:
	NodeEngine(uint64_t id) : id(id), threadPool(1)
	{
		props = new NodeProperties();
	}

	JSON getNodeProperties();

	size_t getId(){return id;};

	void sendNodePropertiesToServer(std::string ip, std::string port);

	void printNodeProperties();

private:
  std::vector<pthread_t> threadPool;
  NodeProperties* props;
  uint64_t id;
};

typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

}
#endif // _IOT_NODE_H

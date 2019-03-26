#ifndef _IOT_NODE_H
#define _IOT_NODE_H

#include "../Runtime/CompiledDummyPlan.hpp"
#include "NodeProperties.hpp"
#include "json.hpp"
#include <CodeGen/QueryExecutionPlan.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/ThreadPool.hpp>
#include <iostream>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <vector>
#include <zmq.hpp>

namespace iotdb {
using JSON = nlohmann::json;

class NodeEngine {
  public:
    NodeEngine(uint64_t id) : id(id), threadPool(1)
    {
        props = new NodeProperties();
        iotdb::Dispatcher::instance();
    }

    JSON getNodeProperties();

    size_t getId() { return id; };

    void deployQuery(QueryExecutionPlanPtr ptr);

    void sendNodePropertiesToServer(std::string ip, std::string port);

    void printNodeProperties();

  private:
    std::vector<pthread_t> threadPool;
    NodeProperties* props;
    uint64_t id;
};

typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

} // namespace iotdb
#endif // _IOT_NODE_H

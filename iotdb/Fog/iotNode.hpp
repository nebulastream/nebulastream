#ifndef _IOT_NODE_H
#define _IOT_NODE_H

#include <iostream>
#include <string>
#include <zmq.hpp>
#include <vector>
#include <pthread.h>
#include <unistd.h>
#include "metrics.hpp"
#include "json.hpp"

using JSON = nlohmann::json;

class IotNode {
public:
  IotNode(uint64_t id=10) : id(id), threadPool(1) {
    this->startNodeInfoReporter();
  }

  static void *runNodeInfoReporter(void *arg) {
    uint64_t id = *((uint64_t *)arg);

    zmq::context_t context (1);
    zmq::socket_t socket (context, ZMQ_REQ);
    std::cout << "Connecting to master..." << std::endl;
    socket.connect("tcp://localhost:5555");

    Metrics *metrics = new Metrics();
    JSON result;
    // report node information every *interval* seconds
    while (true) {
      metrics->readCpuStats();
      metrics->readNetworkStats();
      metrics->readMemStats();
      metrics->readFsStats();

      JSON j = metrics->load();
      result[std::to_string(id)] = j;
      result[std::to_string(id)]["precombinedbinary"] = j.dump();
      result[std::to_string(id)]["queryplan"] = j.dump();

      int size = result.dump().size();
      zmq::message_t request(size);
      memcpy(request.data(), result.dump().c_str(), size);
      socket.send(request);

      zmq::message_t reply;
      socket.recv(&reply);
      sleep(1);
    }
  }

  void startNodeInfoReporter() {
    int rc = pthread_create (&threadPool[0], NULL, runNodeInfoReporter, (void *)(&this->id));
    assert(rc == 0);
    std::cout << threadPool.size() << " threads fired up..." << std::endl;
  }

  void serve() {
    do {

    } while (true);
  }

private:
  std::vector<pthread_t> threadPool;
  // Metrics *metrics;
  uint64_t id;
};

#endif // _IOT_NODE_H

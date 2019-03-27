#include <Optimizer/FogRunTime.hpp>

namespace iotdb {

void FogRunTime::registerNode(NodeEnginePtr ptr)
{
    if (nodeInfos.find(ptr->getId()) == nodeInfos.end()) {
        nodeInfos[ptr->getId()] = ptr;
    }
    else {
        assert(0);
    }
}

void FogRunTime::deployQuery(FogExecutionPlan fogPlan) { assert(0); }

void FogRunTime::deployQuery(QueryExecutionPlanPtr cPlan)
{
    // setup node by sending query
    assert(nodeInfos.size() == 1);
    for (auto node : nodeInfos) {
        node.second->deployQuery(cPlan);
    }

    // setup sensor

    // start query
}

void FogRunTime::receiveNodeInfo()
{
    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_REP);
    socket.bind("tcp://*:5555");
    int mallocSize = 8096 * 10;
    char* dest = (char*)malloc(sizeof(char) * mallocSize);
    // char dest[8096] = { 0 };
    while (true) {
        zmq::message_t request;
        socket.recv(&request);
        if (request.size() > mallocSize) {
            mallocSize = request.size();
            free(dest);
            dest = (char*)malloc(sizeof(char) * mallocSize);
        }
        memcpy(dest, request.data(), request.size());
        dest[request.size()] = '\0';

        zmq::message_t reply(5);
        memcpy(reply.data(), "World", 5);
        socket.send(reply);
    }
}

} // namespace iotdb

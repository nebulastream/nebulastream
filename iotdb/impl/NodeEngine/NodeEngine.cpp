#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <string>
//#include <tests/testPlans/compiledTestPlan.hpp>

using namespace std;
namespace iotdb {
// todo: better return ptr ?
JSON NodeEngine::getNodeProperties()
{
    props->readMemStats();
    props->readCpuStats();
    props->readNetworkStats();
    props->readFsStats();

    return props->load();
}

void NodeEngine::sendNodePropertiesToServer(std::string ip, std::string port)
{

    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_REQ);
    std::cout << "Connecting to master..." << std::endl;
    std::string connectStr = "tcp://" + ip + ":" + port;
    socket.connect(connectStr.c_str());

    NodeProperties* metrics = new NodeProperties();
    JSON result;

    int size = result.dump().size();
    zmq::message_t request(size);
    memcpy(request.data(), result.dump().c_str(), size);
    socket.send(request);

    zmq::message_t reply;
    socket.recv(&reply);
    sleep(1);
}

void NodeEngine::deployQuery(QueryExecutionPlanPtr qep)
{
    // TODO:add compile here
    Dispatcher::instance().registerQuery(qep);

    ThreadPool thread_pool;

    thread_pool.start();

    std::cout << "Waiting 2 seconds " << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
}

void NodeEngine::printNodeProperties()
{
    cout << "cpu stats=" << endl;
    cout << props->getCpuStats() << endl;

    cout << "network stats=" << endl;
    cout << props->getNetworkStats() << endl;

    cout << "memory stats=" << endl;
    cout << props->getMemStats() << endl;

    cout << "filesystem stats=" << endl;
    cout << props->getFsStats() << endl;
}
} // namespace iotdb

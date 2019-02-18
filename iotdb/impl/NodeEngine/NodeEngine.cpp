#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <string>

using namespace std;
namespace iotdb{
//todo: better return ptr ?
JSON NodeEngine::getNodeProperties()
{
	props->readCpuStats();
	props->readNetworkStats();
	props->readMemStats();
	props->readFsStats();

	return props->load();
}

void NodeEngine::sendNodePropertiesToServer(std::string ip, std::string port)
{

	zmq::context_t context (1);
	zmq::socket_t socket (context, ZMQ_REQ);
	std::cout << "Connecting to master..." << std::endl;
	std::string connectStr = "tcp://" + ip + ":" + port;
	socket.connect(connectStr.c_str());

	NodeProperties *metrics = new NodeProperties();
	JSON result;

	int size = result.dump().size();
	zmq::message_t request(size);
	memcpy(request.data(), result.dump().c_str(), size);
	socket.send(request);

	zmq::message_t reply;
	socket.recv(&reply);
	sleep(1);
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
}

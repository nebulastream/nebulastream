#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <string>
//#include <tests/testPlans/compiledTestPlan.hpp>

using namespace std;
namespace iotdb{
JSON NodeEngine::getNodePropertiesAsJSON()
{
	props->readMemStats();
	props->readCpuStats();
	props->readNetworkStats();
	props->readDiskStats();

	return props->load();
}

NodeProperties* NodeEngine::getNodeProperties()
{
    return props;
}

void NodeEngine::deployQuery(QueryExecutionPlanPtr qep)
{
  IOTDB_DEBUG("NODEENGINE: deploy query" << qep)

	Dispatcher::instance().registerQuery(qep);
	qeps.push_back(qep);
}

void NodeEngine::undeployQuery(QueryExecutionPlanPtr qep)
{
    IOTDB_DEBUG("NODEENGINE: deregister query" << qep)
    Dispatcher::instance().deregisterQuery(qep);

    qeps.erase(std::find(qeps.begin(),qeps.end(),qep));
}

void NodeEngine::init()
{
    IOTDB_DEBUG("NODEENGINE: init node engine")

    iotdb::Dispatcher::instance();
    iotdb::BufferManager::instance();
    iotdb::ThreadPool::instance();
}

void NodeEngine::start()
{
    IOTDB_DEBUG("NODEENGINE: start thread pool")
    ThreadPool::instance().start();
}

void NodeEngine::startWithRedeploy()
{
    for(auto& q : qeps)
    {
        IOTDB_DEBUG("NODEENGINE: register query " << q)
        Dispatcher::instance().registerQuery(q);
    }
    IOTDB_DEBUG("NODEENGINE: start thread pool")
    ThreadPool::instance().start();
}

void NodeEngine::stop()
{
    IOTDB_DEBUG("NODEENGINE: stop thread pool")
    ThreadPool::instance().stop();
}

void NodeEngine::stopWithUndeploy()
{
    IOTDB_DEBUG("NODEENGINE: stop thread pool")
    ThreadPool::instance().stop();
    for(auto& q : qeps)
    {
        IOTDB_DEBUG("NODEENGINE: deregister query " << q)
        Dispatcher::instance().deregisterQuery(q);
    }
}

void NodeEngine::applyConfig(Config& conf)
{
    if(conf.getNumberOfWorker() != ThreadPool::instance().getNumberOfThreads())
    {
        IOTDB_DEBUG("NODEENGINE: changing numberOfWorker from " << ThreadPool::instance().getNumberOfThreads() << " to " << conf.getNumberOfWorker())
        ThreadPool::instance().setNumberOfThreads(conf.getNumberOfWorker());
    }
    if(conf.getBufferCount() !=  BufferManager::instance().getNumberOfBuffers())
    {
        IOTDB_DEBUG("NODEENGINE: changing bufferCount from " << BufferManager::instance().getNumberOfBuffers() << " to " << conf.getBufferCount())
        BufferManager::instance().setNumberOfBuffers(conf.getBufferCount());
    }
    if(conf.getBufferSizeInByte() !=  BufferManager::instance().getBufferSize())
    {
        IOTDB_DEBUG("NODEENGINE: changing buffer size from " << BufferManager::instance().getBufferSize() << " to " << conf.getBufferSizeInByte())
        BufferManager::instance().setBufferSize(conf.getBufferSizeInByte());
    }
    IOTDB_DEBUG("NODEENGINE: config successuflly changed")
}

void NodeEngine::resetQEPs()
{
    for(auto& q : qeps)
    {
        IOTDB_DEBUG("NODEENGINE: deregister query " << q)
        Dispatcher::instance().deregisterQuery(q);
    }
    IOTDB_DEBUG("NODEENGINE: clear qeps")

    qeps.clear();
}

}

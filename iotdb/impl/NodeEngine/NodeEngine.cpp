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
	Dispatcher::instance().registerQuery(qep);

//	ThreadPool::instance().start(1);
//    std::cout << "Waiting 2 seconds " << std::endl;
//    std::this_thread::sleep_for(std::chrono::seconds(2));
}

void NodeEngine::undeployQuery(QueryExecutionPlanPtr qep)
{
    Dispatcher::instance().deregisterQuery(qep);
}

void NodeEngine::init()
{
    iotdb::Dispatcher::instance();
    iotdb::BufferManager::instance();
    iotdb::ThreadPool::instance();
}

void NodeEngine::start()
{
    for(auto& q : qeps)
    {
        IOTDB_DEBUG("IOTNODE: register query " << q)
        Dispatcher::instance().registerQuery(q);
    }
    IOTDB_DEBUG("IOTNODE: start thread pool")
    ThreadPool::instance().start();
}

void NodeEngine::stop()
{
    IOTDB_DEBUG("IOTNODE: stop thread pool")
    ThreadPool::instance().stop();
    for(auto& q : qeps)
    {
        IOTDB_DEBUG("IOTNODE: deregister query " << q)
        Dispatcher::instance().deregisterQuery(q);
    }
}

void NodeEngine::applyConfig(Config& conf)
{
    if(conf.getNumberOfWorker() != ThreadPool::instance().getNumberOfThreads())
    {
        IOTDB_DEBUG("IOTNODE: changing numberOfWorker from " << ThreadPool::instance().getNumberOfThreads() << " to " << conf.getNumberOfWorker())
        ThreadPool::instance().setNumberOfThreads(conf.getNumberOfWorker());
    }
    if(conf.getBufferCount() !=  BufferManager::instance().getNumberOfBuffers())
    {
        IOTDB_DEBUG("IOTNODE: changing bufferCount from " << BufferManager::instance().getNumberOfBuffers() << " to " << conf.getBufferCount())
        BufferManager::instance().setNumberOfBuffers(conf.getBufferCount());
    }
    if(conf.getBufferSizeInByte() !=  BufferManager::instance().getBufferSize())
    {
        IOTDB_DEBUG("IOTNODE: changing buffer size from " << BufferManager::instance().getBufferSize() << " to " << conf.getBufferSizeInByte())
        BufferManager::instance().setBufferSize(conf.getBufferSizeInByte());
    }
    IOTDB_DEBUG("IOTNODE: config successuflly changed")

}

}

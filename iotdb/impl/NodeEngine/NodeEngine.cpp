#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <string>
#include <Util/Logger.hpp>

using namespace std;
namespace NES {

static constexpr size_t DEFAULT_BUFFER_SIZE = 4096;
static constexpr size_t DEFAULT_NUM_BUFFERS = 1024;

JSON NodeEngine::getNodePropertiesAsJSON() {
    props->readMemStats();
    props->readCpuStats();
    props->readNetworkStats();
    props->readDiskStats();

    return props->getExistingMetrics();
}

std::string NodeEngine::getNodePropertiesAsString() {
    props->readMemStats();
    props->readCpuStats();
    props->readNetworkStats();
    props->readDiskStats();

    return props->dump();
}

NodeProperties* NodeEngine::getNodeProperties() {
    return props.get();
}

NodeEngine::NodeEngine() {
    props = std::make_shared<NodeProperties>();

    stoppedEngine = false;
    forceStop = false;
}

NodeEngine::~NodeEngine() {
    forceStop = true;
    stop();
}

bool NodeEngine::deployQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("NodeEngine: deployQuery query " << qep)

    bool successRegister = registerQuery(qep);
    if (!successRegister) {
        NES_ERROR("NodeEngine::deployQuery: failed to register query")
        return false;
    } else {
        NES_DEBUG("NodeEngine::deployQuery: successfully register query")
    }
    bool successStart = startQuery(qep);
    if (!successStart) {
        NES_ERROR("NodeEngine::deployQuery: failed to start query")
        return false;
    } else {
        NES_DEBUG("NodeEngine::deployQuery: successfully start query")
    }
    return true;
}

bool NodeEngine::registerQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("NodeEngine: registerQuery query " << qep)

    if (queryStatusMap.find(qep) == queryStatusMap.end()) {
        if (dispatcher->registerQuery(qep)) {
            queryStatusMap.insert({qep, NodeEngineQueryStatus::registered});
            NES_DEBUG("NodeEngine: register of QEP " << qep << " succeeded")
            return true;
        } else {
            NES_DEBUG("NodeEngine: register of QEP " << qep << " failed")
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep already exists. register failed" << qep)
        return false;
    }
}

bool NodeEngine::startQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("NodeEngine: startQuery=" << qep)
    if (queryStatusMap.find(qep) != queryStatusMap.end()) {
        if (dispatcher->startQuery(qep)) {
            NES_DEBUG("NodeEngine: start of QEP " << qep << " succeeded")
            queryStatusMap[qep] = NodeEngineQueryStatus::started;
            return true;
        } else {
            NES_DEBUG("NodeEngine: start of QEP " << qep << " failed")
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep does not exists. start failed" << qep)
        return false;
    }
}

bool NodeEngine::undeployQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("NodeEngine: undeployQuery query=" << qep)
    bool successStop = stopQuery(qep);
    if (!successStop) {
        NES_ERROR("NodeEngine::undeployQuery: failed to stop query")
        return false;
    } else {
        NES_DEBUG("NodeEngine::undeployQuery: successfully stop query")
    }

    bool successUnregister = unregisterQuery(qep);
    if (!successUnregister) {
        NES_ERROR("NodeEngine::undeployQuery: failed to unregister query")
        return false;
    } else {
        NES_DEBUG("NodeEngine::undeployQuery: successfully unregister query")
        return true;
    }
}

bool NodeEngine::unregisterQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("NodeEngine: unregisterQuery query=" << qep)
    if (queryStatusMap.find(qep) != queryStatusMap.end()) {
        if (dispatcher->deregisterQuery(qep)) {
            size_t delCnt = queryStatusMap.erase(qep);
            NES_DEBUG("NodeEngine: unregister of QEP " << qep << " succeeded with cnt=" << delCnt)
            return true;
        } else {
            NES_ERROR("NodeEngine: unregister of QEP " << qep << " failed")
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep does not exists. unregister failed" << qep)
        return false;
    }
}

bool NodeEngine::stopQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("NodeEngine:stopQuery for qep" << qep)
    if (queryStatusMap.find(qep) != queryStatusMap.end()) {
        if (dispatcher->stopQuery(qep)) {
            NES_DEBUG("NodeEngine: stop of QEP " << qep << " succeeded")
            queryStatusMap[qep] = NodeEngineQueryStatus::stopped;
            return true;
        } else {
            NES_DEBUG("NodeEngine: stop of QEP " << qep << " failed")
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep does not exists. stop failed" << qep)
        return false;
    }
}

DispatcherPtr NodeEngine::getDispatcher() {
    return dispatcher;
}



bool NodeEngine::start() {
    NES_DEBUG("NodeEngine:start reset dispatcher")
    dispatcher->resetDispatcher();

    NES_DEBUG("NodeEngine: start thread pool")
    bool successTp = startDispatcher();
    NES_DEBUG("NodeEngine: start thread pool success=" << successTp)

    NES_DEBUG("NodeEngine: start buffer manager")
    bool successBm = startBufferManager();
    NES_DEBUG("NodeEngine: start buffer manager success=" << successBm)

    return successTp && successBm;
}

bool NodeEngine::stop() {
    //TODO: add check if still queries are running
    if (!stoppedEngine) {
        NES_DEBUG("NodeEngine:stop stop NodeEngine, undeploy " << queryStatusMap.size() << " queries");
        for(auto entry : queryStatusMap)
        {
            if(entry.second == NodeEngineQueryStatus::started && !forceStop)
            {
                NES_ERROR("NodeEngine::stop: cannot stop as query " << entry.first << " still running")
                return false;
            }
        }

        std::vector<QueryExecutionPlanPtr> copyOfVec;
        std::transform(queryStatusMap.begin(), queryStatusMap.end(), std::back_inserter(copyOfVec),
                       [](std::pair<QueryExecutionPlanPtr, NodeEngineQueryStatus> const& dev) {
                         return dev.first;
                       });


        for (QueryExecutionPlanPtr qep : copyOfVec) {
            NES_DEBUG("QEP to del is =")
            qep->print();
            bool success = undeployQuery(qep);
            if (success) {
                NES_DEBUG("NodeEngine:stop undeployQuery query " << qep << " successfully")

            } else {
                NES_ERROR("NodeEngine:stop undeploy query " << qep << " failed")
                return false;
            }
        }

        NES_DEBUG("NodeEngine:stop undeploy successful")
        dispatcher->resetDispatcher();
        copyOfVec.clear();
        stoppedEngine = true;

        bool successTp = stopDispatcher();
        NES_DEBUG("NodeEngine:stop stop threadpool with success=" << successTp)

        bool successBm = stopBufferManager();
        NES_DEBUG("NodeEngine:stop stop buffer manager with success=" << successBm)

        return successTp && successBm;
    } else {
        NES_WARNING("NodeEngine::stop: engine already stopped")
        return true;
    }
}

BufferManagerPtr NodeEngine::getBufferManager()
{
    return bufferManager;
}

bool NodeEngine::startBufferManager()
{
    if(bufferManager)
    {
        NES_ERROR("NodeEngine::startBufferManager: buffer manager already exists")
        throw Exception("Error while start buffer manager");
    }
    NES_DEBUG("startBufferManager: setup buffer manager")
    bufferManager = std::make_shared<BufferManager>(DEFAULT_BUFFER_SIZE, DEFAULT_NUM_BUFFERS);
    return bufferManager->isReady();
}

bool NodeEngine::startBufferManager(size_t bufferSize, size_t numBuffers)
{
    if(bufferManager)
    {
        NES_ERROR("NodeEngine::startBufferManager: buffer manager already exists")
        throw Exception("Error while start buffer manager");
    }

    NES_DEBUG("startBufferManager: setup buffer manager")
    bufferManager = std::make_shared<BufferManager>(bufferSize, numBuffers);
    return bufferManager->isReady();
}

bool NodeEngine::stopBufferManager()
{
    if(!bufferManager)
    {
        NES_ERROR("NodeEngine::stopBufferManager buffer manager does not exists")
        throw Exception("Error while stop buffer manager");
    }
    NES_DEBUG("Dispatcher::stopBufferManager: stop")
    delete bufferManager.get();
    return true;
}

bool NodeEngine::startDispatcher()
{
    NES_DEBUG("startDispatcher: setup buffer manager")
    if(dispatcher)
    {
        NES_ERROR("NodeEngine::startDispatcher: dispatcher already exists")
        throw Exception("Error while start dispatcher");
    }
    NES_DEBUG("startDispatcher: setup dispatcher")
    dispatcher = std::make_shared<Dispatcher>();
    return dispatcher->startThreadPool();
}

bool NodeEngine::stopDispatcher()
{
    if(!dispatcher)
    {
        NES_ERROR("NodeEngine::stopDispatcher dispatcher does not exists")
        throw Exception("Error while stop dispatcher");
    }
    NES_DEBUG("stopDispatcher: stop dispatcher")

    dispatcher->stopThreadPool();
    delete dispatcher.get();
    return true;
}




}

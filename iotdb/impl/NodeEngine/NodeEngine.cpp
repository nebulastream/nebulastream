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

bool NodeEngine::deployQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("NodeEngine: deploy query " << qep)
    if (qeps.find(qep) == qeps.end()) {
        if (Dispatcher::instance().registerQuery(qep)) {
            qeps.insert(qep);
            NES_DEBUG("NodeEngine: deployment of QEP " << qep << " succeeded")
            return true;
        } else {
            NES_DEBUG("NodeEngine: deployment of QEP " << qep << " failed")
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep already exists. Deployment failed" << qep)
        return false;
    }
}

bool NodeEngine::startQuery(QueryExecutionPlanPtr qep)
{
    NES_DEBUG("NodeEngine: startQuery " << qep)
    if (qeps.find(qep) != qeps.end()) {
        if (Dispatcher::instance().startQuery(qep)) {
            NES_DEBUG("NodeEngine: start of QEP " << qep << " succeeded")
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
    NES_DEBUG("NodeEngine: undeployQuery query" << qep)
    if (qeps.find(qep) != qeps.end()) {
        if(Dispatcher::instance().deregisterQuery(qep))
        {
            qeps.erase(qep);
            NES_DEBUG("NodeEngine: undeploy of QEP " << qep << " succeeded")
        } else {
            NES_ERROR("NodeEngine: undeploy of QEP " << qep << " failed")
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep does not exists. undeployQuery failed" << qep)
        return false;
    }
}


bool NodeEngine::stopQuery(QueryExecutionPlanPtr qep)
{
    NES_DEBUG("NodeEngine: stopQuery " << qep)
    if (qeps.find(qep) != qeps.end()) {
        if (Dispatcher::instance().stopQuery(qep)) {
            NES_DEBUG("NodeEngine: stop of QEP " << qep << " succeeded")
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


void NodeEngine::init() {
    NES_DEBUG("NodeEngine: init node engine")
    // TODO remove singleton and reconfigure
    NES::Dispatcher::instance();
    if (!NES::BufferManager::instance().isReady()) {
        NES::BufferManager::instance().configure(DEFAULT_BUFFER_SIZE, DEFAULT_NUM_BUFFERS);
    }
    NES::ThreadPool::instance();
}

bool NodeEngine::start() {
    NES_DEBUG("NodeEngine: start thread pool")
    NES::Dispatcher::instance().resetDispatcher();
    return ThreadPool::instance().start();
}

bool NodeEngine::startWithRedeploy() {
    for (QueryExecutionPlanPtr qep : qeps) {
        NES_DEBUG("NodeEngine: register query " << qep)
        if (Dispatcher::instance().registerQuery(qep)) {
            NES_DEBUG("NodeEngine: registration of QEP " << qep << " succeeded")
        } else {
            NES_DEBUG("NodeEngine: registration of QEP " << qep << " failed")
        }
    }

    NES_DEBUG("NodeEngine: start thread pool")
    return ThreadPool::instance().start();
}

bool NodeEngine::stop() {
    NES_DEBUG("NodeEngine: stop thread pool");
    for (QueryExecutionPlanPtr qep : qeps) {
        NES_DEBUG("NodeEngine: deregister query " << qep)
        Dispatcher::instance().deregisterQuery(qep);
    }
    NES::Dispatcher::instance().resetDispatcher();
    return ThreadPool::instance().stop();
}

bool NodeEngine::stopWithUndeploy() {
    NES_DEBUG("NodeEngine: stop thread pool")
    for (QueryExecutionPlanPtr qep : qeps) {
        NES_DEBUG("NodeEngine: deregister query " << qep)
        Dispatcher::instance().deregisterQuery(qep);
    }
    NES::Dispatcher::instance().resetDispatcher();
    return ThreadPool::instance().stop();
}

void NodeEngine::applyConfig(Config &conf) {
    if (conf.getNumberOfWorker() != ThreadPool::instance().getNumberOfThreads()) {
        NES_DEBUG(
                "NodeEngine: changing numberOfWorker from " << ThreadPool::instance().getNumberOfThreads() << " to "
                                                            << conf.getNumberOfWorker())
        ThreadPool::instance().setNumberOfThreadsWithRestart(conf.getNumberOfWorker());
    }
    BufferManager::instance().configure(conf.getBufferSizeInByte(), conf.getBufferCount());
    NES_DEBUG("NodeEngine: config successuflly changed")
}

void NodeEngine::resetQEPs() {
    for (QueryExecutionPlanPtr qep : qeps) {
        NES_DEBUG("NodeEngine: deregister query " << qep)
        Dispatcher::instance().deregisterQuery(qep);
    }
    NES_DEBUG("NodeEngine: clear qeps")
    qeps.clear();
}

void NodeEngine::setDOPWithRestart(size_t dop) {
    NES::ThreadPool::instance().setNumberOfThreadsWithRestart(dop);
}
void NodeEngine::setDOPWithoutRestart(size_t dop) {
    NES::ThreadPool::instance().setNumberOfThreadsWithoutRestart(dop);
}

size_t NodeEngine::getDOP() {
    return NES::ThreadPool::instance().getNumberOfThreads();
}

}

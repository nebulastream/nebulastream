#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <string>
#include <Util/Logger.hpp>
//#include <tests/testPlans/compiledTestPlan.hpp>

using namespace std;
namespace NES {
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
      if (Dispatcher::instance().registerQueryWithStart(qep)) {
          qeps.insert(qep);
          NES_DEBUG("NodeEngine: deployment of QEP " << qep << " succeeded!")
          return true;
      }
      else {
          NES_DEBUG("NodeEngine: deployment of QEP " << qep << " failed!")
          return false;
      }
  }
  else {
    NES_DEBUG("NodeEngine: qep already exists. Deployment failed!" << qep)
    return false;
  }
}

bool NodeEngine::deployQueryWithoutStart(QueryExecutionPlanPtr qep) {
    NES_DEBUG("NodeEngine: deploy query " << qep)
    if (qeps.find(qep) == qeps.end()) {
        if (Dispatcher::instance().registerQueryWithoutStart(qep)) {
            qeps.insert(qep);
            return true;
        }
        else {
            return false;
        }
    }
    else {
        NES_DEBUG("NodeEngine: qep already exists. Deployment failed!" << qep)
        return false;
    }
}

bool NodeEngine::undeployQuery(QueryExecutionPlanPtr qep) {
  NES_DEBUG("NodeEngine: deregister query" << qep)
  if (qeps.find(qep) != qeps.end()) {
    qeps.erase(qep);
    return Dispatcher::instance().deregisterQuery(qep);
  }
  else {
    NES_DEBUG("NodeEngine: qep already exists. Deregister failed!" << qep)
    return false;
  }
}

void NodeEngine::init() {
  NES_DEBUG("NodeEngine: init node engine")

  NES::Dispatcher::instance();
  NES::BufferManager::instance();
  NES::ThreadPool::instance();
}

void NodeEngine::start() {
  NES_DEBUG("NodeEngine: start thread pool")
  ThreadPool::instance().start();
}

void NodeEngine::startWithRedeploy() {
  for (QueryExecutionPlanPtr qep : qeps) {
    NES_DEBUG("NodeEngine: register query " << qep)
    bool succeed = Dispatcher::instance().registerQueryWithStart(qep);
    if (succeed){
        NES_DEBUG("NodeEngine: registration of QEP " << qep << " succeeded!")
    }
    else {
        NES_DEBUG("NodeEngine: registration of QEP " << qep << " failed!")
    }
  }

  NES_DEBUG("NodeEngine: start thread pool")
  ThreadPool::instance().start();
}

void NodeEngine::stop() {
  NES_DEBUG("NodeEngine: stop thread pool")
  ThreadPool::instance().stop();
}

void NodeEngine::stopWithUndeploy() {
  NES_DEBUG("NodeEngine: stop thread pool")
  ThreadPool::instance().stop();

  for (QueryExecutionPlanPtr qep : qeps) {
    NES_DEBUG("NodeEngine: deregister query " << qep)
    Dispatcher::instance().deregisterQuery(qep);
  }
}

void NodeEngine::applyConfig(Config& conf) {
  if (conf.getNumberOfWorker() != ThreadPool::instance().getNumberOfThreads()) {
    NES_DEBUG(
        "NodeEngine: changing numberOfWorker from " << ThreadPool::instance().getNumberOfThreads() << " to " << conf.getNumberOfWorker())
    ThreadPool::instance().setNumberOfThreadsWithRestart(conf.getNumberOfWorker());
  }
  if (conf.getBufferCount() != BufferManager::instance().getNumberOfBuffers()) {
    NES_DEBUG(
        "NodeEngine: changing bufferCount from " << BufferManager::instance().getNumberOfBuffers() << " to " << conf.getBufferCount())
    BufferManager::instance().setNumberOfBuffers(conf.getBufferCount());
  }
  if (conf.getBufferSizeInByte() != BufferManager::instance().getBufferSize()) {
    NES_DEBUG(
        "NodeEngine: changing buffer size from " << BufferManager::instance().getBufferSize() << " to " << conf.getBufferSizeInByte())
    BufferManager::instance().setBufferSize(conf.getBufferSizeInByte());
  }
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

void NodeEngine::setDOPWithRestart(size_t dop)
{
  NES::ThreadPool::instance().setNumberOfThreadsWithRestart(dop);
}
void NodeEngine::setDOPWithoutRestart(size_t dop)
{
  NES::ThreadPool::instance().setNumberOfThreadsWithoutRestart(dop);
}

size_t NodeEngine::getDOP()
{
return NES::ThreadPool::instance().getNumberOfThreads();
}

}

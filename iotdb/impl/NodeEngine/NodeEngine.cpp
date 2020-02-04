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

void NodeEngine::deployQuery(QueryExecutionPlanPtr qep) {
  NES_DEBUG("NODEENGINE: deploy query " << qep)
  if (qeps.find(qep) == qeps.end()) {
    qeps.insert(qep);
    Dispatcher::instance().registerQueryWithStart(qep);
  }
  else {
    NES_DEBUG("NODEENGINE: qep already exists. Deployment failed!" << qep)
  }
}

void NodeEngine::deployQueryWithoutStart(QueryExecutionPlanPtr qep) {
  NES_DEBUG("NODEENGINE: deploy query " << qep)
  if (qeps.find(qep) == qeps.end()) {
    qeps.insert(qep);
    Dispatcher::instance().registerQueryWithoutStart(qep);
  }
  else {
    NES_DEBUG("NODEENGINE: qep already exists. Deployment failed!" << qep)
  }
}

void NodeEngine::undeployQuery(QueryExecutionPlanPtr qep) {
  NES_DEBUG("NODEENGINE: deregister query" << qep)
  if (qeps.find(qep) != qeps.end()) {
    qeps.erase(qep);
    Dispatcher::instance().deregisterQuery(qep);
  }
  else {
    NES_DEBUG("NODEENGINE: qep already exists. Deregister failed!" << qep)
  }
}

void NodeEngine::init() {
  NES_DEBUG("NODEENGINE: init node engine")

  NES::Dispatcher::instance();
  NES::BufferManager::instance();
  NES::ThreadPool::instance();
}

void NodeEngine::start() {
  NES_DEBUG("NODEENGINE: start thread pool")
  ThreadPool::instance().start();
}

void NodeEngine::startWithRedeploy() {
  for (QueryExecutionPlanPtr qep : qeps) {
    NES_DEBUG("NODEENGINE: register query " << qep)
    Dispatcher::instance().registerQueryWithStart(qep);
  }

  NES_DEBUG("NODEENGINE: start thread pool")
  ThreadPool::instance().start();
}

void NodeEngine::stop() {
  NES_DEBUG("NODEENGINE: stop thread pool")
  ThreadPool::instance().stop();
}

void NodeEngine::stopWithUndeploy() {
  NES_DEBUG("NODEENGINE: stop thread pool")
  ThreadPool::instance().stop();

  for (QueryExecutionPlanPtr qep : qeps) {
    NES_DEBUG("NODEENGINE: deregister query " << qep)
    Dispatcher::instance().deregisterQuery(qep);
  }
}

void NodeEngine::applyConfig(Config& conf) {
  if (conf.getNumberOfWorker() != ThreadPool::instance().getNumberOfThreads()) {
    NES_DEBUG(
        "NODEENGINE: changing numberOfWorker from " << ThreadPool::instance().getNumberOfThreads() << " to " << conf.getNumberOfWorker())
    ThreadPool::instance().setNumberOfThreadsWithRestart(conf.getNumberOfWorker());
  }
  if (conf.getBufferCount() != BufferManager::instance().getNumberOfBuffers()) {
    NES_DEBUG(
        "NODEENGINE: changing bufferCount from " << BufferManager::instance().getNumberOfBuffers() << " to " << conf.getBufferCount())
    BufferManager::instance().setNumberOfBuffers(conf.getBufferCount());
  }
  if (conf.getBufferSizeInByte() != BufferManager::instance().getBufferSize()) {
    NES_DEBUG(
        "NODEENGINE: changing buffer size from " << BufferManager::instance().getBufferSize() << " to " << conf.getBufferSizeInByte())
    BufferManager::instance().setBufferSize(conf.getBufferSizeInByte());
  }
  NES_DEBUG("NODEENGINE: config successuflly changed")
}

void NodeEngine::resetQEPs() {
  for (QueryExecutionPlanPtr qep : qeps) {
    NES_DEBUG("NODEENGINE: deregister query " << qep)
    Dispatcher::instance().deregisterQuery(qep);
  }
  NES_DEBUG("NODEENGINE: clear qeps")
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

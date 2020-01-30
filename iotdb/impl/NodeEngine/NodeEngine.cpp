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
  NES_DEBUG("NODEENGINE: deploy query" << qep)

  Dispatcher::instance().registerQueryWithStart(qep);
  qeps.insert({qep->getQueryId(), qep});
}

void NodeEngine::deployQueryWithoutStart(QueryExecutionPlanPtr qep) {
  NES_DEBUG("NODEENGINE: deploy query" << qep)

  Dispatcher::instance().registerQueryWithoutStart(qep);
  qeps.insert({qep->getQueryId(), qep});
}

void NodeEngine::undeployQuery(const std::string& queryId) {
  NES_DEBUG("NODEENGINE: deregister query" << queryId)
  Dispatcher::instance().deregisterQuery(queryId);
  qeps.erase(queryId);
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
  for (std::pair<std::string, QueryExecutionPlanPtr> e : qeps) {
    NES_DEBUG("NODEENGINE: register query " << e.first)
    Dispatcher::instance().registerQueryWithStart(e.second);
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

  for (std::pair<std::string, QueryExecutionPlanPtr> e : qeps) {
    NES_DEBUG("NODEENGINE: deregister query " << e.first)
    Dispatcher::instance().deregisterQuery(e.first);
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
  for (std::pair<std::string, QueryExecutionPlanPtr> e : qeps) {
    NES_DEBUG("NODEENGINE: deregister query " << e.first)
    Dispatcher::instance().deregisterQuery(e.first);
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

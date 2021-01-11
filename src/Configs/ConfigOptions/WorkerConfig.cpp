

#include <Configs/ConfigOptions/WorkerConfig.hpp>

namespace NES{
    WorkerConfig::WorkerConfig(NES::ConfigOption<std::string> localWorkerIp, NES::ConfigOption<std::string> coordinatorIp,
                                    NES::ConfigOption<uint16_t> coordinatorPort, NES::ConfigOption<uint16_t> rpcPort,
                                    NES::ConfigOption<uint16_t> dataPort, NES::ConfigOption<uint16_t> numberOfSlots,
                                    NES::ConfigOption<uint16_t> numWorkerThreads, NES::ConfigOption<std::string> parentId,
                                    NES::ConfigOption<std::string> logLevel)
        : localWorkerIp(localWorkerIp), coordinatorIp(coordinatorIp), coordinatorPort(coordinatorPort), rpcPort(rpcPort),
          dataPort(dataPort), numberOfSlots(numberOfSlots), numWorkerThreads(numWorkerThreads), parentId(parentId),
          logLevel(logLevel) {}

    //todo add sanaty checks to setter
    const ConfigOption<std::string>& NES::WorkerConfig::getLocalWorkerIp() const { return localWorkerIp; }
    void WorkerConfig::setLocalWorkerIp(const NES::ConfigOption<std::string>& localWorkerIp) {
        WorkerConfig::localWorkerIp = localWorkerIp;
    }
    const ConfigOption<std::string>& NES::WorkerConfig::getCoordinatorIp() const { return coordinatorIp; }
    void WorkerConfig::setCoordinatorIp(const NES::ConfigOption<std::string>& coordinatorIp) {
        WorkerConfig::coordinatorIp = coordinatorIp;
    }
    const ConfigOption<uint16_t>& NES::WorkerConfig::getCoordinatorPort() const { return coordinatorPort; }
    void WorkerConfig::setCoordinatorPort(const NES::ConfigOption<uint16_t>& coordinatorPort) {
        WorkerConfig::coordinatorPort = coordinatorPort;
    }
    const ConfigOption<uint16_t>& NES::WorkerConfig::getRpcPort() const { return rpcPort; }
    void WorkerConfig::setRpcPort(const NES::ConfigOption<uint16_t>& rpcPort) { WorkerConfig::rpcPort = rpcPort; }
    const ConfigOption<uint16_t>& NES::WorkerConfig::getDataPort() const { return dataPort; }
    void WorkerConfig::setDataPort(const NES::ConfigOption<uint16_t>& dataPort) { WorkerConfig::dataPort = dataPort; }
    const ConfigOption<uint16_t>& NES::WorkerConfig::getNumberOfSlots() const { return numberOfSlots; }
    void WorkerConfig::setNumberOfSlots(const NES::ConfigOption<uint16_t>& numberOfSlots) {
        WorkerConfig::numberOfSlots = numberOfSlots;
    }
    const ConfigOption<uint16_t>& NES::WorkerConfig::getNumWorkerThreads() const { return numWorkerThreads; }
    void WorkerConfig::setNumWorkerThreads(const NES::ConfigOption<uint16_t>& numWorkerThreads) {
        WorkerConfig::numWorkerThreads = numWorkerThreads;
    }
    const ConfigOption<std::string>& NES::WorkerConfig::getParentId() const { return parentId; }
    void WorkerConfig::setParentId(const NES::ConfigOption<std::string>& parentId) { WorkerConfig::parentId = parentId; }
    const ConfigOption<std::string>& NES::WorkerConfig::getLogLevel() const { return logLevel; }
    void WorkerConfig::setLogLevel(const NES::ConfigOption<std::string>& logLevel) { WorkerConfig::logLevel = logLevel; }
    WorkerConfig::WorkerConfig() {}
    }
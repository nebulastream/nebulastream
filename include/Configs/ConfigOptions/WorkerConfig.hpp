//
// Created by eleicha on 06.01.21.
//

#ifndef NES_WORKERCONFIG_HPP
#define NES_WORKERCONFIG_HPP

#include <Configs/ConfigOption.hpp>
#include <string>
#include <thread>

using namespace std;

namespace NES {

class WorkerConfig {

  public:
    WorkerConfig();
    WorkerConfig(string filePath);
    WorkerConfig(ConfigOption<std::string> localWorkerIp, ConfigOption<std::string> coordinatorIp,
                 ConfigOption<uint16_t> coordinatorPort, ConfigOption<uint16_t> rpcPort, ConfigOption<uint16_t> dataPort,
                 ConfigOption<uint16_t> numberOfSlots, ConfigOption<uint16_t> numWorkerThreads,
                 ConfigOption<std::string> parentId, ConfigOption<std::string> logLevel);

    const ConfigOption<std::string>& getLocalWorkerIp() const;
    void setLocalWorkerIp(const ConfigOption<std::string>& localWorkerIp);
    const ConfigOption<std::string>& getCoordinatorIp() const;
    void setCoordinatorIp(const ConfigOption<std::string>& coordinatorIp);
    const ConfigOption<uint16_t>& getCoordinatorPort() const;
    void setCoordinatorPort(const ConfigOption<uint16_t>& coordinatorPort);
    const ConfigOption<uint16_t>& getRpcPort() const;
    void setRpcPort(const ConfigOption<uint16_t>& rpcPort);
    const ConfigOption<uint16_t>& getDataPort() const;
    void setDataPort(const ConfigOption<uint16_t>& dataPort);
    const ConfigOption<uint16_t>& getNumberOfSlots() const;
    void setNumberOfSlots(const ConfigOption<uint16_t>& numberOfSlots);
    const ConfigOption<uint16_t>& getNumWorkerThreads() const;
    void setNumWorkerThreads(const ConfigOption<uint16_t>& numWorkerThreads);
    const ConfigOption<std::string>& getParentId() const;
    void setParentId(const ConfigOption<std::string>& parentId);
    const ConfigOption<std::string>& getLogLevel() const;
    void setLogLevel(const ConfigOption<std::string>& logLevel);
    const string& getFilePath() const;
    void setFilePath(const string& filePath);

  private:
    ConfigOption<std::string> localWorkerIp =
        ConfigOption("localWorkerIp", std::string("127.0.0.1"), "Worker IP.", "string", false);
    ConfigOption<std::string> coordinatorIp =
        ConfigOption("coordinatorIp", std::string("127.0.0.1"),
                     "Server IP of the NES Coordinator to which the NES Worker should connect.", "string", false);
    ConfigOption<uint16_t> coordinatorPort =
        ConfigOption("coordinatorPort", uint16_t(0),
                     "RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs "
                     "to be the same as restPort in Coordinator.",
                     "uint16_t", false);
    ConfigOption<uint16_t> rpcPort =
        ConfigOption("rpcPort", uint16_t(8081), "RPC server port of the NES Worker.", "uint16_t", false);
    ConfigOption<uint16_t> dataPort = ConfigOption("dataPort", uint16_t(3001), "Data port of the NES Worker.", "uint16_t", false);
    ConfigOption<uint16_t> numberOfSlots = ConfigOption("numberOfSlots", uint16_t(std::thread::hardware_concurrency()),
                                                        "Number of computing slots for the NES Worker.", "uint16_t", false);
    ConfigOption<uint16_t> numWorkerThreads =
        ConfigOption("numWorkerThreads", uint16_t(1), "Number of worker threads.", "uint16_t", false);
    ConfigOption<std::string> parentId = ConfigOption("parentId", std::string("-1"), "Parent ID of this node.", "string", false);
    ConfigOption<std::string> logLevel =
        ConfigOption("logLevel", std::string("LOG_DEBUG"), "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ",
                     "string", false);
    string filePath;
};

}// namespace NES

#endif//NES_WORKERCONFIG_HPP

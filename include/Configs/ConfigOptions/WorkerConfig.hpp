//
// Created by eleicha on 06.01.21.
//

#ifndef NES_WORKERCONFIG_HPP
#define NES_WORKERCONFIG_HPP

#include <Configs/ConfigOption.hpp>
#include <string>

namespace NES {

class WorkerConfig {

  public:
    CoordinatorConfig(ConfigOption<std::string> localWorkerIp, ConfigOption<std::string> coordinatorIp,
                      ConfigOption<uint16_t> coordinatorPort, ConfigOption<uint16_t> rpcPort, ConfigOption<uint16_t> dataPort,
                      ConfigOption<uint16_t> numberOfSlots, ConfigOption<uint16_t> numWorkerThreads,
                      ConfigOption<std::string> parentId, ConfigOption<bool> enableQueryMerging,
                      ConfigOption<std::string> logLevel);

    ConfigOption<std::string> localWorkerIp;
    ConfigOption<std::string> coordinatorIp;
    ConfigOption<uint16_t> coordinatorPort;
    ConfigOption<uint16_t> rpcPort;
    ConfigOption<uint16_t> dataPort;
    ConfigOption<uint16_t> numberOfSlots;
    ConfigOption<uint16_t> numWorkerThreads;
    ConfigOption<std::string> parentId;
    ConfigOption<bool> enableQueryMerging;
    ConfigOption<std::string> logLevel;
};

}// namespace NES

#endif//NES_WORKERCONFIG_HPP

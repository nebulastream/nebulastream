#ifndef NES_COORDINATORCONFIG_HPP
#define NES_COORDINATORCONFIG_HPP

#include <Configs/ConfigOption.hpp>
#include <map>
#include <string>
#include <thread>

using namespace std;

namespace NES {

class CoordinatorConfig {

  public:
    CoordinatorConfig();

    void overwriteConfigWithYAMLFileInput(string filePath);
    void overwriteConfigWithCommandLineInput(map<string,string> inputParams);
    void resetCoordinatorOptions();

    const ConfigOption<std::string>& getRestIp() const;
    void setRestIp(const string& restIp);
    const ConfigOption<std::string>& getCoordinatorIp() const;
    void setCoordinatorIp(const string& coordinatorIp);
    const ConfigOption<uint16_t>& getRpcPort() const;
    void setRpcPort(const uint16_t& rpcPort);
    const ConfigOption<uint16_t>& getRestPort() const;
    void setRestPort(const uint16_t& restPort);
    const ConfigOption<uint16_t>& getDataPort() const;
    void setDataPort(const uint16_t& dataPort);
    const ConfigOption<uint16_t>& getNumberOfSlots() const;
    void setNumberOfSlots(const uint16_t& numberOfSlots);
    const ConfigOption<bool>& getEnableQueryMerging() const;
    void setEnableQueryMerging(const bool& enableQueryMerging);
    const ConfigOption<std::string>& getLogLevel() const;
    void setLogLevel(const string& logLevel);

  private:
    ConfigOption<std::string> restIp =
        ConfigOption("restIp", std::string("127.0.0.1"), "NES ip of the REST server.", "string", false);
    ConfigOption<std::string> coordinatorIp =
        ConfigOption("coordinatorIp", std::string("127.0.0.1"), "RPC IP address of NES Coordinator.", "string", false);
    ConfigOption<uint16_t> rpcPort =
        ConfigOption("rpcPort", uint16_t(4000), "RPC server port of the NES Coordinator", "uint16_t", false);
    ConfigOption<uint16_t> restPort =
        ConfigOption("restPort", uint16_t(8081), "Port exposed for rest endpoints", "uint16_t", false);
    ConfigOption<uint16_t> dataPort = ConfigOption("dataPort", uint16_t(3001), "NES data server port", "uint16_t", false);
    ConfigOption<uint16_t> numberOfSlots = ConfigOption("numberOfSlots", uint16_t(std::thread::hardware_concurrency()),
                                                        "Number of computing slots for NES Coordinator", "uint16_t", false);
    ConfigOption<bool> enableQueryMerging =
        ConfigOption("enableQueryMerging", false, "Enable Query Merging Feature", "bool", false);
    ConfigOption<std::string> logLevel =
        ConfigOption("logLevel", std::string("LOG_DEBUG"),
                     "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)", "string", false);
};

}// namespace NES

#endif//NES_COORDINATORCONFIG_HPP

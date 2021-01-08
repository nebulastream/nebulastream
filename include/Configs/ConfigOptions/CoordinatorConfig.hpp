#ifndef NES_COORDINATORCONFIG_HPP
#define NES_COORDINATORCONFIG_HPP

#include <Configs/ConfigOption.hpp>
#include <string>
#include <thread>

namespace NES {

class CoordinatorConfig {

  public:
    CoordinatorConfig();
    CoordinatorConfig(ConfigOption<std::string> restIp, ConfigOption<std::string> coordinatorIp, ConfigOption<uint16_t> restPort,
                      ConfigOption<uint16_t> rpcPort, ConfigOption<uint16_t> dataPort, ConfigOption<uint16_t> numberOfSlots,
                      ConfigOption<bool> enableQueryMerging, ConfigOption<std::string> logLevel);
    /**
 * @brief get rest ip
 * @return rest ip
 */
    const ConfigOption<std::string>& getRestIp() const;
    /**
     * set rest Ip
     * @param restIp
     */
    void setRestIp(const ConfigOption<std::string>& restIp);
    /**
     * @brief get coordinator ip
     * @return coordinator ip
     */
    const ConfigOption<std::string>& getCoordinatorIp() const;
    /**
     * set coordinator ip
     * @param coordinatorIp
     */
    void setCoordinatorIp(const ConfigOption<std::string>& coordinatorIp);
    /**
     * get rest port
     * @return rest port
     */
    const ConfigOption<uint16_t>& getRestPort() const;
    /**
     * set rest port
     * @param restPort
     */
    void setRestPort(const ConfigOption<uint16_t>& restPort);
    /**
     * get rpc port
     * @return rpc port
     */
    const ConfigOption<uint16_t>& getRpcPort() const;
    /**
     * set rpc port
     * @param rpcPort
     */
    void setRpcPort(const ConfigOption<uint16_t>& rpcPort);
    /**
     * get data port
     * @return data port
     */
    const ConfigOption<uint16_t>& getDataPort() const;
    /**
     * set data port
     * @param dataPort
     */
    void setDataPort(const ConfigOption<uint16_t>& dataPort);
    /**
     * get number of slots
     * @return number of slots
     */
    const ConfigOption<uint16_t>& getNumberOfSlots() const;
    /**
     * set number of slots
     * @param number Of Slots
     */
    void setNumberOfSlots(const ConfigOption<uint16_t>& numberOfSlots);
    /**
     * get value for query merging enabled or not
     * @return enableQueryMerging
     */
    const ConfigOption<bool>& getEnableQueryMerging() const;
    /**
     * set enable query merging
     * @param enableQueryMerging
     */
    void setEnableQueryMerging(const ConfigOption<bool>& enableQueryMerging);
    /**
     * get log level
     * @return logLevel
     */
    const ConfigOption<std::string>& getLogLevel() const;
    /**
     * set log level
     * @param logLevel
     */
    void setLogLevel(const ConfigOption<std::string>& logLevel);

  private:
    ConfigOption<std::string> restIp =
        ConfigOption("restIp", std::string("127.0.0.1"), "NES ip of the REST server.", "string", ConfigType::DEFAULT, false);
    ConfigOption<std::string> coordinatorIp = ConfigOption(
        "coordinatorIp", std::string("127.0.0.1"), "RPC IP address of NES Coordinator.", "string", ConfigType::DEFAULT, false);
    ConfigOption<uint16_t> rpcPort =
        ConfigOption("rpcPort", uint16_t(4000), "RPC server port of the NES Coordinator", "uint16_t", ConfigType::DEFAULT, false);
    ConfigOption<uint16_t> restPort =
        ConfigOption("restPort", uint16_t(8081), "Port exposed for rest endpoints", "uint16_t", ConfigType::DEFAULT, false);
    ConfigOption<uint16_t> dataPort =
        ConfigOption("dataPort", uint16_t(3001), "NES data server port", "uint16_t", ConfigType::DEFAULT, false);
    ConfigOption<uint16_t> numberOfSlots =
        ConfigOption("numberOfSlots", uint16_t(std::thread::hardware_concurrency()),
                     "Number of computing slots for NES Coordinator", "uint16_t", ConfigType::DEFAULT, false);
    ConfigOption<bool> enableQueryMerging =
        ConfigOption("enableQueryMerging", false, "Enable Query Merging Feature", "bool", ConfigType::DEFAULT, false);
    ConfigOption<std::string> logLevel = ConfigOption("logLevel", std::string("LOG_DEBUG"),
                                                      "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)",
                                                      "string", ConfigType::DEFAULT, false);
};

}// namespace NES

#endif//NES_COORDINATORCONFIG_HPP

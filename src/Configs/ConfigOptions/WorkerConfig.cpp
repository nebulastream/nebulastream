

#include <Configs/ConfigOption.hpp>
#include <Configs/ConfigOptions/WorkerConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <sys/stat.h>

namespace NES {

WorkerConfig::WorkerConfig() { NES_INFO("Generated new Worker Config object. Configurations initialized with default values."); }

/*void WorkerConfig::overwriteConfigWithYAMLFileInput(string filePath, Yaml::Node config) {

    struct stat buffer {};
    if (!filePath.empty() && !(stat(filePath.c_str(), &buffer) == -1)) {

        NES_INFO("NesWorkerConfig: Using config file with path: " << filePath << " .");

        Yaml::Parse(config, filePath.c_str());

        setCoordinatorPort(config["coordinatorPort"].As<uint16_t>());
        setRpcPort(config["rpcPort"].As<uint16_t>());
        setDataPort(config["dataPort"].As<uint16_t>());
        setLocalWorkerIp(config["localWorkerIp"].As<string>());
        setCoordinatorIp(config["coordinatorIp"].As<string>());
        setNumberOfSlots(config["numberOfSlots"].As<uint16_t>());
        setNumWorkerThreads(config["numWorkerThreads"].As<bool>());
        setParentId(config["parentId"].As<string>());
        setLogLevel(config["logLevel"].As<string>());
    } else {
        NES_ERROR("NesWorkerConfig: No file path was provided or file could not be found at " << filePath << ".");
        NES_INFO("Keeping default values for Coordinator Config.");
    }
}*/
void WorkerConfig::overwriteConfigWithCommandLineInput(map<string,string> inputParams) {
    try {

        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            NES_INFO("NesWorkerConfig: Using command line input parameter " << it->first << " with value " << it->second);
            if (it->first == "--localWorkerIp") {
                setLocalWorkerIp(it->second);
            } else if (it->first == "--coordinatorIp") {
                setCoordinatorIp(it->second);
            } else if (it->first == "--rpcPort") {
                setRpcPort(stoi(it->second));
            } else if (it->first == "--coordinatorPort") {
                setCoordinatorPort(stoi(it->second));
            } else if (it->first == "--dataPort") {
                setDataPort(stoi(it->second));
            } else if (it->first == "--numberOfSlots") {
                setNumberOfSlots(stoi(it->second));
            } else if (it->first == "--numWorkerThreads") {
                setNumWorkerThreads(stoi(it->second));
            } else if (it->first == "--parentId") {
                setParentId(it->second);
            } else if (it->first == "--logLevel") {
                setLogLevel(it->second);
            }
        }
    } catch (exception e) {
        NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from command line. Keeping default "
                  "values.");
        resetWorkerOptions();
    }
}

void WorkerConfig::resetWorkerOptions() {
    setLocalWorkerIp(localWorkerIp.getDefaultValue());
    setCoordinatorIp(coordinatorIp.getDefaultValue());
    setCoordinatorPort(coordinatorPort.getDefaultValue());
    setRpcPort(rpcPort.getDefaultValue());
    setDataPort(dataPort.getDefaultValue());
    setNumberOfSlots(numberOfSlots.getDefaultValue());
    setNumWorkerThreads(numWorkerThreads.getDefaultValue());
    setParentId(parentId.getDefaultValue());
    setLogLevel(logLevel.getDefaultValue());
}

const ConfigOption<std::string>& WorkerConfig::getLocalWorkerIp() const { return localWorkerIp; }
void WorkerConfig::setLocalWorkerIp(const string& localWorkerIp) { WorkerConfig::localWorkerIp.setValue(localWorkerIp); }
const ConfigOption<std::string>& WorkerConfig::getCoordinatorIp() const { return coordinatorIp; }
void WorkerConfig::setCoordinatorIp(const string& coordinatorIp) { WorkerConfig::coordinatorIp.setValue(coordinatorIp); }
const ConfigOption<uint16_t>& WorkerConfig::getCoordinatorPort() const { return coordinatorPort; }
void WorkerConfig::setCoordinatorPort(const uint16_t& coordinatorPort) {
    WorkerConfig::coordinatorPort.setValue(coordinatorPort);
}
const ConfigOption<uint16_t>& WorkerConfig::getRpcPort() const { return rpcPort; }
void WorkerConfig::setRpcPort(const uint16_t& rpcPort) { WorkerConfig::rpcPort.setValue(rpcPort); }
const ConfigOption<uint16_t>& WorkerConfig::getDataPort() const { return dataPort; }
void WorkerConfig::setDataPort(const uint16_t& dataPort) { WorkerConfig::dataPort.setValue(dataPort); }
const ConfigOption<uint16_t>& WorkerConfig::getNumberOfSlots() const { return numberOfSlots; }
void WorkerConfig::setNumberOfSlots(const uint16_t& numberOfSlots) { WorkerConfig::numberOfSlots.setValue(numberOfSlots); }
const ConfigOption<uint16_t>& WorkerConfig::getNumWorkerThreads() const { return numWorkerThreads; }
void WorkerConfig::setNumWorkerThreads(const uint16_t& numWorkerThreads) { WorkerConfig::numWorkerThreads.setValue(numWorkerThreads); }
const ConfigOption<std::string>& WorkerConfig::getParentId() const { return parentId; }
void WorkerConfig::setParentId(const string& parentId) { WorkerConfig::parentId.setValue(parentId); }
const ConfigOption<std::string>& WorkerConfig::getLogLevel() const { return logLevel; }
void WorkerConfig::setLogLevel(const string& logLevel) { WorkerConfig::logLevel.setValue(logLevel); }
}// namespace NES
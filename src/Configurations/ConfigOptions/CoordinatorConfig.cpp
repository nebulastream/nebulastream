/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>

namespace NES {

CoordinatorConfigPtr CoordinatorConfig::create() { return std::make_shared<CoordinatorConfig>(CoordinatorConfig()); }

CoordinatorConfig::CoordinatorConfig() {
    NES_INFO("Generated new Coordinator Config object. Configurations initialized with default values.");
    restIp = ConfigOption<std::string>::create("restIp", "127.0.0.1", "NES ip of the REST server.");
    coordinatorIp = ConfigOption<std::string>::create("coordinatorIp", "127.0.0.1", "RPC IP address of NES Coordinator.");
    rpcPort = ConfigOption<uint32_t>::create("rpcPort", 4000, "RPC server port of the NES Coordinator");
    restPort = ConfigOption<uint32_t>::create("restPort", 8081, "Port exposed for rest endpoints");
    dataPort = ConfigOption<uint32_t>::create("dataPort", 3001, "NES data server port");
    numberOfSlots = ConfigOption<uint32_t>::create("numberOfSlots", UINT16_MAX, "Number of computing slots for NES Coordinator");
    enableQueryMerging = ConfigOption<bool>::create("enableQueryMerging", false, "Enable Query Merging Feature");
    logLevel = ConfigOption<std::string>::create("logLevel",
                                                 "LOG_DEBUG",
                                                 "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)");
    numberOfBuffersInGlobalBufferManager =
        ConfigOption<uint32_t>::create("numberOfBuffersInGlobalBufferManager", 1024, "Number buffers in global buffer pool.");
    numberOfBuffersPerPipeline =
        ConfigOption<uint32_t>::create("numberOfBuffersPerPipeline", 128, "Number buffers in task local buffer pool.");
    numberOfBuffersInSourceLocalBufferPool = ConfigOption<uint32_t>::create("numberOfBuffersInSourceLocalBufferPool",
                                                                            64,
                                                                            "Number buffers in source local buffer pool.");
    bufferSizeInBytes = ConfigOption<uint32_t>::create("bufferSizeInBytes", 4096, "BufferSizeInBytes.");
    numWorkerThreads = ConfigOption<uint32_t>::create("numWorkerThreads", 1, "Number of worker threads.");
    queryBatchSize = ConfigOption<uint32_t>::create("queryBatchSize", 1, "The number of queries to be processed together");

    queryMergerRule = ConfigOption<std::string>::create("queryMergerRule",
                                                        "SyntaxBasedCompleteQueryMergerRule",
                                                        "The rule to be used for performing query merging");

    enableSemanticQueryValidation =
        ConfigOption<bool>::create("enableSemanticQueryValidation", false, "Enable semantic query validation feature");

    persistenceType = ConfigOption<std::string>::create("persistenceType", "none", "Type of persistence (support: none, file");
    persistenceDir = ConfigOption<std::string>::create("persistenceDir", "/tmp/nes/data/", "Directory for Persistence");
}

void CoordinatorConfig::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("CoordinatorConfig: Using config file with path: " << filePath << " .");
        Yaml::Node config = *(new Yaml::Node());
        Yaml::Parse(config, filePath.c_str());
        try {
            setRestPort(config["restPort"].As<uint16_t>());
            setRpcPort(config["rpcPort"].As<uint16_t>());
            setDataPort(config["dataPort"].As<uint16_t>());
            setRestIp(config["restIp"].As<std::string>());
            setCoordinatorIp(config["coordinatorIp"].As<std::string>());
            setNumberOfSlots(config["numberOfSlots"].As<uint16_t>());
            setEnableQueryMerging(config["enableQueryMerging"].As<bool>());
            setLogLevel(config["logLevel"].As<std::string>());
            setQueryBatchSize(config["queryBatchSize"].As<uint32_t>());
            setNumberOfBuffersInGlobalBufferManager(config["numberOfBuffersInGlobalBufferManager"].As<uint32_t>());
            setnumberOfBuffersPerPipeline(config["numberOfBuffersPerPipeline"].As<uint32_t>());
            setNumberOfBuffersInSourceLocalBufferPool(config["numberOfBuffersInSourceLocalBufferPool"].As<uint32_t>());
            setQueryMergerRule(config["queryMergerRule"].As<std::string>());
            setEnableSemanticQueryValidation(config["enableSemanticQueryValidation"].As<bool>());
            setPersistenceType(config["persistenceType"].As<std::string>());
            setPersistenceDir(config["persistenceDir"].As<std::string>());
        } catch (std::exception& e) {
            NES_ERROR("CoordinatorConfig: Error while initializing configuration parameters from YAML file. " << e.what());
            NES_WARNING("CoordinatorConfig: Keeping default values.");
            resetCoordinatorOptions();
        }
        return;
    }
    NES_ERROR("CoordinatorConfig: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("CoordinatorConfig: Keeping default values for Coordinator Config.");
}

void CoordinatorConfig::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
    try {
        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            if (it->first == "--restIp") {
                setRestIp(it->second);
            } else if (it->first == "--coordinatorIp") {
                setCoordinatorIp(it->second);
            } else if (it->first == "--coordinatorPort") {
                setRpcPort(stoi(it->second));
            } else if (it->first == "--restPort") {
                setRestPort(stoi(it->second));
            } else if (it->first == "--dataPort") {
                setDataPort(stoi(it->second));
            } else if (it->first == "--numberOfSlots") {
                setNumberOfSlots(stoi(it->second));
            } else if (it->first == "--enableQueryMerging") {
                setEnableQueryMerging((it->second == "true"));
            } else if (it->first == "--logLevel") {
                setLogLevel(it->second);
            } else if (it->first == "--queryBatchSize") {
                setQueryBatchSize(stoi(it->second));
            } else if (it->first == "--numberOfBuffersInGlobalBufferManager") {
                setNumberOfBuffersInGlobalBufferManager(stoi(it->second));
            } else if (it->first == "--numberOfBuffersPerPipeline") {
                setnumberOfBuffersPerPipeline(stoi(it->second));
            } else if (it->first == "--numberOfBuffersInSourceLocalBufferPool") {
                setNumberOfBuffersInSourceLocalBufferPool(stoi(it->second));
            } else if (it->first == "--queryMergerRule") {
                setQueryMergerRule(it->second);
            } else if (it->first == "--enableSemanticQueryValidation") {
                setEnableSemanticQueryValidation((it->second == "true"));
            } else if (it->first == "--persistenceType") {
                setPersistenceType(it->second);
            } else if (it->first == "--persistenceDir") {
                setPersistenceDir(it->second);
            } else {
                NES_WARNING("Unknow configuration value :" << it->first);
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("CoordinatorConfig: Error while initializing configuration parameters from command line.");
        NES_WARNING("CoordinatorConfig: Keeping default values.");
        resetCoordinatorOptions();
    }
}

void CoordinatorConfig::resetCoordinatorOptions() {
    setRestPort(restPort->getDefaultValue());
    setRpcPort(rpcPort->getDefaultValue());
    setDataPort(dataPort->getDefaultValue());
    setRestIp(restIp->getDefaultValue());
    setCoordinatorIp(coordinatorIp->getDefaultValue());
    setNumberOfSlots(numberOfSlots->getDefaultValue());
    setEnableQueryMerging(enableQueryMerging->getDefaultValue());
    setLogLevel(logLevel->getDefaultValue());
    setQueryBatchSize(queryBatchSize->getDefaultValue());
    setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager->getDefaultValue());
    setnumberOfBuffersPerPipeline(numberOfBuffersPerPipeline->getDefaultValue());
    setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
    setQueryMergerRule(queryMergerRule->getDefaultValue());
    setEnableSemanticQueryValidation(enableSemanticQueryValidation->getDefaultValue());
    setPersistenceType(persistenceType->getDefaultValue());
    setPersistenceDir(persistenceDir->getDefaultValue());
}

StringConfigOption CoordinatorConfig::getRestIp() { return restIp; }

void CoordinatorConfig::setRestIp(std::string restIpValue) { restIp->setValue(restIpValue); }

StringConfigOption CoordinatorConfig::getCoordinatorIp() { return coordinatorIp; }

void CoordinatorConfig::setCoordinatorIp(std::string coordinatorIpValue) { coordinatorIp->setValue(coordinatorIpValue); }

IntConfigOption CoordinatorConfig::getRpcPort() { return rpcPort; }

void CoordinatorConfig::setRpcPort(uint16_t rpcPortValue) { rpcPort->setValue(rpcPortValue); }

IntConfigOption CoordinatorConfig::getRestPort() { return restPort; }

void CoordinatorConfig::setRestPort(uint16_t restPortValue) { restPort->setValue(restPortValue); }

IntConfigOption CoordinatorConfig::getDataPort() { return dataPort; }

void CoordinatorConfig::setDataPort(uint16_t dataPortValue) { dataPort->setValue(dataPortValue); }

IntConfigOption CoordinatorConfig::getNumberOfSlots() { return numberOfSlots; }

void CoordinatorConfig::setNumberOfSlots(uint16_t numberOfSlotsValue) { numberOfSlots->setValue(numberOfSlotsValue); }

void CoordinatorConfig::setNumWorkerThreads(uint16_t numWorkerThreadsValue) { numWorkerThreads->setValue(numWorkerThreadsValue); }

IntConfigOption CoordinatorConfig::getNumWorkerThreads() { return numWorkerThreads; }

BoolConfigOption CoordinatorConfig::getEnableQueryMerging() { return enableQueryMerging; }

void CoordinatorConfig::setEnableQueryMerging(bool enableQueryMergingValue) {
    CoordinatorConfig::enableQueryMerging->setValue(enableQueryMergingValue);
}

StringConfigOption CoordinatorConfig::getLogLevel() { return logLevel; }

void CoordinatorConfig::setLogLevel(std::string logLevelValue) { logLevel->setValue(logLevelValue); }

IntConfigOption CoordinatorConfig::getNumberOfBuffersInGlobalBufferManager() { return numberOfBuffersInGlobalBufferManager; }
IntConfigOption CoordinatorConfig::getnumberOfBuffersPerPipeline() { return numberOfBuffersPerPipeline; }
IntConfigOption CoordinatorConfig::getNumberOfBuffersInSourceLocalBufferPool() { return numberOfBuffersInSourceLocalBufferPool; }

void CoordinatorConfig::setNumberOfBuffersInGlobalBufferManager(uint64_t count) {
    numberOfBuffersInGlobalBufferManager->setValue(count);
}
void CoordinatorConfig::setnumberOfBuffersPerPipeline(uint64_t count) { numberOfBuffersPerPipeline->setValue(count); }
void CoordinatorConfig::setNumberOfBuffersInSourceLocalBufferPool(uint64_t count) {
    numberOfBuffersInSourceLocalBufferPool->setValue(count);
}

IntConfigOption CoordinatorConfig::getBufferSizeInBytes() { return bufferSizeInBytes; }

void CoordinatorConfig::setBufferSizeInBytes(uint64_t sizeInBytes) { bufferSizeInBytes->setValue(sizeInBytes); }

IntConfigOption CoordinatorConfig::getQueryBatchSize() { return queryBatchSize; }

void CoordinatorConfig::setQueryBatchSize(uint32_t batchSize) { queryBatchSize->setValue(batchSize); }

StringConfigOption CoordinatorConfig::getQueryMergerRule() { return queryMergerRule; }

void CoordinatorConfig::setQueryMergerRule(std::string queryMergerRuleValue) { queryMergerRule->setValue(queryMergerRuleValue); }

BoolConfigOption CoordinatorConfig::getEnableSemanticQueryValidation() { return enableSemanticQueryValidation; }

void CoordinatorConfig::setEnableSemanticQueryValidation(bool enableSemanticQueryValidation) {
    CoordinatorConfig::enableSemanticQueryValidation->setValue(enableSemanticQueryValidation);
}

void CoordinatorConfig::setPersistenceType(std::string type) { persistenceType->setValue(type); }

StringConfigOption CoordinatorConfig::getPersistenceType() { return persistenceType; }

void CoordinatorConfig::setPersistenceDir(std::string dir) { persistenceDir->setValue(dir); }

StringConfigOption CoordinatorConfig::getPersistenceDir() { return persistenceDir; }

}// namespace NES


#include <Configs/ConfigOptions/YAMLConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hh>
#include <Util/yaml/YamlDef.hh>
#include <string>

namespace NES{

template<typename Tp>
YAMLConfig<Tp>::YAMLConfig(std::map<std::string, Tp> configurations) : configurations(configurations) {}

template<typename Tp>
std::map<std::string, Tp> YAMLConfig<Tp>::loadConfig(std::string filePath, ConfigOptionType executableType) {

    NES_INFO("NESWORKERSTARTER: Using config file with path: " << filePath << " .");
    struct stat buffer {};
    if (stat(filePath.c_str(), &buffer) == -1) {
        NES_ERROR("NESWORKERSTARTER: Configuration file not found at: " << filePath << '\n');
        return EXIT_FAILURE;
    }
    Yaml::Node config;
    Yaml::Parse(config, filePath.c_str());

    switch (executableType) {
        case ConfigOptionType::COORDINATOR:
            configurations.template insert("restPort", config["restPort"].As<uint16_t>());
            configurations.template insert("rpcPort", config["rpcPort"].As<uint16_t>());
            configurations.template insert("dataPort", config["dataPort"].As<uint16_t>());
            configurations.template insert("restIp", config["restIp"].As<std::string>());
            configurations.template insert("restPort", config["restPort"].As<std::string>());
            configurations.template insert("numberOfSlots", config["numberOfSlots"].As<std::string>());
            configurations.template insert("enableQueryMerging", config["enableQueryMerging"].As<bool>());
            configurations.template insert("logLevel", config["logLevel"].As<std::string>());
            break;
        case ConfigOptionType::WORKER:
            configurations.template insert("rpcPort", config["rpcPort"].As<uint16_t>());
            configurations.template insert("coordinatorPort", config["coordinatorPort"].As<uint16_t>());
            configurations.template insert("coordinatorIp", config["coordinatorIp"].As<std::string>());
            configurations.template insert("localWorkerIp", config["localWorkerIp"].As<std::string>());
            configurations.template insert("dataPort", config["dataPort"].As<uint16_t>());
            configurations.template insert("numberOfSlots", config["numberOfSlots"].As<uint16_t>());
            configurations.template insert("numWorkerThreads", config["numWorkerThreads"].As<uint16_t>());
            configurations.template insert("parentId", config["parentId"].As<std::string>());
            configurations.template insert("logLevel", config["logLevel"].As<std::string>());
            break;
        case ConfigOptionType::SOURCE:
            configurations.template insert("sourceType", config["sourceType"].As<std::string>());
            configurations.template insert("sourceConfig", config["sourceConfig"].As<std::string>());
            configurations.template insert("sourceFrequency", config["sourceFrequency"].As<uint16_t>());
            configurations.template insert("numberOfBuffersToProduce", config["numberOfBuffersToProduce"].As<uint16_t>());
            configurations.template insert("numberOfTuplesToProducePerBuffer", config["numberOfTuplesToProducePerBuffer"].As<uint32_t>());
            configurations.template insert("skipHeader", config["skipHeader"].As<bool>());
            configurations.template insert("physicalStreamName", config["physicalStreamName"].As<std::string>());
            configurations.template insert("logicalStreamName", config["logicalStreamName"].As<std::string>());
    }

    return configurations;
}
template<typename Tp>
const std::map<std::string, Tp>& YAMLConfig<Tp>::getConfigurations() const {
    return configurations;
}

}
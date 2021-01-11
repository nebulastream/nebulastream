

#include <Configs/ConfigOptions/InitializeConfigs.hpp>
#include <Configs/ConfigOption.hpp>
#include <Configs/ConfigOptions/SourceConfig.hpp>
#include <Configs/ConfigOptions/CoordinatorConfig.hpp>
#include <Configs/ConfigOptions/WorkerConfig.hpp>

namespace NES{

template<typename Tp>
InitializeConfigurations<Tp>::InitializeConfigurations() {}
template<typename Tp>
std::any InitializeConfigurations<Tp>::initializeConfigurations(ConfigOptionType executableType) {

    switch (executableType){
        case ConfigOptionType::COORDINATOR: return new CoordinatorConfig();
        case ConfigOptionType::WORKER: return new WorkerConfig();
        case ConfigOptionType::SOURCE: return new SourceConfig();
    }

    return nullptr_t;
}
template<typename Tp>
std::any InitializeConfigurations<Tp>::initializeConfigurations(std::map<std::string, Tp> configurations,
                                                                ConfigOptionType executableType) {

    switch (executableType){
        case ConfigOptionType::COORDINATOR:
            return new CoordinatorConfig(configurations.at("restIp"), configurations.at("coordinatorIp"), configurations.at("restPort"), configurations.at("rpcPort"),
                                         configurations.at("dataPort"), configurations.at("numberOfSlots"), configurations.at("enableQueryMerging"), configurations.at("logLevel"));
        case ConfigOptionType::WORKER: return new WorkerConfig(configurations.at("localWorkerIp"), configurations.at("coordinatorIp"),
                                                               configurations.at("coordinatorPort"), configurations.at("rpcPort"), configurations.at("dataPort"),
                                                               configurations.at("numberOfSlots"), configurations.at("numWorkerThreads"),
                                                               configurations.at("parentId"), configurations.at("logLevel"));
        case ConfigOptionType::SOURCE: return new SourceConfig(configurations.at("sourceType"), configurations.at("sourceConfig"),
                                                               configurations.at("sourceFrequency"), configurations.at("numberOfBuffersToProduce"),
                                                               configurations.at("numberOfTuplesToProducePerBuffer"),
                                                               configurations.at("physicalStreamName"), configurations.at("logicalStreamName"),
                                                               configurations.at("skipHeader"), configurations.at("logLevel"));
    }

    return std::any();
}
template<typename Tp>
std::any InitializeConfigurations<Tp>::overwriteConfigWithCommandLineInput(std::any configObject) {



    return std::any();
}

}
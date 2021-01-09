
#include <Configs/ConfigOptions/YAMLConfig.hpp>


namespace NES{

YAMLConfig::YAMLConfig(std::map<std::string, _Tp> configurations) {}

std::map<std::string, _Tp> YAMLConfig::loadConfig(std::string filePath) { return nullptr; }
void YAMLConfig::setConfigurations(const std::map<std::string, _Tp>& configurations) {
    YAMLConfig::configurations = configurations;
}
}
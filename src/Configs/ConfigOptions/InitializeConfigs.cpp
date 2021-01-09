

#include <Configs/ConfigOptions/InitializeConfigs.hpp>


namespace NES{

NES::InitializeConfigurations::InitializeConfigurations() {}
std::any InitializeConfigurations::initializeConfigurations() { return std::any(); }
template<typename Tp>
std::any InitializeConfigurations::initializeConfigurations(std::map<std::string, Tp> configurations) { return std::any(); }
std::any InitializeConfigurations::overwriteConfigWithCommandLineInput() { return std::any(); }
}
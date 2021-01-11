//
// Created by eleicha on 06.01.21.
//

#ifndef NES_YAMLCONFIG_HPP
#define NES_YAMLCONFIG_HPP

#include <Configs/ConfigOption.hpp>
#include <map>
#include <string>

namespace NES {

template<typename Tp>
class YAMLConfig {

  public:
    YAMLConfig(std::map<std::string, Tp> configurations);

    std::map<std::string, Tp> loadConfig(std::string filePath, ConfigOptionType executableType);

    const std::map<std::string, Tp>& getConfigurations() const;

  private:
    std::map<std::string, Tp> configurations;
};

}// namespace NES

#endif//NES_YAMLCONFIG_HPP

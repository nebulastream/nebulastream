//
// Created by eleicha on 06.01.21.
//

#ifndef NES_YAMLCONFIG_HPP
#define NES_YAMLCONFIG_HPP

#include <Configs/ConfigOption.hpp>
#include <map>
#include <string>

namespace NES {

class YAMLConfig {

  public:
    //todo: does _tp work?
    YAMLConfig(std::map<std::string, _Tp> configurations);

    std::map<std::string, _Tp> loadConfig(std::string filePath);
    void setConfigurations(const std::map<std::string, _Tp>& configurations);

  private:
    std::map<std::string, _Tp> configurations;
};

}// namespace NES

#endif//NES_YAMLCONFIG_HPP

//
// Created by eleicha on 06.01.21.
//

#ifndef NES_INITIALIZECONFIGS_HPP
#define NES_INITIALIZECONFIGS_HPP

#include "CoordinatorConfig.hpp"
#include <any>
#include <map>
#include <string>
#include <typeinfo>

namespace NES {

template<typename Tp>
class InitializeConfigurations {

  public:
    InitializeConfigurations();

    /**
     * initialize a configuration object
     * @return return a configuration object
     */
    std::any initializeConfigurations(ConfigOptionType executableType);

    /**
     * initialize configurations with yaml file input
     * @param configurations from a YAML file saved in a HashMap
     * @return return a configuration object
     */
    std::any initializeConfigurations(std::map<std::string, Tp> configurations, ConfigOptionType executableType);

    /**
     * overwrite default or YAML file values with command line input
     * @return return a configuration object
     */
    std::any overwriteConfigWithCommandLineInput(std::any configObject);
};

}// namespace NES

#endif//NES_INITIALIZECONFIGS_HPP

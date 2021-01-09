//
// Created by eleicha on 06.01.21.
//

#ifndef NES_INITIALIZECONFIGS_HPP
#define NES_INITIALIZECONFIGS_HPP

#include <any>
#include <map>
#include <string>
#include <typeinfo>

namespace NES {

class InitializeConfigurations {

  public:
    InitializeConfigurations();

    /**
     * initialize a configuration object
     * @return return a configuration object
     */
    std::any initializeConfigurations();

    /**
     * initialize configurations with yaml file input
     * @param configurations from a YAML file saved in a HashMap
     * @return return a configuration object
     */
    template<typename Tp>
    std::any initializeConfigurations(std::map<std::string, Tp> configurations);

    /**
     * overwrite default or YAML file values with command line input
     * @return return a configuration object
     */
    std::any overwriteConfigWithCommandLineInput();
};

}// namespace NES

#endif//NES_INITIALIZECONFIGS_HPP

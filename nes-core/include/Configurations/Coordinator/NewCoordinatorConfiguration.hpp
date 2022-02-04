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

#ifndef NES_INCLUDE_CONFIGURATIONS_COORDINATOR_NEWCOORDINATORCONFIGURATION_HPP_
#define NES_INCLUDE_CONFIGURATIONS_COORDINATOR_NEWCOORDINATORCONFIGURATION_HPP_

#include <Catalogs/Source/LogicalSource.hpp>
#include <Configurations/ConfigOptions/BaseConfiguration.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <iostream>
#include <map>
#include <string>
#include <thread>

namespace NES {

namespace Configurations {

enum LogLevel { LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE };

class LogicalSourceFactory {
  public:
    static LogicalSourcePtr createFromString(std::string string);
    static LogicalSourcePtr createFromYaml(Yaml::Node);
};

/**
 * @brief ConfigOptions for Coordinator
 */
class TestConfiguration : public BaseConfiguration {
  public:
    StringOption restIp = {"restIp", "127.0.0.1", "NES ip of the REST server."};
    StringOption dataip = {"dataip", "127.0.0.1", "NES ip of the REST server."};
    IntOption rpcPort = {"rpcPort", 4000, "RPC server port of the NES Coordinator"};
    BoolOption enableMonitoring = {"enableMonitoring", false, "Enable monitoring"};
    EnumOption<LogLevel> logLevel = {"logLevel", LOG_DEBUG, "Sets the log level"};
    OptimizerConfiguration optimizerConfig = {"optimizerConfig", "Defines optimizer configuration"};
    SequenceOption<WrapOption<LogicalSourcePtr, LogicalSourceFactory>> logicalSources = {"logicalSources",
                                                                                         "defines the logical sources"};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&restIp, &dataip, &rpcPort, &enableMonitoring, &logLevel, &optimizerConfig, &logicalSources};
    }
};

}// namespace Configurations
}// namespace NES

#endif// NES_INCLUDE_CONFIGURATIONS_COORDINATOR_NEWCOORDINATORCONFIGURATION_HPP_

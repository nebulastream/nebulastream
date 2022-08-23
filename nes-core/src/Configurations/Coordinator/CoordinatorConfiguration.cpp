/*
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

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <string>
#include <map>
#include <utility>

namespace NES {

namespace Configurations {

CoordinatorConfigurationPtr CoordinatorConfiguration::create(const int argc, const char** argv) {
    // Convert the POSIX command line arguments to a map of strings
    std::map<std::string, std::string> commandLineParams;
    // TODO Refactor code: extract to helper method; use unordered map
    for (int i = 1; i < argc; ++i) {
        const int pos = std::string(argv[i]).find('=');
        commandLineParams.insert({std::string(argv[i]).substr(0, pos),
                                  std::string(argv[i]).substr(pos + 1, std::string(argv[i]).length() - 1)});
    }

    // Create a configuration object with default values.
    CoordinatorConfigurationPtr config = CoordinatorConfiguration::create();

    // Read options from the YAML file.
    auto configPath = commandLineParams.find("--" + CONFIG_PATH);
    if (configPath != commandLineParams.end()) {
        config->overwriteConfigWithYAMLFileInput(configPath->second);
    }

    // Load any worker configuration file that was specified in the coordinator configuration file.
    if (config->workerConfigPath.getValue() != config->workerConfigPath.getDefaultValue()) {
        config->worker.overwriteConfigWithYAMLFileInput(config->workerConfigPath);
    }

    // Load any worker configuration file that was specified on the command line.
    auto workerConfigPath = commandLineParams.find("--" + WORKER_CONFIG_PATH);
    if (workerConfigPath != commandLineParams.end()) {
        config->worker.overwriteConfigWithYAMLFileInput(workerConfigPath->second);
    }

    // Options specified on the command line have the highest precedence.
    config->overwriteConfigWithCommandLineInput(commandLineParams);

    return config;
}

}// namespace Configurations


}// namespace NES

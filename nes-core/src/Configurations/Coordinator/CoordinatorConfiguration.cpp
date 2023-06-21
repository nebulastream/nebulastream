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
#include <map>
#include <string>

namespace NES {

namespace Configurations {

// The evaluation order is as follows:
// 1. A worker option inside coordinator.yml.
// 2. An option inside worker-1.yml, which was specified inside coordinator.yml.
// 3. An option inside worker-2.yml, which was specified on the command line.
// 4. A worker option on the command line.
// This evaluation follows the expectation that options on the command line (including workerConfigPath)
// overwrite options in the configuration file.
CoordinatorConfigurationPtr CoordinatorConfiguration::create(const int argc, const char** argv) {
    // Convert the POSIX command line arguments to a map of strings.
    std::map<std::string, std::string> commandLineParams;
    for (int i = 1; i < argc; ++i) {
        const int pos = std::string(argv[i]).find('=');
        const std::string arg{argv[i]};
        commandLineParams.insert({arg.substr(0, pos), arg.substr(pos + 1, arg.length() - 1)});
    }

    // Create a configuration object with default values.
    CoordinatorConfigurationPtr config = CoordinatorConfiguration::createDefault();

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

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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCEFACTORYPLUGIN_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCEFACTORYPLUGIN_HPP_

#include <Util/PluginRegistry.hpp>
namespace NES {

class PhysicalSourceType;
using PhysicalSourceTypePtr = std::shared_ptr<PhysicalSourceType>;

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

namespace Configurations {

class PhysicalSourceFactoryPlugin {
  public:
    PhysicalSourceFactoryPlugin() = default;
    virtual PhysicalSourceTypePtr createPhysicalSourceType(std::string sourceType,
                                                           const std::map<std::string, std::string>& commandLineParams) = 0;
    virtual PhysicalSourceTypePtr createPhysicalSourceType(std::string sourceType, Yaml::Node& yamlConfig) = 0;
    virtual ~PhysicalSourceFactoryPlugin() = default;
};

using PhysicalSourceFactoryPluginRegistry = Util::PluginRegistry<PhysicalSourceFactoryPlugin>;

}// namespace Configurations
}// namespace NES
#endif // NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCEFACTORYPLUGIN_HPP_

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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_RESOURCESREADER_SYSTEMRESOURCESREADERFACTORY_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_RESOURCESREADER_SYSTEMRESOURCESREADERFACTORY_HPP_
#include <memory>

namespace NES::Monitoring {

class AbstractSystemResourcesReader;
using AbstractSystemResourcesReaderPtr = std::shared_ptr<AbstractSystemResourcesReader>;

class SystemResourcesReaderFactory {
  public:
    /**
     * @brief Creates the appropriate SystemResourcesReader for the OS
     * @return the SystemResourcesReader
     */
    static AbstractSystemResourcesReaderPtr getSystemResourcesReader();
};

}// namespace NES::Monitoring

#endif // NES_RUNTIME_INCLUDE_MONITORING_RESOURCESREADER_SYSTEMRESOURCESREADERFACTORY_HPP_

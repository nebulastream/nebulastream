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

#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/LinuxSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Monitoring {

AbstractSystemResourcesReaderPtr SystemResourcesReaderFactory::getSystemResourcesReader() {
#ifdef __linux__
    auto abstractReader = std::make_shared<LinuxSystemResourcesReader>();
    NES_INFO("SystemResourcesReaderFactory: Linux detected, return LinuxSystemResourcesReader");
#else
    auto abstractReader = std::make_shared<AbstractSystemResourcesReader>();
    NES_INFO("SystemResourcesReaderFactory: OS not supported, return DefaultReader");
#endif

    return abstractReader;
};

}// namespace NES::Monitoring

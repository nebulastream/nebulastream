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

#include <Configurations/Persistence/DefaultStreamCatalogPersistence.hpp>
#include <Util/Logger.hpp>
#include <filesystem>

namespace NES {

DefaultStreamCatalogPersistencePtr DefaultStreamCatalogPersistence::create() {
    return std::make_shared<DefaultStreamCatalogPersistence>();
}

bool DefaultStreamCatalogPersistence::persistLogicalStream(const std::string&, const std::string&) { return true; }
bool DefaultStreamCatalogPersistence::deleteLogicalStream(const std::string&) { return true; }

std::map<std::string, std::string> DefaultStreamCatalogPersistence::loadLogicalStreams() {
    return std::map<std::string, std::string>();
}

}// namespace NES

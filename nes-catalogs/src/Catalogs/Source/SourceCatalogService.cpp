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

#include <API/Schema.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogService.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {
SourceCatalogService::SourceCatalogService(Catalogs::Source::SourceCatalogPtr sourceCatalog)
    : sourceCatalog(std::move(sourceCatalog)) {
    NES_DEBUG("SourceCatalogService()");
    NES_ASSERT(this->sourceCatalog, "sourceCatalogPtr has to be valid");
}

bool SourceCatalogService::updateLogicalSource(const std::string& logicalSourceName, SchemaPtr schema) {
    NES_DEBUG("SourceCatalogService::update logical source {} with schema {}", logicalSourceName, schema->toString());
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    return sourceCatalog->updateLogicalSource(logicalSourceName, std::move(schema));
}

bool SourceCatalogService::unregisterLogicalSource(const std::string& logicalSourceName) {
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    NES_DEBUG("SourceCatalogService::unregisterLogicalSource: register logical source= {}", logicalSourceName);
    bool success = sourceCatalog->removeLogicalSource(logicalSourceName);
    return success;
}

LogicalSourcePtr SourceCatalogService::getLogicalSource(const std::string& logicalSourceName) {
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
    return std::make_shared<LogicalSource>(*logicalSource);
}

std::map<std::string, std::string> SourceCatalogService::getAllLogicalSourceAsString() {
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    return sourceCatalog->getAllLogicalSourceAsString();
}

std::vector<Catalogs::Source::SourceCatalogEntryPtr>
SourceCatalogService::getPhysicalSources(const std::string& logicalSourceName) {
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    return sourceCatalog->getPhysicalSources(logicalSourceName);
}

bool SourceCatalogService::reset() {
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    return sourceCatalog->reset();
}
}//namespace NES

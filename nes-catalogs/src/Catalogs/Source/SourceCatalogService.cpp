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

#include <utility>
#include <API/Schema.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogService.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

SourceCatalogService::SourceCatalogService(Catalogs::Source::SourceCatalogPtr sourceCatalog) : sourceCatalog(std::move(sourceCatalog))
{
    NES_DEBUG("SourceCatalogService()");
    NES_ASSERT(this->sourceCatalog, "sourceCatalogPtr has to be valid");
}

std::pair<bool, std::string> SourceCatalogService::registerPhysicalSource(
    const std::string& physicalSourceName, const std::string& logicalSourceName, WorkerId topologyNodeId)
{
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);

    if (!sourceCatalog->containsLogicalSource(logicalSourceName))
    {
        NES_ERROR("SourceCatalogService::RegisterPhysicalSource logical source does not exist {}", logicalSourceName);
        return {false, fmt::format("RegisterPhysicalSource logical source does not exist {}", logicalSourceName)};
    }

    NES_DEBUG(
        "SourceCatalogService::RegisterPhysicalSource: try to register physical node id {} physical source= {} logical "
        "source= {}",
        topologyNodeId,
        physicalSourceName,
        logicalSourceName);
    auto physicalSource = PhysicalSource::create(logicalSourceName, physicalSourceName);
    auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, topologyNodeId);
    bool success = sourceCatalog->addPhysicalSource(logicalSourceName, sce);
    if (!success)
    {
        NES_ERROR("SourceCatalogService::RegisterPhysicalSource: adding physical source was not successful.");
        return {false, "RegisterPhysicalSource: adding physical source was not successful."};
    }
    return {success, ""};
}

bool SourceCatalogService::unregisterPhysicalSource(
    const std::string& physicalSourceName, const std::string& logicalSourceName, WorkerId topologyNodeId)
{
    NES_DEBUG(
        "SourceCatalogService::UnregisterPhysicalSource: try to remove physical source with name {} logical name {} workerId= {}",
        physicalSourceName,
        logicalSourceName,
        topologyNodeId);
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);

    NES_DEBUG("SourceCatalogService: node= {}", topologyNodeId);

    bool success = sourceCatalog->removePhysicalSource(logicalSourceName, physicalSourceName, topologyNodeId);
    if (!success)
    {
        NES_ERROR("SourceCatalogService::RegisterPhysicalSource: removing physical source was not successful.");
        return false;
    }
    return success;
}

bool SourceCatalogService::registerLogicalSource(const std::string& logicalSourceName, SchemaPtr schema)
{
    NES_DEBUG("SourceCatalogService::registerLogicalSource: register logical source= {} schema= {}", logicalSourceName, schema->toString());
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    return sourceCatalog->addLogicalSource(logicalSourceName, std::move(schema));
}

bool SourceCatalogService::updateLogicalSource(const std::string& logicalSourceName, SchemaPtr schema)
{
    NES_DEBUG("SourceCatalogService::update logical source {} with schema {}", logicalSourceName, schema->toString());
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    return sourceCatalog->updateLogicalSource(logicalSourceName, std::move(schema));
}

bool SourceCatalogService::unregisterLogicalSource(const std::string& logicalSourceName)
{
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    NES_DEBUG("SourceCatalogService::unregisterLogicalSource: register logical source= {}", logicalSourceName);
    bool success = sourceCatalog->removeLogicalSource(logicalSourceName);
    return success;
}

LogicalSourcePtr SourceCatalogService::getLogicalSource(const std::string& logicalSourceName)
{
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
    return std::make_shared<LogicalSource>(*logicalSource);
}

std::map<std::string, std::string> SourceCatalogService::getAllLogicalSourceAsString()
{
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    return sourceCatalog->getAllLogicalSourceAsString();
}

std::vector<Catalogs::Source::SourceCatalogEntryPtr> SourceCatalogService::getPhysicalSources(const std::string& logicalSourceName)
{
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    return sourceCatalog->getPhysicalSources(logicalSourceName);
}

bool SourceCatalogService::reset()
{
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    return sourceCatalog->reset();
}

bool SourceCatalogService::addKeyDistributionEntry(const Catalogs::Source::SourceCatalogEntryPtr& entry, std::set<uint64_t> keys)
{
    std::unique_lock<std::mutex> lock(sourceCatalogMutex);
    if (sourceCatalog->getKeyDistributionMap().contains(entry))
    {
        NES_WARNING("SourceCatalogService::addKeyDistributionEntry: entry {} already exists, overwriting values", entry->toString());
    }
    sourceCatalog->getKeyDistributionMap()[entry] = std::move(keys);
    return true;
}

bool SourceCatalogService::unregisterAllPhysicalSourcesByWorker(WorkerId workerId)
{
    std::unique_lock lock(sourceCatalogMutex);
    NES_DEBUG("SourceCatalogService::unregisterAllPhysicalSourcesByWorker: remove all physical sources for worker: {}", workerId);
    sourceCatalog->removeAllPhysicalSourcesByWorker(workerId);

    return true;
}

} //namespace NES

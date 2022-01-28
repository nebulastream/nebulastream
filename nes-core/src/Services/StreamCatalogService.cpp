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

#include "Catalogs/Source/SourceCatalogEntry.hpp"
#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <CoordinatorRPCService.pb.h>
#include <Services/StreamCatalogService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

StreamCatalogService::StreamCatalogService(SourceCatalogPtr streamCatalog) : streamCatalog(std::move(streamCatalog)) {
    NES_DEBUG("StreamCatalogService()");
    NES_ASSERT(this->streamCatalog, "streamCatalogPtr has to be valid");
}

bool StreamCatalogService::registerPhysicalStream(TopologyNodePtr topologyNode,
                                                  const std::string& physicalSourceName,
                                                  const std::string& logicalSourceName) {
    if (!topologyNode) {
        NES_ERROR("StreamCatalogService::RegisterPhysicalSource node not found");
        return false;
    }

    NES_DEBUG("StreamCatalogService::RegisterPhysicalSource: try to register physical node id "
              << topologyNode->getId() << " physical stream=" << physicalSourceName << " logical stream=" << logicalSourceName);
    std::unique_lock<std::mutex> lock(addRemovePhysicalStream);
    auto physicalSource = PhysicalSource::create(logicalSourceName, physicalSourceName);
    auto logicalSource = streamCatalog->getStreamForLogicalStream(logicalSourceName);
    SourceCatalogEntryPtr sce = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, topologyNode);
    bool success = streamCatalog->addPhysicalSource(logicalSourceName, sce);
    if (!success) {
        NES_ERROR("StreamCatalogService::RegisterPhysicalSource: adding physical stream was not successful.");
        return false;
    }
    return success;
}

bool StreamCatalogService::unregisterPhysicalStream(TopologyNodePtr topologyNode,
                                                    const std::string& physicalSourceName,
                                                    const std::string& logicalSourceName) {
    NES_DEBUG("StreamCatalogService::UnregisterPhysicalSource: try to remove physical stream with name "
              << physicalSourceName << " logical name " << logicalSourceName << " workerId=" << topologyNode->getId());
    std::unique_lock<std::mutex> lock(addRemovePhysicalStream);

    if (!topologyNode) {
        NES_DEBUG("StreamCatalogService::UnregisterPhysicalSource: sensor not found with workerId" << topologyNode->getId());
        return false;
    }
    NES_DEBUG("StreamCatalogService: node=" << topologyNode->toString());

    bool success = streamCatalog->removePhysicalStream(logicalSourceName, physicalSourceName, topologyNode->getId());
    if (!success) {
        NES_ERROR("StreamCatalogService::RegisterPhysicalSource: removing physical stream was not successful.");
        return false;
    }
    return success;
}

bool StreamCatalogService::registerLogicalSource(const std::string& logicalSourceName, const std::string& schemaString) {
    NES_DEBUG("StreamCatalogService::registerLogicalSource: register logical stream=" << logicalSourceName
                                                                                      << " schema=" << schemaString);
    std::unique_lock<std::mutex> lock(addRemoveLogicalStream);
    return streamCatalog->addLogicalStream(logicalSourceName, schemaString);
}

bool StreamCatalogService::registerLogicalSource(const std::string& logicalSourceName, SchemaPtr schema) {
    NES_DEBUG("StreamCatalogService::registerLogicalSource: register logical stream=" << logicalSourceName);
    std::unique_lock<std::mutex> lock(addRemoveLogicalStream);
    return streamCatalog->addLogicalStream(logicalSourceName, std::move(schema));
}

bool StreamCatalogService::unregisterLogicalStream(const std::string& logicalStreamName) {
    std::unique_lock<std::mutex> lock(addRemoveLogicalStream);
    NES_DEBUG("StreamCatalogService::unregisterLogicalStream: register logical stream=" << logicalStreamName);
    bool success = streamCatalog->removeLogicalStream(logicalStreamName);
    return success;
}

}//namespace NES

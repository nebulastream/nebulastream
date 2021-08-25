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

#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <CoordinatorRPCService.pb.h>
#include <NodeStats.pb.h>
#include <Services/StreamCatalogService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

StreamCatalogService::StreamCatalogService(StreamCatalogPtr streamCatalog) : streamCatalog(std::move(streamCatalog)) {
    NES_DEBUG("StreamCatalogService()");
    NES_ASSERT(this->streamCatalog, "streamCatalogPtr has to be valid");
}

bool StreamCatalogService::registerPhysicalStream(TopologyNodePtr physicalNode,
                                                  const std::string& sourceType,
                                                  const std::string& physicalStreamName,
                                                  const std::string& logicalStreamName) {
    if (!physicalNode) {
        NES_ERROR("StreamCatalogService::RegisterPhysicalStream node not found");
        return false;
    }

    NES_DEBUG("StreamCatalogService::RegisterPhysicalStream: try to register physical node id "
              << physicalNode->getId() << " physical stream=" << physicalStreamName << " logical stream=" << logicalStreamName);
    std::unique_lock<std::mutex> lock(addRemovePhysicalStream);

    StreamCatalogEntryPtr sce =
        std::make_shared<StreamCatalogEntry>(sourceType, physicalStreamName, logicalStreamName, physicalNode);
    bool success = streamCatalog->addPhysicalStream(logicalStreamName, sce);
    if (!success) {
        NES_ERROR("StreamCatalogService::RegisterPhysicalStream: adding physical stream was not successful.");
        return false;
    }
    return success;
}

bool StreamCatalogService::unregisterPhysicalStream(TopologyNodePtr physicalNode,
                                                    const std::string& physicalStreamName,
                                                    const std::string& logicalStreamName) {
    NES_DEBUG("StreamCatalogService::UnregisterPhysicalStream: try to remove physical stream with name "
              << physicalStreamName << " logical name " << logicalStreamName << " workerId=" << physicalNode->getId());
    std::unique_lock<std::mutex> lock(addRemovePhysicalStream);

    if (!physicalNode) {
        NES_DEBUG("StreamCatalogService::UnregisterPhysicalStream: sensor not found with workerId" << physicalNode->getId());
        return false;
    }
    NES_DEBUG("StreamCatalogService: node=" << physicalNode->toString());

    bool success = streamCatalog->removePhysicalStream(logicalStreamName, physicalStreamName, physicalNode->getId());
    if (!success) {
        NES_ERROR("StreamCatalogService::RegisterPhysicalStream: removing physical stream was not successful.");
        return false;
    }
    return success;
}

bool StreamCatalogService::registerLogicalStream(const std::string& logicalStreamName, const std::string& schemaString) {
    NES_DEBUG("StreamCatalogService::registerLogicalStream: register logical stream=" << logicalStreamName
                                                                                      << " schema=" << schemaString);
    std::unique_lock<std::mutex> lock(addRemoveLogicalStream);
    return streamCatalog->addLogicalStream(logicalStreamName, schemaString);
}

bool StreamCatalogService::unregisterLogicalStream(const std::string& logicalStreamName) {
    std::unique_lock<std::mutex> lock(addRemoveLogicalStream);
    NES_DEBUG("StreamCatalogService::unregisterLogicalStream: register logical stream=" << logicalStreamName);
    bool success = streamCatalog->removeLogicalStream(logicalStreamName);
    return success;
}

}//namespace NES

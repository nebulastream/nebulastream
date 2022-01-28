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

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

void SourceCatalog::addDefaultStreams() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("Streamcatalog addDefaultStreams");
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    bool success = addLogicalStream("default_logical", schema);
    if (!success) {
        NES_ERROR("SourceCatalog::addDefaultStreams: error while add default_logical");
        throw Exception("Error while addDefaultStreams SourceCatalog");
    }

    //TODO I think we should get rid of this soon
    SchemaPtr schemaExdra = Schema::create()
                                ->addField("id", DataTypeFactory::createUInt64())
                                ->addField("metadata_generated", DataTypeFactory::createUInt64())
                                ->addField("metadata_title", DataTypeFactory::createFixedChar(50))
                                ->addField("metadata_id", DataTypeFactory::createFixedChar(50))
                                ->addField("features_type", DataTypeFactory::createFixedChar(50))
                                ->addField("features_properties_capacity", DataTypeFactory::createUInt64())
                                ->addField("features_properties_efficiency", DataTypeFactory::createFloat())
                                ->addField("features_properties_mag", DataTypeFactory::createFloat())
                                ->addField("features_properties_time", DataTypeFactory::createUInt64())
                                ->addField("features_properties_updated", DataTypeFactory::createUInt64())
                                ->addField("features_properties_type", DataTypeFactory::createFixedChar(50))
                                ->addField("features_geometry_type", DataTypeFactory::createFixedChar(50))
                                ->addField("features_geometry_coordinates_longitude", DataTypeFactory::createFloat())
                                ->addField("features_geometry_coordinates_latitude", DataTypeFactory::createFloat())
                                ->addField("features_eventId ", DataTypeFactory::createFixedChar(50));

    bool success2 = addLogicalStream("exdra", schemaExdra);
    if (!success2) {
        NES_ERROR("SourceCatalog::addDefaultStreams: error while adding exdra logical stream");
        throw Exception("Error while addDefaultStreams SourceCatalog");
    }
}
SourceCatalog::SourceCatalog(QueryParsingServicePtr queryParsingService) : queryParsingService(queryParsingService) {
    NES_DEBUG("SourceCatalog: construct stream catalog");
    addDefaultStreams();
    NES_DEBUG("SourceCatalog: construct stream catalog successfully");
}

bool SourceCatalog::addLogicalStream(const std::string& streamName, const std::string& streamSchema) {
    std::unique_lock lock(catalogMutex);
    SchemaPtr schema = queryParsingService->createSchemaFromCode(streamSchema);
    NES_DEBUG("SourceCatalog: schema successfully created");
    return addLogicalStream(streamName, schema);
}

bool SourceCatalog::addLogicalStream(const std::string& logicalStreamName, SchemaPtr schemaPtr) {
    std::unique_lock lock(catalogMutex);
    //check if stream already exist
    NES_DEBUG("SourceCatalog: search for logical stream in addLogicalStream() " << logicalStreamName);

    if (!containsLogicalSource(logicalStreamName)) {
        NES_DEBUG("SourceCatalog: add logical stream " << logicalStreamName);
        logicalSourceNameToSchemaMapping[logicalStreamName] = std::move(schemaPtr);
        return true;
    }
    NES_ERROR("SourceCatalog: logical stream " << logicalStreamName << " already exists");
    return false;
}

bool SourceCatalog::removeLogicalStream(const std::string& logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("SourceCatalog: search for logical stream in removeLogicalStream() " << logicalStreamName);

    //if log stream does not exists
    if (logicalSourceNameToSchemaMapping.find(logicalStreamName) == logicalSourceNameToSchemaMapping.end()) {
        NES_ERROR("SourceCatalog: logical stream " << logicalStreamName << " does not exists");
        return false;
    }
    NES_DEBUG("SourceCatalog: remove logical stream " << logicalStreamName);
    if (logicalToPhysicalSourceMapping[logicalStreamName].size() != 0) {
        NES_DEBUG("SourceCatalog: cannot remove " << logicalStreamName << " because there are physical entries for this stream");
        return false;
    }
    uint64_t cnt = logicalSourceNameToSchemaMapping.erase(logicalStreamName);
    NES_DEBUG("SourceCatalog: removed " << cnt << " copies of the stream");
    NES_ASSERT(!containsLogicalSource(logicalStreamName), "log stream should not exist");
    return true;
}

bool SourceCatalog::addPhysicalSource(const std::string& logicalSourceName, const SourceCatalogEntryPtr& newSourceCatalogEntry) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("SourceCatalog: search for logical stream in addPhysicalSource() " << logicalSourceName);

    // check if logical stream exists
    if (!containsLogicalSource(logicalSourceName)) {
        NES_ERROR("SourceCatalog: logical stream " << logicalSourceName << " does not exists when inserting physical stream "
                                                   << newSourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName());
        return false;
    } else {
        NES_DEBUG("SourceCatalog: logical stream " << logicalSourceName << " exists try to add physical stream "
                                                   << newSourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName());

        //get current physical stream for this logical stream
        std::vector<SourceCatalogEntryPtr> physicalStreams = logicalToPhysicalSourceMapping[logicalSourceName];

        //check if physical stream does not exist yet
        for (const SourceCatalogEntryPtr& sourceCatalogEntry : physicalStreams) {
            auto physicalSourceToTest = sourceCatalogEntry->getPhysicalSource();
            NES_DEBUG("test node id=" << sourceCatalogEntry->getNode()->getId()
                                      << " phyStr=" << physicalSourceToTest->getPhysicalSourceName());
            if (physicalSourceToTest->getPhysicalSourceName()
                    == newSourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName()
                && sourceCatalogEntry->getNode()->getId() == newSourceCatalogEntry->getNode()->getId()) {
                NES_ERROR("SourceCatalog: node with id=" << sourceCatalogEntry->getNode()->getId() << " name="
                                                         << physicalSourceToTest->getPhysicalSourceName() << " already exists");
                return false;
            }
        }
    }

    NES_DEBUG("SourceCatalog: physical stream " << newSourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName()
                                                << " does not exist, try to add");

    //if first one
    if (testIfLogicalStreamExistsInLogicalToPhysicalMapping(logicalSourceName)) {
        NES_DEBUG("stream already exist, just add new entry");
        logicalToPhysicalSourceMapping[logicalSourceName].push_back(newSourceCatalogEntry);
    } else {
        NES_DEBUG("stream does not exist, create new item");
        logicalToPhysicalSourceMapping.insert(
            std::pair<std::string, std::vector<SourceCatalogEntryPtr>>(logicalSourceName, std::vector<SourceCatalogEntryPtr>()));
        logicalToPhysicalSourceMapping[logicalSourceName].push_back(newSourceCatalogEntry);
    }

    NES_DEBUG("SourceCatalog: physical stream " << newSourceCatalogEntry->getPhysicalSource()
                                                << " id=" << newSourceCatalogEntry->getNode()->getId() << " successful added");
    return true;
}

bool SourceCatalog::removeAllPhysicalStreams(const std::string&) {
    std::unique_lock lock(catalogMutex);
    NES_NOT_IMPLEMENTED();
}

bool SourceCatalog::removePhysicalStream(const std::string& logicalStreamName,
                                         const std::string& physicalStreamName,
                                         std::uint64_t hashId) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("SourceCatalog: search for logical stream in removePhysicalStream() " << logicalStreamName);

    // check if logical stream exists
    if (logicalSourceNameToSchemaMapping.find(logicalStreamName) == logicalSourceNameToSchemaMapping.end()) {
        NES_ERROR("SourceCatalog: logical stream "
                  << logicalStreamName << " does not exists when trying to remove physical stream with hashId" << hashId);
        return false;
    }
    NES_DEBUG("SourceCatalog: logical stream " << logicalStreamName << " exists try to remove physical stream"
                                               << physicalStreamName << " from node " << hashId);
    for (auto entry = logicalToPhysicalSourceMapping[logicalStreamName].cbegin();
         entry != logicalToPhysicalSourceMapping[logicalStreamName].cend();
         entry++) {
        NES_DEBUG("test node id=" << entry->get()->getNode()->getId()
                                  << " phyStr=" << entry->get()->getPhysicalSource()->getPhysicalSourceName());
        NES_DEBUG("test to be deleted id=" << hashId << " phyStr=" << physicalStreamName);
        if (entry->get()->getPhysicalSource()->getPhysicalSourceName() == physicalStreamName) {
            NES_DEBUG("SourceCatalog: node with name=" << physicalStreamName << " exists try match hashId" << hashId);

            if (entry->get()->getNode()->getId() == hashId) {
                NES_DEBUG("SourceCatalog: node with id=" << hashId << " name=" << physicalStreamName << " exists try to erase");
                logicalToPhysicalSourceMapping[logicalStreamName].erase(entry);
                NES_DEBUG("SourceCatalog: number of entries afterwards "
                          << logicalToPhysicalSourceMapping[logicalStreamName].size());
                return true;
            }
        }
    }
    NES_DEBUG("SourceCatalog: physical stream " << physicalStreamName << " does not exist on node with id" << hashId
                                                << " and with logicalSourceName " << logicalStreamName);

    NES_DEBUG("SourceCatalog: physical stream " << physicalStreamName << " does not exist on node with id" << hashId);
    return false;
}

bool SourceCatalog::removePhysicalStreamByHashId(uint64_t hashId) {
    std::unique_lock lock(catalogMutex);
    for (const auto& logStream : logicalToPhysicalSourceMapping) {
        NES_DEBUG("SourceCatalog: check log stream " << logStream.first);
        for (auto entry = logicalToPhysicalSourceMapping[logStream.first].cbegin();
             entry != logicalToPhysicalSourceMapping[logStream.first].cend();
             entry++) {
            if (entry->get()->getNode()->getId() == hashId) {
                NES_DEBUG("SourceCatalog: found entry with nodeid=" << entry->get()->getNode()->getId() << " physicalStream="
                                                                    << entry->get()->getPhysicalSource()->getPhysicalSourceName()
                                                                    << " logicalStream=" << logStream.first);
                //TODO: fix this to return value of erase to update entry or if you use the foreach loop, collect the entries to remove, and remove them in a batch after
                NES_DEBUG("SourceCatalog: deleted physical stream with hashID"
                          << hashId << "and name" << entry->get()->getPhysicalSource()->getPhysicalSourceName());
                logicalToPhysicalSourceMapping[logStream.first].erase(entry);
                return true;
            }
        }
    }
    return false;
}

SchemaPtr SourceCatalog::getSchemaForLogicalStream(const std::string& logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return logicalSourceNameToSchemaMapping[logicalStreamName];
}

LogicalSourcePtr SourceCatalog::getStreamForLogicalStream(const std::string& logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return LogicalSource::create(logicalStreamName, logicalSourceNameToSchemaMapping[logicalStreamName]);
}

LogicalSourcePtr SourceCatalog::getStreamForLogicalStreamOrThrowException(const std::string& logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    if (logicalSourceNameToSchemaMapping.find(logicalStreamName) != logicalSourceNameToSchemaMapping.end()) {
        return LogicalSource::create(logicalStreamName, logicalSourceNameToSchemaMapping[logicalStreamName]);
    }
    NES_ERROR("SourceCatalog::getStreamForLogicalStreamOrThrowException: stream does not exists " << logicalStreamName);
    throw Exception("Required stream does not exists " + logicalStreamName);
}

bool SourceCatalog::containsLogicalSource(const std::string& logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return logicalSourceNameToSchemaMapping.find(logicalStreamName)//if log stream does not exists
        != logicalSourceNameToSchemaMapping.end();
}
bool SourceCatalog::testIfLogicalStreamExistsInLogicalToPhysicalMapping(const std::string& logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return logicalToPhysicalSourceMapping.find(logicalStreamName)//if log stream does not exists
        != logicalToPhysicalSourceMapping.end();
}

std::vector<TopologyNodePtr> SourceCatalog::getSourceNodesForLogicalStream(const std::string& logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    std::vector<TopologyNodePtr> listOfSourceNodes;

    //get current physical stream for this logical stream
    std::vector<SourceCatalogEntryPtr> physicalStreams = logicalToPhysicalSourceMapping[logicalStreamName];

    if (physicalStreams.empty()) {
        return listOfSourceNodes;
    }

    for (const SourceCatalogEntryPtr& entry : physicalStreams) {
        listOfSourceNodes.push_back(entry->getNode());
    }

    return listOfSourceNodes;
}

bool SourceCatalog::reset() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("SourceCatalog: reset Stream Catalog");
    logicalSourceNameToSchemaMapping.clear();
    logicalToPhysicalSourceMapping.clear();

    addDefaultStreams();
    NES_DEBUG("SourceCatalog: reset Stream Catalog completed");
    return true;
}

std::string SourceCatalog::getPhysicalStreamAndSchemaAsString() {
    std::unique_lock lock(catalogMutex);
    std::stringstream ss;
    for (const auto& entry : logicalToPhysicalSourceMapping) {
        ss << "stream name=" << entry.first << " with " << entry.second.size() << " elements:";
        for (const SourceCatalogEntryPtr& sce : entry.second) {
            ss << sce->toString();
        }
        ss << std::endl;
    }
    return ss.str();
}

std::vector<SourceCatalogEntryPtr> SourceCatalog::getPhysicalSources(const std::string& logicalStreamName) {
    return logicalToPhysicalSourceMapping[logicalStreamName];
}

std::map<std::string, SchemaPtr> SourceCatalog::getAllLogicalStream() { return logicalSourceNameToSchemaMapping; }

std::map<std::string, std::string> SourceCatalog::getAllLogicalStreamAsString() {
    std::unique_lock lock(catalogMutex);
    std::map<std::string, std::string> allLogicalStreamAsString;
    const std::map<std::string, SchemaPtr> allLogicalStream = getAllLogicalStream();

    for (auto const& [key, val] : allLogicalStream) {
        allLogicalStreamAsString[key] = val->toString();
    }
    return allLogicalStreamAsString;
}

bool SourceCatalog::updatedLogicalStream(std::string& streamName, std::string& streamSchema) {
    NES_INFO("SourceCatalog: Update the logical stream " << streamName << " with the schema " << streamSchema);
    std::unique_lock lock(catalogMutex);

    NES_TRACE("SourceCatalog: Check if logical stream exists in the catalog.");
    if (!containsLogicalSource(streamName)) {
        NES_ERROR("SourceCatalog: Unable to find logical stream " << streamName << " to update.");
        return false;
    }

    NES_TRACE("SourceCatalog: create a new schema object and add to the catalog");
    SchemaPtr schema = queryParsingService->createSchemaFromCode(streamSchema);
    logicalSourceNameToSchemaMapping[streamName] = schema;
    return true;
}

}// namespace NES

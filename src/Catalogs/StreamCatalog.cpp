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

#include <Catalogs/LogicalStream.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <assert.h>

namespace NES {

void StreamCatalog::addDefaultStreams() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("Streamcatalog addDefaultStreams");
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    bool success = addLogicalStream("default_logical", schema);
    if (!success) {
        NES_ERROR("StreamCatalog::addDefaultStreams: error while add default_logical");
        throw Exception("Error while addDefaultStreams StreamCatalog");
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
        NES_ERROR("StreamCatalog::addDefaultStreams: error while adding exdra logical stream");
        throw Exception("Error while addDefaultStreams StreamCatalog");
    }
}
StreamCatalog::StreamCatalog() : catalogMutex() {
    NES_DEBUG("StreamCatalog: construct stream catalog");
    addDefaultStreams();
    NES_DEBUG("StreamCatalog: construct stream catalog successfully");
}

StreamCatalog::~StreamCatalog() { NES_DEBUG("~StreamCatalog:"); }

bool StreamCatalog::addLogicalStream(const std::string& streamName, const std::string& streamSchema) {
    std::unique_lock lock(catalogMutex);
    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(streamSchema);
    NES_DEBUG("StreamCatalog: schema successfully created");
    return addLogicalStream(streamName, schema);
}

bool StreamCatalog::addLogicalStream(std::string logicalStreamName, SchemaPtr schemaPtr) {
    std::unique_lock lock(catalogMutex);
    //check if stream already exist
    NES_DEBUG("StreamCatalog: search for logical stream in addLogicalStream() " << logicalStreamName);

    if (!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
        NES_DEBUG("StreamCatalog: add logical stream " << logicalStreamName);
        logicalStreamToSchemaMapping[logicalStreamName] = schemaPtr;
        return true;
    } else {
        NES_ERROR("StreamCatalog: logical stream " << logicalStreamName << " already exists");
        return false;
    }
}

bool StreamCatalog::removeLogicalStream(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: search for logical stream in removeLogicalStream() " << logicalStreamName);

    //if log stream does not exists
    if (logicalStreamToSchemaMapping.find(logicalStreamName) == logicalStreamToSchemaMapping.end()) {
        NES_ERROR("StreamCatalog: logical stream " << logicalStreamName << " does not exists");
        return false;
    } else {

        NES_DEBUG("StreamCatalog: remove logical stream " << logicalStreamName);
        if (logicalToPhysicalStreamMapping[logicalStreamName].size() != 0) {
            NES_DEBUG("StreamCatalog: cannot remove " << logicalStreamName
                                                      << " because there are physical entries for this stream");
            return false;
        }
        uint64_t cnt = logicalStreamToSchemaMapping.erase(logicalStreamName);
        NES_DEBUG("StreamCatalog: removed " << cnt << " copies of the stream");
        NES_ASSERT(!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName), "log stream should not exist");
        return true;
    }
}

// BDAPRO implement new method to create physical stream without logical stream name
// BDAPRO - discuss why bool is necessary here. i.e. what should be checked here?
bool StreamCatalog::addPhysicalStreamWithoutLogicalStreams(StreamCatalogEntryPtr newEntry) {
    // check that entry does not contain a logical stream name
    // we wont check that now since there are other adjustments in the next issues to do that excludes this.
    // BDAPRO add entry to misconfigured physical streams. - done
    misconfiguredPhysicalStreams[newEntry->getPhysicalName()] = newEntry->getNode()->getId(); // discuss whether this is the correct id.
    // BDAPRO add entry to hashIdToPhysicalStream mapping - done
    hashIdToPhysicalStream[newEntry->getNode()->getId()] = newEntry;
    return true;
}


// BDAPRO adjust to changes in StreamCatalogEntryPtr in case necessary
bool StreamCatalog::addPhysicalStream(std::string logicalStreamName, StreamCatalogEntryPtr newEntry) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: search for logical stream in addPhysicalStream() " << logicalStreamName);

    // check if logical stream exists
    if (!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
        NES_ERROR("StreamCatalog: logical stream " << logicalStreamName << " does not exists when inserting physical stream "
                                                   << newEntry->getPhysicalName());
        return false;
    } else {
        NES_DEBUG("StreamCatalog: logical stream " << logicalStreamName << " exists try to add physical stream "
                                                   << newEntry->getPhysicalName());

        //get current physical stream for this logical stream
        // BDAPRO Adjust to new logicalToPhysicalStreamMapping type

        std::vector<StreamCatalogEntryPtr> physicalStreams = logicalToPhysicalStreamMapping[logicalStreamName];

        //check if physical stream does not exist yet
        // BDAPRO adjust for loop if necessary because of type change
        for (StreamCatalogEntryPtr entry : physicalStreams) {
            NES_DEBUG("test node id=" << entry->getNode()->getId() << " phyStr=" << entry->getPhysicalName());
            NES_DEBUG("test to be inserted id=" << newEntry->getNode()->getId() << " phyStr=" << newEntry->getPhysicalName());
            if (entry->getPhysicalName() == newEntry->getPhysicalName()) {
                if (entry->getNode()->getId() == newEntry->getNode()->getId()) {
                    NES_ERROR("StreamCatalog: node with id=" << newEntry->getNode()->getId()
                                                             << " name=" << newEntry->getPhysicalName() << " already exists");
                    return false;
                }
            }
        }
    }

    NES_DEBUG("StreamCatalog: physical stream " << newEntry->getPhysicalName() << " does not exist, try to add");

    //if first one
    //BDAPRO: Adjust to new logicalToPhysicalStreamMapping type
    if (testIfLogicalStreamExistsInLogicalToPhysicalMapping(logicalStreamName)) {
        NES_DEBUG("stream already exist, just add new entry");
        logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry);
    } else {
        NES_DEBUG("stream does not exist, create new item");
        logicalToPhysicalStreamMapping.insert(
            std::pair<std::string, std::vector<StreamCatalogEntryPtr>>(logicalStreamName, std::vector<StreamCatalogEntryPtr>()));
        logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry);
    }

    NES_DEBUG("StreamCatalog: physical stream " << newEntry->getPhysicalName() << " id=" << newEntry->getNode()->getId()
                                                << " successful added");
    return true;
}

bool StreamCatalog::removeAllPhysicalStreams(std::string) {
    std::unique_lock lock(catalogMutex);
    NES_NOT_IMPLEMENTED();
}

// BDAPRO add level of indirection through physicalStreams
// rename hashID
bool StreamCatalog::removePhysicalStream(std::string logicalStreamName, std::string physicalStreamName, std::uint64_t hashId) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: search for logical stream in removePhysicalStream() " << logicalStreamName);

    // check if logical stream exists
    if (logicalStreamToSchemaMapping.find(logicalStreamName) == logicalStreamToSchemaMapping.end()) {
        NES_DEBUG("StreamCatalog: logical stream "
                  << logicalStreamName << " does not exists when trying to remove physical stream with hashId" << hashId);
        // BDAPRO add physical stream to misconfigured streams - lets discuss this.
        // misconfiguredPhysicalStreams[physicalStreamName] = hashId;
        return false;
    } else {
        NES_DEBUG("StreamCatalog: logical stream " << logicalStreamName << " exists try to remove physical stream"
                                                   << physicalStreamName << " from node " << hashId);
        for (std::vector<uint64_t>::const_iterator id =
                 logicalToPhysicalStreamMapping[logicalStreamName].cbegin();
             id != logicalToPhysicalStreamMapping[logicalStreamName].cend();
             id++) {
            StreamCatalogEntryPtr entry = hashIdToPhysicalStream[*id];
            NES_DEBUG("test node id=" << entry->getNode()->getId() << " phyStr=" << entry->getPhysicalName());
            NES_DEBUG("test to be deleted id=" << hashId << " phyStr=" << physicalStreamName);
            if (entry->getPhysicalName() == physicalStreamName) {
                NES_DEBUG("StreamCatalog: node with name=" << physicalStreamName << " exists try match hashId" << hashId);

                if (entry->getNode()->getId() == hashId) {
                    NES_DEBUG("StreamCatalog: node with id=" << hashId << " name=" << physicalStreamName
                                                             << " exists try to erase");
                    // BDAPRO add physical stream to misconfigured streams - lets discuss this.
                    misconfiguredPhysicalStreams[physicalStreamName] = hashId;
                    logicalToPhysicalStreamMapping[logicalStreamName].erase(id);
                    NES_DEBUG("StreamCatalog: number of entries afterwards "
                              << logicalToPhysicalStreamMapping[logicalStreamName].size());
                    return true;
                }
            }
        }
        NES_DEBUG("StreamCatalog: physical stream " << physicalStreamName << " does not exist on node with id" << hashId
                                                    << " and with logicalStreamName " << logicalStreamName);
    }
    NES_DEBUG("StreamCatalog: physical stream " << physicalStreamName << " does not exist on node with id" << hashId);
    return false;
}

// BDAPRO add level of indirection through physicalStreams
// care to remove from both data structures
bool StreamCatalog::removePhysicalStreamByHashId(uint64_t hashId) {
    std::unique_lock lock(catalogMutex);
    // BDAPRO remove from those associated to logical Streams - this can be multiple mappings -done
    for (auto logStream : logicalToPhysicalStreamMapping) { // these are single mappings
        NES_DEBUG("StreamCatalog: check log stream " << logStream.first);
        for (std::vector<uint64_t>::const_iterator id = logicalToPhysicalStreamMapping[logStream.first].cbegin();
            id != logicalToPhysicalStreamMapping[logStream.first].cend();
            id++) {
            if (*id == hashId){
                NES_DEBUG("StreamCatalog: found entry with nodeid=" << *id
                                                                    << " physicalStream=" << " " // BDAPRO name necessary?
                                                                    << " logicalStream=" << logStream.first);
                logicalToPhysicalStreamMapping[logStream.first].erase(id);
                NES_DEBUG("StreamCatalog: deleted physical stream with hashID" << hashId << " from StreamCatalog"); // BDAPRO name necessary?

                // BDAPRO remove those from hashIdToPhysicalStream -done
                hashIdToPhysicalStream.erase(hashId);
                return true
            }
        }
    }
    // BDAPRO remove additionally from those not associated to logical streams i.e. free roaming physical streams. -done
    // check if existent. -done
    if(std::find(hashIdToPhysicalStream.begin(), hashIdToPhysicalStream.end(),hashId)!=hashIdToPhysicalStream.end()){
        hashIdToPhysicalStream.erase(hashId);
        return true
    }
    return false;
}

SchemaPtr StreamCatalog::getSchemaForLogicalStream(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return logicalStreamToSchemaMapping[logicalStreamName];
}

LogicalStreamPtr StreamCatalog::getStreamForLogicalStream(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return std::make_shared<LogicalStream>(logicalStreamName, logicalStreamToSchemaMapping[logicalStreamName]);
}

LogicalStreamPtr StreamCatalog::getStreamForLogicalStreamOrThrowException(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    if (logicalStreamToSchemaMapping.find(logicalStreamName) != logicalStreamToSchemaMapping.end()) {
        return std::make_shared<LogicalStream>(logicalStreamName, logicalStreamToSchemaMapping[logicalStreamName]);
    } else {
        NES_ERROR("StreamCatalog::getStreamForLogicalStreamOrThrowException: stream does not exists " << logicalStreamName);
        throw Exception("Required stream does not exists " + logicalStreamName);
    }
}

bool StreamCatalog::testIfLogicalStreamExistsInSchemaMapping(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return logicalStreamToSchemaMapping.find(logicalStreamName)//if log stream does not exists
        != logicalStreamToSchemaMapping.end();
}
bool StreamCatalog::testIfLogicalStreamExistsInLogicalToPhysicalMapping(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return logicalToPhysicalStreamMapping.find(logicalStreamName)//if log stream does not exists
        != logicalToPhysicalStreamMapping.end();
}

// BDAPRO add level of indirection through physicalStreams - done
std::vector<TopologyNodePtr> StreamCatalog::getSourceNodesForLogicalStream(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    std::vector<TopologyNodePtr> listOfSourceNodes;

    //get current physical stream for this logical stream
    // BDAPRO change this to multiple physicalStreams for a logical stream - with indirection. - done
    std::vector<std::uint64_t> physicalStreamsHashIds = logicalToPhysicalStreamMapping[logicalStreamName];

    // BDAPRO loop over vector to get StreamCatalogEntryPtrs - done
    std::vector<StreamCatalogEntryPtr> physicalStreams;
    for (auto id : physicalStreamsHashIds){
        physicalStreams.push_back(hashIdToPhysicalStream[id]);
    }

    if (physicalStreams.empty()) {
        return listOfSourceNodes;
    }

    for (StreamCatalogEntryPtr entry : physicalStreams) {
        listOfSourceNodes.push_back(entry->getNode());
    }

    return listOfSourceNodes;
}

bool StreamCatalog::reset() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: reset Stream Catalog");
    logicalStreamToSchemaMapping.clear();
    logicalToPhysicalStreamMapping.clear();

    addDefaultStreams();
    NES_DEBUG("StreamCatalog: reset Stream Catalog completed");
    return true;
}

// BDAPRO needs adjustment : logicalToPhysicalStreamMapping no longer holds StreamCatalogEntryPtr but hashid as uint64_t
std::string StreamCatalog::getPhysicalStreamAndSchemaAsString() {
    std::unique_lock lock(catalogMutex);
    std::stringstream ss;
    for (auto entry : logicalToPhysicalStreamMapping) {
        ss << "stream name=" << entry.first << " with " << entry.second.size() << " elements:";
        for (StreamCatalogEntryPtr sce : entry.second) {
            ss << sce->toString();
        }
        ss << std::endl;
    }
    return ss.str();
}

// BDAPRO add level of indirection through physicalStreams - done
std::vector<StreamCatalogEntryPtr> StreamCatalog::getPhysicalStreams(std::string logicalStreamName) {
    std::vector<std::uint64_t> physicalStreamsHashIds = logicalToPhysicalStreamMapping[logicalStreamName];
    std::vector<StreamCatalogEntryPtr> physicalStreams;

    // Iterate over all hashIDs and retrieve respective StreamCatalogEntryPtr
    for (auto id : physicalStreamsHashIds){
        physicalStreams.push_back(hashIdToPhysicalStream[id]);
    }
    return physicalStreams;
}

std::map<std::string, SchemaPtr> StreamCatalog::getAllLogicalStream() { return logicalStreamToSchemaMapping; }

std::map<std::string, std::string> StreamCatalog::getAllLogicalStreamAsString() {
    std::unique_lock lock(catalogMutex);
    std::map<std::string, std::string> allLogicalStreamAsString;
    const std::map<std::string, SchemaPtr> allLogicalStream = getAllLogicalStream();

    for (auto const& [key, val] : allLogicalStream) {
        allLogicalStreamAsString[key] = val->toString();
    }
    return allLogicalStreamAsString;
}

bool StreamCatalog::updatedLogicalStream(std::string& streamName, std::string& streamSchema) {
    NES_INFO("StreamCatalog: Update the logical stream " << streamName << " with the schema " << streamSchema);
    std::unique_lock lock(catalogMutex);

    NES_TRACE("StreamCatalog: Check if logical stream exists in the catalog.");
    if (!testIfLogicalStreamExistsInSchemaMapping(streamName)) {
        NES_ERROR("StreamCatalog: Unable to find logical stream " << streamName << " to update.");
        return false;
    }

    NES_TRACE("StreamCatalog: create a new schema object and add to the catalog");
    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(streamSchema);
    logicalStreamToSchemaMapping[streamName] = schema;
    return true;
}

}// namespace NES

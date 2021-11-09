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
#include <Configurations/Persistence/DefaultStreamCatalogPersistence.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>
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

StreamCatalog::StreamCatalog() : StreamCatalog(DefaultStreamCatalogPersistence::create(), nullptr){};

StreamCatalog::StreamCatalog(StreamCatalogPersistencePtr persistence, WorkerRPCClientPtr workerRpcClient)
    : catalogMutex(), persistence(persistence), workerRpcClient(workerRpcClient) {
    NES_DEBUG("StreamCatalog: construct stream catalog");
    addDefaultStreams();
    NES_DEBUG("StreamCatalog: construct stream catalog successfully");
}

StreamCatalog::~StreamCatalog() { NES_DEBUG("~StreamCatalog:"); }

bool StreamCatalog::addLogicalStream(const std::string& streamName, const std::string& streamSchema) {
    std::unique_lock lock(catalogMutex);
    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(streamSchema);
    NES_DEBUG("StreamCatalog: schema successfully created");
    bool success = addLogicalStream(streamName, schema);
    if (success) {
        persistence->persistLogicalStream(streamName, streamSchema);
    }
    return success;
}

bool StreamCatalog::addLogicalStream(std::string logicalStreamName, SchemaPtr schemaPtr) {
    std::unique_lock lock(catalogMutex);
    //check if stream already exist
    NES_DEBUG("StreamCatalog: search for logical stream in addLogicalStream() " << logicalStreamName);
    if (testIfLogicalStreamExistsInMismappedStreams(logicalStreamName)) {
        NES_DEBUG("StreamCatalog: logical stream " << logicalStreamName << " exists in mismapping.");
        addLogicalStreamFromMismappedStreams(logicalStreamName, schemaPtr);
        return true;
    }
    if (!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
        NES_DEBUG("StreamCatalog: add logical stream " << logicalStreamName);
        logicalStreamToSchemaMapping[logicalStreamName] = schemaPtr;
        return true;
    } else {
        NES_ERROR("StreamCatalog: logical stream " << logicalStreamName << " already exists");
        return false;
    }
}

void StreamCatalog::addLogicalStreamFromMismappedStreams(std::string logicalStreamName, SchemaPtr schemaPtr) {
    std::unique_lock lock(catalogMutex);

    NES_DEBUG("StreamCatalog: add logical stream " << logicalStreamName);
    logicalStreamToSchemaMapping[logicalStreamName] = schemaPtr;

    NES_DEBUG("StreamCatalog: remove logical stream " << logicalStreamName << " from mismapped Streams.");
    logicalToPhysicalStreamMapping[logicalStreamName] = mismappedStreams[logicalStreamName];
    mismappedStreams.erase(logicalStreamName);
    NES_DEBUG("StreamCatalog: physical streams of logical stream transferred to REGULAR logical to physical mapping.");

    NES_DEBUG("StreamCatalog: updating state of physical streams involved.");
    for (std::string& physicalStreamName : logicalToPhysicalStreamMapping[logicalStreamName]) {
        nameToPhysicalStream[physicalStreamName]->moveLogicalStreamFromMismappedToRegular(logicalStreamName);
    }
}

std::tuple<bool, std::string> StreamCatalog::addPhysicalStreamWithoutLogicalStreams(StreamCatalogEntryPtr newEntry) {
    std::unique_lock lock(catalogMutex);
    if (nameToPhysicalStream.find(newEntry->getPhysicalName()) != nameToPhysicalStream.end()) {
        std::string newName = newEntry->getPhysicalName() + "_" + UtilityFunctions::simpleHexStringGenerator(10);
        while (nameToPhysicalStream.find(newName) != nameToPhysicalStream.end()) {
            newName = newEntry->getPhysicalName() + "_" + UtilityFunctions::simpleHexStringGenerator(10);
        }
        NES_WARNING("StreamCatalog: physicalStream with name="
                    << newEntry->getPhysicalName() << " already exists. Change it to " << newName
                    << ", new name has to be validated before querying on this Node is allowed.");
        newEntry->changePhysicalStreamName(newName);
    }
    nameToPhysicalStream[newEntry->getPhysicalName()] = newEntry;
    nameToPhysicalStream[newEntry->getPhysicalName()]->setStateToWithoutLogicalStream();
    return {true, newEntry->getPhysicalName()};
}

bool StreamCatalog::addPhysicalStreamToLogicalStream(std::string logicalStreamName, StreamCatalogEntryPtr newEntry) {
    NES_DEBUG("StreamCatalog: logical stream " << logicalStreamName << " exists try to add physical stream "
                                               << newEntry->getPhysicalName());
    //get current physicalStreamNames for this logicalStream
    std::vector<std::string> physicalStreams = logicalToPhysicalStreamMapping[logicalStreamName];
    //get corresponding StreamCatalogEntries

    std::vector<StreamCatalogEntryPtr> entries;
    for (std::string physicalStreamName : physicalStreams) {
        NES_INFO("PUSH BACK");
        entries.push_back(nameToPhysicalStream[physicalStreamName]);
    }

    //if first one
    if (testIfLogicalStreamExistsInLogicalToPhysicalMapping(logicalStreamName)) {
        NES_DEBUG("stream already exist, just add new entry");
        logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry->getPhysicalName());
        nameToPhysicalStream[newEntry->getPhysicalName()] = newEntry;
    } else {
        NES_DEBUG("stream does not exist, create new item");
        logicalToPhysicalStreamMapping.insert(
            std::pair<std::string, std::vector<std::string>>(logicalStreamName, std::vector<std::string>()));
        logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry->getPhysicalName());
        nameToPhysicalStream[newEntry->getPhysicalName()] = newEntry;
    }

    NES_DEBUG("StreamCatalog: physical stream " << newEntry->getPhysicalName() << " id=" << newEntry->getNode()->getId()
                                                << " successful added");

    return true;
}

std::tuple<bool, std::string> StreamCatalog::addPhysicalStream(std::vector<std::string> logicalStreamNames,
                                                               StreamCatalogEntryPtr newEntry) {
    NES_DEBUG("StreamCatalog: acquiring lock");
    std::unique_lock lock(catalogMutex);
    const auto& phyStreamName = newEntry->getPhysicalName();

    //check if physical stream does not exist yet
    if (nameToPhysicalStream.find(newEntry->getPhysicalName()) != nameToPhysicalStream.end()) {
        std::string newName = phyStreamName + "_" + UtilityFunctions::simpleHexStringGenerator(10);
        while (nameToPhysicalStream.find(newName) != nameToPhysicalStream.end()) {
            newName = phyStreamName + "_" + UtilityFunctions::simpleHexStringGenerator(10);
        }
        NES_WARNING("StreamCatalog: physicalStream with name="
                    << newEntry->getPhysicalName() << " already exists. Change it to " << newName
                    << ", new name has to be validated before querying on this Node is allowed.");
        newEntry->changePhysicalStreamName(newName);
        nameToPhysicalStream[newName] = newEntry;
        return {true, newName};
    }

    NES_DEBUG("StreamCatalog: physical stream " << newEntry->getPhysicalName() << " does not exist, try to add");

    NES_DEBUG("StreamCatalog: search for logical streams in addPhysicalStream() "
              << UtilityFunctions::combineStringsWithDelimiter(logicalStreamNames, ","));

    // check if logical stream schemas exist
    auto inAndExclude = testIfLogicalStreamVecExistsInSchemaMapping(logicalStreamNames);

    // Handle logicalStream names which were not found in schema mapping
    for (std::string notFound : std::get<1>(inAndExclude)) {
        NES_WARNING("StreamCatalog: logical stream " << notFound << " does not exists when inserting physical stream "
                                                     << newEntry->getPhysicalName());
        nameToPhysicalStream[phyStreamName] = newEntry;
        nameToPhysicalStream[phyStreamName]->removeLogicalStream(notFound);
        nameToPhysicalStream[phyStreamName]->addLogicalStreamNameToMismapped(notFound);
        mismappedStreams[notFound].push_back(phyStreamName);
    }
    // Handle logicalStream names which were found in schema mapping
    for (std::string found : std::get<0>(inAndExclude)) {
        addPhysicalStreamToLogicalStream(found, newEntry);
    }
    return {true, phyStreamName};
}

bool StreamCatalog::addPhysicalStreamToLogicalStream(std::string physicalStreamName, std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);

    if (!testIfPhysicalStreamWithNameExists(physicalStreamName)) {
        NES_ERROR("StreamCatalog: addPhysicalStreamToLogicalStream - physicalStreamName" << physicalStreamName
                                                                                         << " does not exists.");
        return false;
    }

    if (!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
        NES_WARNING("StreamCatalog: addPhysicalStreamToLogicalStream - logicalStreamName"
                    << logicalStreamName
                    << " does not have a registered schema.\n "
                       "Add physicalStream "
                    << physicalStreamName << "to logical in separate MISCONFIGURED mapping.");
        for (std::string& logStream : nameToPhysicalStream[physicalStreamName]->getMissmappedLogicalName()) {
            if (logStream == logicalStreamName) {
                NES_ERROR("StreamCatalog: addPhysicalStreamToLogicalStream - mismapping already exists");
                return false;
            }
        }
        mismappedStreams[logicalStreamName].push_back(physicalStreamName);
        nameToPhysicalStream[physicalStreamName]->addLogicalStreamNameToMismapped(logicalStreamName);
        try {
            std::vector<std::string> logicalStreams{};
            nameToPhysicalStream[physicalStreamName]->getAllLogicalName(logicalStreams);
            bool success = setLogicalStreamsOnWorker(physicalStreamName, logicalStreams);
            if (!success) {
                NES_ERROR("StreamCatalog: failed to set logical streams on worker node");
            }
            return success;
        } catch (std::exception& ex) {
            NES_ERROR("StreamCatalog: could not propagate to worker node: " << ex.what());
            return false;
        }
        return false;
    }

    // check if physical stream to logical stream mapping already exists
    for (std::string& logStream : nameToPhysicalStream[physicalStreamName]->getLogicalNames()) {
        if (logStream == logicalStreamName) {
            NES_ERROR("StreamCatalog: addPhysicalStreamToLogicalStream - mapping already exists");
            return false;
        }
    }

    if (testIfLogicalStreamExistsInLogicalToPhysicalMapping(logicalStreamName)) {
        NES_DEBUG("stream already exist, just add new entry");
        logicalToPhysicalStreamMapping[logicalStreamName].push_back(physicalStreamName);
    } else {
        NES_DEBUG("stream does not exist, create new item");
        logicalToPhysicalStreamMapping.insert(
            std::pair<std::string, std::vector<std::string>>(logicalStreamName, std::vector<std::string>()));
        logicalToPhysicalStreamMapping[logicalStreamName].push_back(physicalStreamName);
    }
    nameToPhysicalStream[physicalStreamName]->addLogicalStreamName(logicalStreamName);

    try {
        std::vector<std::string> logicalStreams{};
        nameToPhysicalStream[physicalStreamName]->getAllLogicalName(logicalStreams);
        bool success = setLogicalStreamsOnWorker(physicalStreamName, logicalStreams);
        if (!success) {
            NES_ERROR("StreamCatalog: failed to set logical streams on worker node");
        }
        return success;
    } catch (std::exception& ex) {
        NES_ERROR("StreamCatalog: could not propagate to worker node: " << ex.what());
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
        NES_DEBUG("StreamCatalog: delete logical stream " << logicalStreamName);
        if (logicalToPhysicalStreamMapping[logicalStreamName].size() != 0) {
            NES_DEBUG("StreamCatalog: cannot delete " << logicalStreamName
                                                      << " because there are physical entries for this stream");
            return false;
        }
        uint64_t cnt = logicalStreamToSchemaMapping.erase(logicalStreamName);
        NES_DEBUG("StreamCatalog: deleted " << cnt << " copies of the stream");
        NES_ASSERT(!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName), "log stream should not exist");
        persistence->deleteLogicalStream(logicalStreamName);
        return true;
    }
}

bool StreamCatalog::removePhysicalStreamFromLogicalStream(std::string physicalStreamName, std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: Try to remove " << physicalStreamName << " from logical stream " << logicalStreamName);
    if (!testIfPhysicalStreamWithNameExists(physicalStreamName)) {
        NES_ERROR("StreamCatalog: physical stream " << physicalStreamName << " does not exist");
        return false;
    }
    bool exists = false;
    if (!testIfLogicalStreamExistsInLogicalToPhysicalMapping(logicalStreamName)) {
        NES_DEBUG("StreamCatalog: logical stream " << logicalStreamName << " does not have any physical streams.");
        NES_DEBUG("StreamCatalog: In mismappedStreams mapping - try to remove " << physicalStreamName << " from logical stream "
                                                                                << logicalStreamName);
        if (testIfLogicalStreamToPhysicalStreamMappingExistsInMismappedStreams(logicalStreamName, physicalStreamName)) {
            removePhysicalToLogicalMappingFromMismappedStreams(logicalStreamName, physicalStreamName);
            NES_DEBUG("StreamCatalog: In mismappedStreams mapping - physical stream removed from logical.");
        } else {
            return false;
        }
    } else {
        exists = true;
        std::vector<std::string>& phyStreams = logicalToPhysicalStreamMapping[logicalStreamName];
        auto it = std::remove(phyStreams.begin(), phyStreams.end(), physicalStreamName);
        phyStreams.erase(it, phyStreams.end());
        nameToPhysicalStream[physicalStreamName]->removeLogicalStream(logicalStreamName);
        NES_DEBUG("StreamCatalog: number of entries for logical stream "
                  << logicalStreamName << " afterwards " << logicalToPhysicalStreamMapping[logicalStreamName].size());
    }
    try {
        std::vector<std::string> logicalStreams{};
        nameToPhysicalStream[physicalStreamName]->getAllLogicalName(logicalStreams);
        bool success = setLogicalStreamsOnWorker(physicalStreamName, logicalStreams);
        if (!success) {
            NES_ERROR("StreamCatalog: failed to set logical streams on worker node");
        }
        return exists && success;
    } catch (std::exception& ex) {
        NES_ERROR("StreamCatalog: could not propagate to worker node: " << ex.what());
        return false;
    }
}

bool StreamCatalog::removePhysicalToLogicalMappingFromMismappedStreams(std::string logicalStreamName,
                                                                       std::string physicalStreamName) {
    std::unique_lock lock(catalogMutex);
    std::vector<std::string>& phyStreams = mismappedStreams[logicalStreamName];
    auto it = std::remove(phyStreams.begin(), phyStreams.end(), physicalStreamName);
    phyStreams.erase(it, phyStreams.end());
    if (mismappedStreams[logicalStreamName].empty()) {
        mismappedStreams.erase(logicalStreamName);
    }
    nameToPhysicalStream[physicalStreamName]->removeLogicalStreamFromMismapped(logicalStreamName);
    return true;
}

bool StreamCatalog::removeAllMismappings(std::string logicalStreamName) {
    if (mismappedStreams.find(logicalStreamName) == mismappedStreams.end()) {
        NES_ERROR("StreamCatalog: removeAllMismappings - logical stream " + logicalStreamName + " does not exist in mismappings.");
        return false;
    }
    bool failed = false;
    for (std::string& phyStream : mismappedStreams[logicalStreamName]) {
        nameToPhysicalStream[phyStream]->removeLogicalStreamFromMismapped(logicalStreamName);
        try {
            std::vector<std::string> logicalStreams{};
            nameToPhysicalStream[phyStream]->getAllLogicalName(logicalStreams);
            bool success = setLogicalStreamsOnWorker(phyStream, logicalStreams);
            if (!success) {
                NES_ERROR("StreamCatalog: failed to set logical streams on worker node");
                failed = true;
            }
        } catch (std::exception& ex) {
            NES_ERROR("StreamCatalog: could not propagate to worker node: " << ex.what());
            failed = true;
        }
    }
    mismappedStreams.erase(logicalStreamName);
    return !failed;
}

bool StreamCatalog::unregisterPhysicalStreamByHashId(uint64_t hashId) {
    std::unique_lock lock(catalogMutex);
    bool found = false;

    for (auto phyStream : nameToPhysicalStream) {
        if (phyStream.second->getNode()->getId() != hashId) {
            continue;
        }
        auto mismappedLogs = phyStream.second->getMissmappedLogicalName();
        auto logStreams = phyStream.second->getLogicalNames();

        //TODO: change to batch delete
        for (auto mis : mismappedLogs) {
            std::vector<std::string>& misStreams = mismappedStreams[mis];
            auto it = std::remove(misStreams.begin(), misStreams.end(), phyStream.first);
            misStreams.erase(it, misStreams.end());
            if (misStreams.empty()) {
                mismappedStreams.erase(mis);
            }
        }
        for (auto reg : logStreams) {
            std::vector<std::string>& logStreams = logicalToPhysicalStreamMapping[reg];
            auto it = std::remove(logStreams.begin(), logStreams.end(), phyStream.first);
            logStreams.erase(it, logStreams.end());
            if (logStreams.empty()) {
                logicalToPhysicalStreamMapping.erase(reg);
            }
        }
        nameToPhysicalStream.erase(phyStream.first);
    }
    return found;
}

bool StreamCatalog::unregisterPhysicalStream(std::string physicalStreamName, std::uint64_t hashId) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: Try to delete " << physicalStreamName << " from Node with id=" << hashId);
    if (!testIfPhysicalStreamWithNameExists(physicalStreamName)) {
        NES_ERROR("StreamCatalog: physical stream '" << physicalStreamName << "' does not exist.");
        return false;
    }
    StreamCatalogEntryPtr entry = nameToPhysicalStream[physicalStreamName];
    if (hashId != entry->getNode()->getId()) {
        NES_ERROR("StreamCatalog: Node Id does not align with Node Id of physical stream");
        return false;
    }
    bool success = unregisterPhysicalStreamFromAllLogicalStreams(physicalStreamName);

    if (success) {
        NES_DEBUG("StreamCatalog: deleting physicalStream from nameToPhysicalStream mapping");
        nameToPhysicalStream.erase(physicalStreamName);
        NES_DEBUG("StreamCatalog: number of entries afterwards " << nameToPhysicalStream.size());
    }
    return success;
}

bool StreamCatalog::unregisterPhysicalStreamFromAllLogicalStreams(std::string physicalStreamName) {
    StreamCatalogEntryPtr entry = nameToPhysicalStream[physicalStreamName];
    auto logicalStreamNames = entry->getLogicalNames();
    NES_DEBUG("StreamCatalog: Try to remove physical stream "
              << physicalStreamName
              << " from logical streams: " << UtilityFunctions::combineStringsWithDelimiter(logicalStreamNames, ", "));
    for (std::string logicalStreamName : logicalStreamNames) {
        NES_DEBUG("StreamCatalog: Starting to remove logicalStreamName=" << logicalStreamName);

        std::vector<std::string>& phyStreams = logicalToPhysicalStreamMapping[logicalStreamName];
        auto it = std::remove(phyStreams.begin(), phyStreams.end(), physicalStreamName);
        phyStreams.erase(it, phyStreams.end());

        NES_DEBUG("StreamCatalog: number of entries for logical stream "
                  << logicalStreamName << " afterwards " << logicalToPhysicalStreamMapping[logicalStreamName].size());
    }
    auto mismappedLogicalStreamNames = entry->getMissmappedLogicalName();
    if (!mismappedLogicalStreamNames.empty()) {
        NES_DEBUG("StreamCatalog: Try to remove physical stream "
                  << physicalStreamName << " from mismapped logical streams: "
                  << UtilityFunctions::combineStringsWithDelimiter(mismappedLogicalStreamNames, ", "));
        for (std::string logicalStreamName : logicalStreamNames) {
            NES_DEBUG("StreamCatalog: Starting to remove logicalStreamName=" << logicalStreamName);

            std::vector<std::string>& phyStreams = mismappedStreams[logicalStreamName];
            auto it = std::remove(phyStreams.begin(), phyStreams.end(), physicalStreamName);
            phyStreams.erase(it, phyStreams.end());

            NES_DEBUG("StreamCatalog: number of entries for mismapped logical stream "
                      << logicalStreamName << " afterwards " << mismappedStreams[logicalStreamName].size());
        }
    }

    NES_DEBUG("StreamCatalog: removed physical stream from all logical streams");
    return true;
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

std::tuple<std::vector<std::string>, std::vector<std::string>>
StreamCatalog::testIfLogicalStreamVecExistsInSchemaMapping(std::vector<std::string>& logicalStreamNames) {
    std::vector<std::string> included;
    std::vector<std::string> excluded;

    for (std::string logicalStreamName : logicalStreamNames) {
        if (StreamCatalog::testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
            included.push_back(logicalStreamName);
        } else {
            excluded.push_back(logicalStreamName);
        }
    }

    return std::make_tuple(included, excluded);
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

// checks if the corresponding physical stream exists in nameToPhysicalStream Mapping.
bool StreamCatalog::testIfPhysicalStreamWithNameExists(std::string physicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return nameToPhysicalStream.find(physicalStreamName) != nameToPhysicalStream.end();
}

bool StreamCatalog::testIfLogicalStreamExistsInMismappedStreams(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    return mismappedStreams.find(logicalStreamName) != mismappedStreams.end();
}

bool StreamCatalog::testIfLogicalStreamToPhysicalStreamMappingExistsInMismappedStreams(std::string logicalStreamName,
                                                                                       std::string physicalStreamName) {
    std::unique_lock lock(catalogMutex);
    if (!testIfLogicalStreamExistsInMismappedStreams(logicalStreamName)) {
        return false;
    }
    return std::find(mismappedStreams[logicalStreamName].begin(), mismappedStreams[logicalStreamName].end(), physicalStreamName)
        != mismappedStreams[logicalStreamName].end();
}

std::vector<TopologyNodePtr> StreamCatalog::getSourceNodesForLogicalStream(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    std::vector<TopologyNodePtr> listOfSourceNodes;

    //get current physical stream for this logical stream
    std::vector<std::string> physicalStreamNames = logicalToPhysicalStreamMapping[logicalStreamName];

    std::vector<StreamCatalogEntryPtr> physicalStreams;
    for (auto name : physicalStreamNames) {
        physicalStreams.push_back(nameToPhysicalStream[name]);
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
    nameToPhysicalStream.clear();
    mismappedStreams.clear();

    addDefaultStreams();
    NES_DEBUG("StreamCatalog: reset Stream Catalog completed");
    return true;
}

std::string StreamCatalog::getPhysicalStreamAndSchemaAsString() {
    std::unique_lock lock(catalogMutex);
    std::stringstream ss;
    for (auto entry : logicalToPhysicalStreamMapping) {
        ss << "stream name=" << entry.first << " with " << entry.second.size() << " elements:";
        for (std::string physicalStreamName : entry.second) {
            ss << nameToPhysicalStream[physicalStreamName]->toString();
        }
        ss << std::endl;
    }
    return ss.str();
}
std::vector<StreamCatalogEntryPtr> StreamCatalog::getPhysicalStreams() {
    std::unique_lock lock(catalogMutex);
    std::vector<StreamCatalogEntryPtr> physicalStreams;
    for (auto entry : nameToPhysicalStream) {
        // returned a list of physicalStreamNames
        physicalStreams.push_back(entry.second);
    }
    return physicalStreams;
}

std::vector<StreamCatalogEntryPtr> StreamCatalog::getPhysicalStreams(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    std::vector<std::string> physicalStreamsNames = logicalToPhysicalStreamMapping[logicalStreamName];
    std::vector<StreamCatalogEntryPtr> physicalStreams;

    // Iterate over all hashIDs and retrieve respective StreamCatalogEntryPtr
    for (auto physicalStreamName : physicalStreamsNames) {
        physicalStreams.push_back(nameToPhysicalStream[physicalStreamName]);
    }
    return physicalStreams;
}
std::map<std::string, std::vector<std::string>> StreamCatalog::getMismappedPhysicalStreams() { return mismappedStreams; }

std::vector<std::string> StreamCatalog::getMismappedPhysicalStreams(std::string logicalStreamName) {
    std::unique_lock lock(catalogMutex);
    std::vector<std::string> physicalStreamsNames{};
    if (mismappedStreams.find(logicalStreamName) != mismappedStreams.end()) {
        return physicalStreamsNames = mismappedStreams[logicalStreamName];
    }
    return physicalStreamsNames;
}

std::vector<StreamCatalogEntryPtr> StreamCatalog::getAllMisconfiguredPhysicalStreams() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("StreamCatalog: getting all MISCONFIGURED physical streams");
    std::vector<StreamCatalogEntryPtr> streamCatalogEntryPtrs{};
    for (auto entry : nameToPhysicalStream) {
        if (entry.second->getPhysicalStreamState().state != REGULAR) {
            streamCatalogEntryPtrs.push_back(entry.second);
        }
    }
    return streamCatalogEntryPtrs;
}

std::map<std::string, SchemaPtr> StreamCatalog::getAllLogicalStream() { return logicalStreamToSchemaMapping; }

std::map<std::string, SchemaPtr> StreamCatalog::getAllLogicalStreamForPhysicalStream(std::string physicalStreamName) {
    std::unique_lock lock(catalogMutex);
    std::vector<std::string> logicalStreamNames = nameToPhysicalStream[physicalStreamName]->getLogicalNames();
    std::map<std::string, std::string> allLogicalStreamAsString;
    const std::map<std::string, SchemaPtr> allLogicalStream = getAllLogicalStream();
    // filter this now according to logicalStreamNames
    std::map<std::string, SchemaPtr> allLogicalStreamForPhysicalStream;
    for (auto name : logicalStreamNames) {
        allLogicalStreamForPhysicalStream.insert({name, allLogicalStream.find(name)->second});
    }
    return allLogicalStreamForPhysicalStream;
}

std::map<std::string, std::string> StreamCatalog::getAllLogicalStreamForPhysicalStreamAsString(std::string physicalStreamName) {
    std::unique_lock lock(catalogMutex);
    if (!testIfPhysicalStreamWithNameExists(physicalStreamName)) {
        NES_ERROR("StreamCatalog: Unable to find physical stream " << physicalStreamName
                                                                   << " to return logical streams for. Returning empty list");
        std::map<std::string, std::string> map;
        return map;
    }
    std::map<std::string, SchemaPtr> allLogicalStreamForPhysicalStream = getAllLogicalStreamForPhysicalStream(physicalStreamName);
    std::map<std::string, std::string> allLogicalStreamForPhysicalStreamAsString;
    for (auto const& [key, val] : allLogicalStreamForPhysicalStream) {
        allLogicalStreamForPhysicalStreamAsString[key] = val->toString();
    }
    return allLogicalStreamForPhysicalStreamAsString;
}

std::map<std::string, std::string> StreamCatalog::getAllLogicalStreamAsString() {
    std::unique_lock lock(catalogMutex);
    std::map<std::string, std::string> allLogicalStreamAsString;
    const std::map<std::string, SchemaPtr> allLogicalStream = getAllLogicalStream();

    for (auto const& [key, val] : allLogicalStream) {
        allLogicalStreamAsString[key] = val->toString();
    }
    return allLogicalStreamAsString;
}

std::tuple<bool, std::string> StreamCatalog::getSourceConfig(const std::string& physicalStreamName) {
    if (!workerRpcClient) {
        NES_ERROR("StreamCatalog: set source config failed, no workerRpcClient supplied!");
        return {false, ""};
    }
    NES_INFO("StreamCatalog: get source config, physicalStreamName=" << physicalStreamName);

    if (nameToPhysicalStream.find(physicalStreamName) == nameToPhysicalStream.end()) {
        NES_ERROR("StreamCatalog: unable to find stream physicalStreamName=" << physicalStreamName);
        throw Exception("Unknown physical stream " + physicalStreamName);
    }

    auto physicalStream = nameToPhysicalStream[physicalStreamName];
    auto node = physicalStream->getNode();
    auto ipAddress = node->getIpAddress();
    auto grpcPort = node->getGrpcPort();
    std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);

    try {
        return workerRpcClient->getSourceConfig(rpcAddress, physicalStreamName);
    } catch (std::exception& ex) {
        NES_ERROR("StreamCatalog: error on rpc call to retrieve source config, physicalStreamName=" << physicalStreamName);
        throw Exception("Could not retrieve source config for physicalStream " + physicalStreamName);
    }
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

bool StreamCatalog::validatePhysicalStreamName(std::string physicalStreamName) {
    NES_DEBUG("StreamCatalog: trying to validate physical stream with new name " + physicalStreamName);
    std::unique_lock lock(catalogMutex);

    if (nameToPhysicalStream.find(physicalStreamName) != nameToPhysicalStream.end()) {
        if (!nameToPhysicalStream[physicalStreamName]->getPhysicalStreamState().isNameValid()) {
            auto& phyStream = nameToPhysicalStream[physicalStreamName];
            std::vector<std::string> logStreams = phyStream->getLogicalNames();

            // check if logical stream schemas exist
            auto inAndExclude = testIfLogicalStreamVecExistsInSchemaMapping(logStreams);

            // Handle logicalStream names which were not found in schema mapping
            for (std::string notFound : std::get<1>(inAndExclude)) {
                NES_WARNING("StreamCatalog: logical stream " << notFound << " does not exists when inserting physical stream "
                                                             << physicalStreamName);
                phyStream->removeLogicalStream(notFound);
                phyStream->addLogicalStreamNameToMismapped(notFound);
                mismappedStreams[notFound].push_back(physicalStreamName);
            }
            // Handle logicalStream names which were found in schema mapping
            for (std::string found : std::get<0>(inAndExclude)) {
                addPhysicalStreamToLogicalStream(found, phyStream);
            }

            phyStream->getPhysicalStreamState().removeReason(DUPLICATE_PHYSICAL_STREAM_NAME);
            NES_DEBUG("StreamCatalog: physical stream with name " + physicalStreamName + " is now valid.");
            return true;
        }
        NES_DEBUG("StreamCatalog: physical stream with name " + physicalStreamName + " is already valid.");
        return false;
    }
    NES_DEBUG("StreamCatalog: physical stream with given name does not exist");
    return false;
}

bool StreamCatalog::setLogicalStreamsOnWorker(const std::string& physicalStreamName,
                                              const std::vector<std::string>& logicalStreamNames) {
    if (!workerRpcClient) {
        NES_WARNING(
            "StreamCatalog::addPhysicalStreamToLogicalStream: could not propagate update to worker: workerRpcClient no supplied");
        return true;
    }
    NES_INFO("StreamCatalog: set logical streams config, physicalStreamName=" << physicalStreamName);

    if (nameToPhysicalStream.find(physicalStreamName) == nameToPhysicalStream.end()) {
        NES_ERROR("StreamCatalog: unable to find stream physicalStreamName=" << physicalStreamName);
        return false;
    }

    auto physicalStream = nameToPhysicalStream[physicalStreamName];
    auto node = physicalStream->getNode();
    auto ipAddress = node->getIpAddress();
    auto grpcPort = node->getGrpcPort();
    std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);

    try {
        return workerRpcClient->setLogicalStreams(rpcAddress, physicalStreamName, logicalStreamNames);
    } catch (std::exception& ex) {
        NES_ERROR("StreamCatalog: error on rpc call to retrieve source config, physicalStreamName=" << physicalStreamName);
        throw Exception("Could not retrieve source config for physicalStream " + physicalStreamName);
    }
}

std::tuple<bool, std::string> StreamCatalog::setSourceConfig(const std::string& physicalStreamName,
                                                             const std::string& configString) {
    if (!workerRpcClient) {
        NES_ERROR("StreamCatalog: set source config failed, no workerRpcClient supplied!");
        return {false, ""};
    }
    NES_INFO("StreamCatalog: set source config, physicalStreamName=" << physicalStreamName);

    if (nameToPhysicalStream.find(physicalStreamName) == nameToPhysicalStream.end()) {
        NES_ERROR("StreamCatalog: unable to find stream physicalStreamName=" << physicalStreamName);
        return {false, ""};
    }

    auto physicalStream = nameToPhysicalStream[physicalStreamName];
    auto node = physicalStream->getNode();
    auto ipAddress = node->getIpAddress();
    auto grpcPort = node->getGrpcPort();
    std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);

    try {
        return workerRpcClient->setSourceConfig(rpcAddress, physicalStreamName, configString);
    } catch (std::exception& ex) {
        NES_ERROR("StreamCatalog: error on rpc call to set source config, physicalStreamName=" << physicalStreamName);
        throw Exception("Could not retrieve source config for physicalStream " + physicalStreamName);
    }
}

}// namespace NES

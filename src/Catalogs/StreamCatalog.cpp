#include <Catalogs/LogicalStream.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <assert.h>
namespace NES {

void StreamCatalog::addDefaultStreams() {
    NES_DEBUG("Streamcatalog addDefaultStreams");
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    bool success = addLogicalStream("default_logical", schema);
    if (!success) {
        NES_ERROR("StreamCatalog::addDefaultStreams: error while add default_logical");
        throw Exception("Error while addDefaultStreams StreamCatalog");
    }

    //TODO I think we should get rid of this soon
    SchemaPtr schemaExdra = Schema::create()
                                ->addField("type", DataTypeFactory::createFixedChar(30))
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
        NES_ERROR("StreamCatalog::addDefaultStreams: error while add exdra");
        throw Exception("Error while addDefaultStreams StreamCatalog");
    }
}
StreamCatalog::StreamCatalog() {
    NES_DEBUG("StreamCatalog: construct stream catalog");
    addDefaultStreams();
    NES_DEBUG("StreamCatalog: construct stream catalog successfully");
}

StreamCatalog::~StreamCatalog() {
    NES_DEBUG("StreamCatalog: deconstruct stream catalog");
}

bool StreamCatalog::addLogicalStream(const std::string& streamName, const std::string& streamSchema) {
    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(streamSchema);
    NES_DEBUG("StreamCatalog: schema successfully created");
    return addLogicalStream(streamName, schema);
}

bool StreamCatalog::addLogicalStream(std::string logicalStreamName,
                                     SchemaPtr schemaPtr) {
    //check if stream already exist
    NES_DEBUG(
        "StreamCatalog: search for logical stream in addLogicalStream() " << logicalStreamName);

    if (!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
        NES_DEBUG("StreamCatalog: add logical stream " << logicalStreamName);
        logicalStreamToSchemaMapping[logicalStreamName] = schemaPtr;
        return true;
    } else {
        NES_ERROR(
            "StreamCatalog: logical stream " << logicalStreamName << " already exists");
        return false;
    }
}

bool StreamCatalog::removeLogicalStream(std::string logicalStreamName) {
    NES_DEBUG(
        "StreamCatalog: search for logical stream in removeLogicalStream() " << logicalStreamName);

    if (logicalStreamToSchemaMapping.find(logicalStreamName)//if log stream does not exists
        == logicalStreamToSchemaMapping.end()) {
        NES_ERROR(
            "StreamCatalog: logical stream " << logicalStreamName << " does not exists");
        return false;
    } else {
        NES_DEBUG("StreamCatalog: remove logical stream " << logicalStreamName);

        if (logicalToPhysicalStreamMapping[logicalStreamName].size() != 0) {
            NES_DEBUG(
                "StreamCatalog: cannot remove " << logicalStreamName
                                                << " because there are physical entries for this stream");
            return false;
        }
        size_t cnt = logicalStreamToSchemaMapping.erase(logicalStreamName);
        NES_DEBUG("StreamCatalog: removed " << cnt << " copies of the stream");
        assert(!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName));
        return true;
    }
}

bool StreamCatalog::addPhysicalStream(std::string logicalStreamName, StreamCatalogEntryPtr newEntry) {
    NES_DEBUG(
        "StreamCatalog: search for logical stream in addPhysicalStream() " << logicalStreamName);

    // check if logical stream exists
    if (!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
        NES_ERROR(
            "StreamCatalog: logical stream " << logicalStreamName << " does not exists when inserting physical stream "
                                             << newEntry->getPhysicalName());
        return false;
    } else {
        NES_DEBUG(
            "StreamCatalog: logical stream " << logicalStreamName << " exists try to add physical stream "
                                             << newEntry->getPhysicalName());

        //get current physical stream for this logical stream
        std::vector<StreamCatalogEntryPtr> physicalStreams =
            logicalToPhysicalStreamMapping[logicalStreamName];

        //check if physical stream does not exist yet
        for (StreamCatalogEntryPtr entry : physicalStreams) {
            NES_DEBUG(
                "test node id=" << entry->getNode()->getId() << " phyStr=" << entry->getPhysicalName());
            NES_DEBUG(
                "test to be inserted id=" << newEntry->getNode()->getId() << " phyStr=" << newEntry->getPhysicalName());
            if (entry->getPhysicalName() == newEntry->getPhysicalName()) {
                if (entry->getNode()->getId() == newEntry->getNode()->getId()) {
                    NES_ERROR(
                        "StreamCatalog: node with id=" << newEntry->getNode()->getId() << " name="
                                                       << newEntry->getPhysicalName() << " already exists");
                    return false;
                }
            }
        }
    }
    NES_DEBUG(
        "StreamCatalog: physical stream " << newEntry->getPhysicalName() << " does not exist, try to add");

    //if first one
    if (testIfLogicalStreamExistsInLogicalToPhysicalMapping(logicalStreamName)) {
        NES_DEBUG("stream already exist, just add new entry");
        logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry);
    } else {
        NES_DEBUG("stream does not exist, create new item");
        logicalToPhysicalStreamMapping.insert(
            pair<std::string, std::vector<StreamCatalogEntryPtr>>(
                logicalStreamName, std::vector<StreamCatalogEntryPtr>()));
        logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry);
    }

    NES_DEBUG(
        "StreamCatalog: physical stream " << newEntry->getPhysicalName() << " id=" << newEntry->getNode()->getId()
                                          << " successful added");
    return true;
}

bool StreamCatalog::removeAllPhysicalStreams(std::string physicalStreamName) {
    NES_NOT_IMPLEMENTED();
}

bool StreamCatalog::removePhysicalStream(string logicalStreamName, string physicalStreamName, std::size_t hashId) {
    NES_DEBUG(
        "StreamCatalog: search for logical stream in removePhysicalStream() " << logicalStreamName);

    // check if logical stream exists
    if (logicalStreamToSchemaMapping.find(logicalStreamName)//log stream does not exists
        == logicalStreamToSchemaMapping.end()) {
        NES_ERROR(
            "StreamCatalog: logical stream " << logicalStreamName
                                             << " does not exists when trying to remove physical stream with hashId"
                                             << hashId);
        return false;
    } else {
        NES_DEBUG(
            "StreamCatalog: logical stream " << logicalStreamName << " exists try to remove physical stream"
                                             << physicalStreamName << " from node " << hashId);
        for (vector<StreamCatalogEntryPtr>::const_iterator entry =
                 logicalToPhysicalStreamMapping[logicalStreamName].cbegin();
             entry != logicalToPhysicalStreamMapping[logicalStreamName].cend();
             entry++) {
            NES_DEBUG(
                "test node id=" << entry->get()->getNode()->getId() << " phyStr="
                                << entry->get()->getPhysicalName());
            NES_DEBUG(
                "test to be deleted id=" << hashId << " phyStr=" << physicalStreamName);
            if (entry->get()->getPhysicalName() == physicalStreamName) {
                NES_DEBUG(
                    "StreamCatalog: node with name=" << physicalStreamName << " exists try match hashId"
                                                     << hashId);

                if (entry->get()->getNode()->getId() == hashId) {
                    NES_DEBUG(
                        "StreamCatalog: node with id=" << hashId << " name="
                                                       << physicalStreamName << " exists try to erase");
                    logicalToPhysicalStreamMapping[logicalStreamName].erase(entry);
                    NES_DEBUG(
                        "StreamCatalog: number of entries afterwards "
                        << logicalToPhysicalStreamMapping[logicalStreamName].size());
                    return true;
                }
            }
        }
        NES_DEBUG("StreamCatalog: physical stream " << physicalStreamName << " does not exist on node with id"
                                                    << hashId << " and with logicalStreamName " << logicalStreamName);
    }
    NES_DEBUG("StreamCatalog: physical stream " << physicalStreamName << " does not exist on node with id"
                                                << hashId);
    return false;
}

bool StreamCatalog::removePhysicalStreamByHashId(size_t hashId) {

    for (auto logStream : logicalToPhysicalStreamMapping) {
        NES_DEBUG("StreamCatalog: check log stream " << logStream.first);
        for (vector<StreamCatalogEntryPtr>::const_iterator entry =
                 logicalToPhysicalStreamMapping[logStream.first].cbegin();
             entry != logicalToPhysicalStreamMapping[logStream.first].cend();
             entry++) {
            if (entry->get()->getNode()->getId() == hashId) {
                NES_DEBUG(
                    "StreamCatalog: found entry with nodeid=" << entry->get()->getNode()->getId()
                                                              << " physicalStream=" << entry->get()->getPhysicalName()
                                                              << " logicalStream="
                                                              << logStream.first);
                //TODO: fix this to return value of erase to update entry or if you use the foreach loop, collect the entries to remove, and remove them in a batch after
                logicalToPhysicalStreamMapping[logStream.first].erase(entry);
                NES_DEBUG("StreamCatalog: deleted physical stream with hashID" << hashId << "and name"
                                                                               << entry->get()->getPhysicalName()
                                                                               << " successfully");
                return true;
            }
        }
    }
    return false;
}

SchemaPtr StreamCatalog::getSchemaForLogicalStream(std::string logicalStreamName) {
    return logicalStreamToSchemaMapping[logicalStreamName];
}

LogicalStreamPtr StreamCatalog::getStreamForLogicalStream(std::string logicalStreamName) {
    return std::make_shared<LogicalStream>(
        logicalStreamName, logicalStreamToSchemaMapping[logicalStreamName]);
}

LogicalStreamPtr StreamCatalog::getStreamForLogicalStreamOrThrowException(std::string logicalStreamName) {
    if (logicalStreamToSchemaMapping.find(logicalStreamName)
        != logicalStreamToSchemaMapping.end()) {
        return std::make_shared<LogicalStream>(
            logicalStreamName, logicalStreamToSchemaMapping[logicalStreamName]);
    } else {
        NES_ERROR(
            "StreamCatalog::getStreamForLogicalStreamOrThrowException: stream does not exists " << logicalStreamName);
        throw Exception("Required stream does not exists " + logicalStreamName);
    }
}

bool StreamCatalog::testIfLogicalStreamExistsInSchemaMapping(std::string logicalStreamName) {
    return logicalStreamToSchemaMapping.find(logicalStreamName)//if log stream does not exists
        != logicalStreamToSchemaMapping.end();
}
bool StreamCatalog::testIfLogicalStreamExistsInLogicalToPhysicalMapping(std::string logicalStreamName) {
    return logicalToPhysicalStreamMapping.find(logicalStreamName)//if log stream does not exists
        != logicalToPhysicalStreamMapping.end();
}

vector<NESTopologyEntryPtr> StreamCatalog::getSourceNodesForLogicalStream(string logicalStreamName) {

    vector<NESTopologyEntryPtr> listOfSourceNodes;

    //get current physical stream for this logical stream
    vector<StreamCatalogEntryPtr> physicalStreams = logicalToPhysicalStreamMapping[logicalStreamName];

    if (physicalStreams.empty()) {
        return listOfSourceNodes;
    }

    for (StreamCatalogEntryPtr entry : physicalStreams) {
        listOfSourceNodes.push_back(entry->getNode());
    }

    return listOfSourceNodes;
}

bool StreamCatalog::reset() {
    NES_DEBUG("StreamCatalog: reset Stream Catalog");
    logicalStreamToSchemaMapping.clear();
    logicalToPhysicalStreamMapping.clear();

    addDefaultStreams();
    NES_DEBUG("StreamCatalog: reset Stream Catalog completed");
    return true;
}

std::string StreamCatalog::getPhysicalStreamAndSchemaAsString() {
    stringstream ss;
    for (auto entry : logicalToPhysicalStreamMapping) {
        ss << "stream name=" << entry.first << " with " << entry.second.size()
           << " elements:";
        for (StreamCatalogEntryPtr sce : entry.second) {
            ss << sce->toString();
        }
        ss << std::endl;
    }
    return ss.str();
}

std::vector<StreamCatalogEntryPtr> StreamCatalog::getPhysicalStreams(
    std::string logicalStreamName) {
    return logicalToPhysicalStreamMapping[logicalStreamName];
}

std::map<std::string, SchemaPtr> StreamCatalog::getAllLogicalStream() {
    return logicalStreamToSchemaMapping;
}

std::map<std::string, std::string> StreamCatalog::getAllLogicalStreamAsString() {

    std::map<std::string, std::string> allLogicalStreamAsString;
    const map<std::string, SchemaPtr> allLogicalStream = getAllLogicalStream();

    for (auto const& [key, val] : allLogicalStream) {
        allLogicalStreamAsString[key] = val->toString();
    }
    return allLogicalStreamAsString;
}

bool StreamCatalog::updatedLogicalStream(std::string& streamName, std::string& streamSchema) {
    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(streamSchema);
    bool removed = removeLogicalStream(streamName);

    if (removed) {
        return addLogicalStream(streamName, schema);
    } else {
        return false;
    }
}

}// namespace NES

#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>

namespace iotdb {

StreamCatalog& StreamCatalog::instance() {
  static StreamCatalog instance;
  return instance;
}

StreamCatalog::StreamCatalog() {

  Schema schema = Schema::create().addField("id", BasicType::UINT32).addField(
      "value", BasicType::UINT64);
  addLogicalStream("default_logical", std::make_shared<Schema>(schema));
  IOTDB_DEBUG("StreamCatalog: construct stream catalog")
}

StreamCatalog::~StreamCatalog() {
  IOTDB_DEBUG("StreamCatalog: deconstruct stream catalog")
}

bool StreamCatalog::addLogicalStream(std::string logicalStreamName,
                                     SchemaPtr schemaPtr) {
  //check if stream already exist
  IOTDB_DEBUG(
      "StreamCatalog: search for logical stream in addLogicalStream() " << logicalStreamName)
        IOTDB_DEBUG("stream before=" << getLogicalStreamAndSchemaAsString())

  if (!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
    IOTDB_DEBUG("StreamCatalog: add logical stream " << logicalStreamName)
    logicalStreamToSchemaMapping[logicalStreamName] = schemaPtr;
    return true;
  } else {
    IOTDB_ERROR(
        "StreamCatalog: logical stream " << logicalStreamName << " already exists")
    IOTDB_DEBUG("stream=" << getLogicalStreamAndSchemaAsString())
    return false;
  }
}

bool StreamCatalog::removeLogicalStream(std::string logicalStreamName) {
  IOTDB_DEBUG(
      "StreamCatalog: search for logical stream in removeLogicalStream() " << logicalStreamName)

  if (logicalStreamToSchemaMapping.find(logicalStreamName)  //if log stream does not exists
  == logicalStreamToSchemaMapping.end()) {
    IOTDB_ERROR(
        "StreamCatalog: logical stream " << logicalStreamName << " does not exists")
    return false;
  } else {
    IOTDB_DEBUG("StreamCatalog: remove logical stream " << logicalStreamName)

    if (logicalToPhysicalStreamMapping[logicalStreamName].size() != 0) {
      IOTDB_DEBUG(
          "StreamCatalog: cannot remove " << logicalStreamName << " because there are physical entries for this stream")
      return false;
    }
    size_t cnt = logicalStreamToSchemaMapping.erase(logicalStreamName);
    IOTDB_DEBUG("StreamCatalog: removed " << cnt << " copies of the stream")
    assert(!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName));
    return true;
  }
}

bool StreamCatalog::addPhysicalStream(std::string logicalStreamName,
                                      StreamCatalogEntryPtr newEntry) {
  IOTDB_DEBUG(
      "StreamCatalog: search for logical stream in addPhysicalStream() " << logicalStreamName)

  // check if logical stream exists
  if (!testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
    IOTDB_ERROR(
        "StreamCatalog: logical stream " << logicalStreamName << " does not exists when inserting physical stream " << newEntry->getPhysicalName())
    return false;
  } else {
    IOTDB_DEBUG(
        "StreamCatalog: logical stream " << logicalStreamName << " exists try to add physical stream " << newEntry->getPhysicalName())

    //get current physical stream for this logical stream
    std::vector<StreamCatalogEntryPtr> physicalStreams =
        logicalToPhysicalStreamMapping[logicalStreamName];

    //check if physical stream does not exist yet
    for (StreamCatalogEntryPtr &entry : physicalStreams) {
      IOTDB_DEBUG(
          "test node id=" << entry->getNode()->getId() << " phyStr=" << entry->getPhysicalName())
      IOTDB_DEBUG(
          "test to be inserted id=" << newEntry->getNode()->getId() << " phyStr=" << newEntry->getPhysicalName())
      if (entry->getPhysicalName() == newEntry->getPhysicalName()) {
        if (entry->getNode()->getId() == newEntry->getNode()->getId()) {
          IOTDB_ERROR(
              "StreamCatalog: node with id=" << newEntry->getNode()->getId() << " name=" << newEntry->getPhysicalName() << " already exists")
          return false;
        }
      }
    }
  }
  IOTDB_DEBUG(
      "StreamCatalog: physical stream " << newEntry->getPhysicalName() << " does not exist, try to add")

  //if first one
  if (testIfLogicalStreamExistsInLogicalToPhysicalMapping(logicalStreamName)) {
    IOTDB_DEBUG("stream already exist, just add new entry")
    logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry);
  } else {
    IOTDB_DEBUG("stream does not exist, create new item")
    logicalToPhysicalStreamMapping.insert(
        pair<std::string, std::vector<StreamCatalogEntryPtr>>(
            logicalStreamName, std::vector<StreamCatalogEntryPtr>()));
    logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry);
  }

  IOTDB_DEBUG(
      "StreamCatalog: physical stream " << newEntry->getPhysicalName() << " id=" << newEntry->getNode()->getId() << " successful added")
  return true;
}

bool StreamCatalog::removePhysicalStream(std::string logicalStreamName,
                                         StreamCatalogEntryPtr newEntry) {
  IOTDB_DEBUG(
      "StreamCatalog: search for logical stream in removePhysicalStream() " << logicalStreamName)

  // check if logical stream exists
  if (logicalStreamToSchemaMapping.find(logicalStreamName)  //log stream does not exists
  == logicalStreamToSchemaMapping.end()) {
    IOTDB_ERROR(
        "StreamCatalog: logical stream " << logicalStreamName << " does not exists when removing physical stream " << newEntry->getPhysicalName())
    return false;
  } else {
    IOTDB_DEBUG(
        "StreamCatalog: logical stream " << logicalStreamName << " exists try to remove physical stream " << newEntry->getPhysicalName())

    for (vector<StreamCatalogEntryPtr>::const_iterator entry =
        logicalToPhysicalStreamMapping[logicalStreamName].cbegin();
        entry != logicalToPhysicalStreamMapping[logicalStreamName].cend();
        entry++) {
      IOTDB_DEBUG(
          "test node id=" << entry->get()->getNode()->getId() << " phyStr=" << entry->get()->getPhysicalName())
      IOTDB_DEBUG(
          "test to be inserted id=" << newEntry->getNode()->getId() << " phyStr=" << newEntry->getPhysicalName())
      if (entry->get()->getPhysicalName() == newEntry->getPhysicalName()) {
        if (entry->get()->getNode()->getId() == newEntry->getNode()->getId()) {
          IOTDB_DEBUG(
              "StreamCatalog: node with id=" << newEntry->getNode()->getId() << " name=" << newEntry->getPhysicalName() << " exists")
          logicalToPhysicalStreamMapping[logicalStreamName].erase(entry);
          return true;
        }
      }
    }
  }
  IOTDB_DEBUG(
      "StreamCatalog: physical stream " << newEntry->getPhysicalName() << " does not exist")
  return false;
}

SchemaPtr StreamCatalog::getSchemaForLogicalStream(
    std::string logicalStreamName) {
  return logicalStreamToSchemaMapping[logicalStreamName];
}

StreamPtr StreamCatalog::getStreamForLogicalStream(
    std::string logicalStreamName) {
  return std::make_shared<Stream>(
      logicalStreamName, logicalStreamToSchemaMapping[logicalStreamName]);
}

StreamPtr StreamCatalog::getStreamForLogicalStreamOrThrowException(
    std::string logicalStreamName) {
  if (logicalStreamToSchemaMapping.find(logicalStreamName)
      != logicalStreamToSchemaMapping.end()) {
    return std::make_shared<Stream>(
        logicalStreamName, logicalStreamToSchemaMapping[logicalStreamName]);
  } else {
    throw Exception("Required stream does not exists " + logicalStreamName);
  }
}

bool StreamCatalog::testIfLogicalStreamExistsInSchemaMapping(
    std::string logicalStreamName) {
  return logicalStreamToSchemaMapping.find(logicalStreamName)  //if log stream does not exists
  != logicalStreamToSchemaMapping.end();
}
bool StreamCatalog::testIfLogicalStreamExistsInLogicalToPhysicalMapping(
    std::string logicalStreamName) {
  return logicalToPhysicalStreamMapping.find(logicalStreamName)  //if log stream does not exists
  != logicalToPhysicalStreamMapping.end();
}

deque<NESTopologyEntryPtr> StreamCatalog::getSourceNodesForLogicalStream(
    std::string logicalStreamName) {

//get current physical stream for this logical stream
  std::vector<StreamCatalogEntryPtr> physicalStreams =
      logicalToPhysicalStreamMapping[logicalStreamName];

  deque<NESTopologyEntryPtr> listOfSourceNodes;
  for (StreamCatalogEntryPtr &entry : physicalStreams) {
    listOfSourceNodes.push_back(entry->getNode());
  }

  return listOfSourceNodes;
}

void StreamCatalog::reset() {
  //TODO: check for potential memory loss
  logicalStreamToSchemaMapping.clear();
  logicalToPhysicalStreamMapping.clear();
  Schema schema = Schema::create().addField("id", BasicType::UINT32).addField(
      "value", BasicType::UINT64);
  addLogicalStream("default_logical", std::make_shared<Schema>(schema));
}

std::string StreamCatalog::getLogicalStreamAndSchemaAsString() {
  stringstream ss;
  for (auto entry : logicalStreamToSchemaMapping) {
    //FIXME: this has to be done beacuas somehow the name is not deleted in the list
    if (testIfLogicalStreamExistsInSchemaMapping(entry.first)) {
      ss << "logical stream name=" << entry.first;
      if (entry.second != nullptr)
        ss << " schema:" << entry.second->toString() << std::endl;
    }
  }
  return ss.str();
}

std::string StreamCatalog::getPhysicalStreamAndSchemaAsString() {
  stringstream ss;
  for (auto entry : logicalToPhysicalStreamMapping) {
    ss << "stream name=" << entry.first << " with " << entry.second.size()
       << " elements:";
    for (StreamCatalogEntryPtr &sce : entry.second) {
      ss << sce->toString();
    }
    ss << std::endl;
  }
  return ss.str();
}

}

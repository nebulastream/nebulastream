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

void StreamCatalog::addLogicalStream(std::string logicalStreamName,
                                     SchemaPtr schemaPtr) {
  //check if stream already exist
  IOTDB_DEBUG(
      "StreamCatalog: search for logical stream in addLogicalStream() " << logicalStreamName)

  if (logicalStreamToSchemaMapping.find(logicalStreamName)
      == logicalStreamToSchemaMapping.end()) {
    IOTDB_DEBUG("StreamCatalog: add logical stream " << logicalStreamName)
    logicalStreamToSchemaMapping[logicalStreamName] = schemaPtr;
  } else {
    IOTDB_ERROR(
        "StreamCatalog: logical stream " << logicalStreamName << " already exists")
  }
}

void StreamCatalog::addPhysicalStream(std::string logicalStreamName,
                                      StreamCatalogEntryPtr newEntry) {
  IOTDB_DEBUG(
      "StreamCatalog: search for logical stream in addPhysicalStream() " << logicalStreamName)

  // check if logical stream exists
  if (logicalStreamToSchemaMapping.find(logicalStreamName)
      == logicalStreamToSchemaMapping.end()) {
    IOTDB_ERROR(
        "StreamCatalog: logical stream " << logicalStreamName << " does not exists when inserting physical stream " << newEntry->getPhysicalName())
  } else {

    IOTDB_DEBUG(
        "StreamCatalog: logical stream " << logicalStreamName << " exists try to add physical stream " << newEntry->getPhysicalName())

    //get current physical stream for this logical stream
    std::vector<StreamCatalogEntryPtr> physicalStreams =
        logicalToPhysicalStreamMapping[logicalStreamName];

    //check if physical stream does not exist yet
    for (StreamCatalogEntryPtr &entry : physicalStreams) {
      if (entry->getPhysicalName() == newEntry->getPhysicalName()
          && entry->getNode()->getNodeId() == newEntry->getNode()->getNodeId()) {
        IOTDB_ERROR(
            "StreamCatalog: node with id=" << newEntry->getNode()->getNodeId()<< " name=" << newEntry->getPhysicalName() << " already exists")
        return;
      }
    }
    IOTDB_DEBUG(
        "StreamCatalog: physical stream " << newEntry->getPhysicalName() << " does not exist, try to add")
    logicalToPhysicalStreamMapping[logicalStreamName].push_back(newEntry);

    IOTDB_DEBUG("StreamCatalog: physical stream " << newEntry->getPhysicalName() << " successful added")
  }
}

SchemaPtr StreamCatalog::getSchemaForLogicalStream(
    std::string logicalStreamName) {
  return logicalStreamToSchemaMapping[logicalStreamName];
}

bool StreamCatalog::testIfLogicalStreamExists(std::string logicalStreamName) {
  if (logicalStreamToSchemaMapping.count(logicalStreamName))
    return true;
  else
    return false;
}

deque<NESTopologyEntryPtr> StreamCatalog::getSourceNodesForLogicalStream(
    std::string logicalStreamName) {

  //get current physical stream for this logical stream
  std::vector<StreamCatalogEntryPtr> physicalStreams =
      logicalToPhysicalStreamMapping[logicalStreamName];

  deque<NESTopologyEntryPtr> listOfSourceNodes;
  for (StreamCatalogEntryPtr& entry : physicalStreams) {
    listOfSourceNodes.push_back(entry->getNode());
  }

  return listOfSourceNodes;
}
}

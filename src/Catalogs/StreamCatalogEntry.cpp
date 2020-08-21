#include <Catalogs/StreamCatalogEntry.hpp>
#include <Topology/PhysicalNode.hpp>
#include <Util/Logger.hpp>

namespace NES {

StreamCatalogEntry::StreamCatalogEntry(PhysicalStreamConfig streamConf, PhysicalNodePtr node)
    : streamConf(streamConf), node(node){};

std::string StreamCatalogEntry::getSourceType() {
    return streamConf.sourceType;
}

std::string StreamCatalogEntry::getSourceConfig() {
    return streamConf.sourceConfig;
}

PhysicalNodePtr StreamCatalogEntry::getNode() {
    return node;
}

std::string StreamCatalogEntry::getPhysicalName() {
    return streamConf.physicalStreamName;
}

std::string StreamCatalogEntry::getLogicalName() {
    return streamConf.logicalStreamName;
}

double StreamCatalogEntry::getSourceFrequency() {
    return streamConf.sourceFrequency;
}

size_t StreamCatalogEntry::getNumberOfBuffersToProduce() {
    return streamConf.numberOfBuffersToProduce;
}

std::string StreamCatalogEntry::toString() {
    std::stringstream ss;
    ss << "physicalName=" << streamConf.physicalStreamName
       << " logicalStreamName=" << streamConf.logicalStreamName << " sourceType="
       << streamConf.sourceType << " sourceConfig=" << streamConf.sourceConfig
       << " sourceFrequency=" << streamConf.sourceFrequency
       << " numberOfBuffersToProduce=" << streamConf.numberOfBuffersToProduce
       << " on node=" + std::to_string(node->getId());
    return ss.str();
}

}// namespace NES

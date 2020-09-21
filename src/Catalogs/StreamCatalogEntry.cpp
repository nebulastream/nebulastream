#include <Catalogs/StreamCatalogEntry.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

namespace NES {

StreamCatalogEntry::StreamCatalogEntry(PhysicalStreamConfigPtr streamConf, TopologyNodePtr node)
    : streamConf(streamConf), node(node){};

std::string StreamCatalogEntry::getSourceType() {
    return streamConf->getSourceType();
}

std::string StreamCatalogEntry::getSourceConfig() {
    return streamConf->getSourceConfig();
}

TopologyNodePtr StreamCatalogEntry::getNode() {
    return node;
}

std::string StreamCatalogEntry::getPhysicalName() {
    return streamConf->getPhysicalStreamName();
}

std::string StreamCatalogEntry::getLogicalName() {
    return streamConf->getLogicalStreamName();
}

double StreamCatalogEntry::getSourceFrequency() {
    return streamConf->getSourceFrequency();
}

size_t StreamCatalogEntry::getNumberOfBuffersToProduce() {
    return streamConf->getNumberOfBuffersToProduce();
}

size_t StreamCatalogEntry::getNumberOfTuplesToProducePerBuffer() {
    return streamConf->getNumberOfTuplesToProducePerBuffer();
}
std::string StreamCatalogEntry::toString() {
    std::stringstream ss;
    ss << "physicalName=" << streamConf->getPhysicalStreamName()
       << " logicalStreamName=" << streamConf->getLogicalStreamName() << " sourceType="
       << streamConf->getSourceType() << " sourceConfig=" << streamConf->getSourceConfig()
       << " sourceFrequency=" << streamConf->getSourceFrequency()
       << " numberOfBuffersToProduce=" << streamConf->getNumberOfBuffersToProduce()
       << " on node=" + std::to_string(node->getId());
    return ss.str();
}

}// namespace NES

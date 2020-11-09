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

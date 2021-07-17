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
#include <Util/UtilityFunctions.hpp>

namespace NES {

StreamCatalogEntry::StreamCatalogEntry(std::string sourceType,
                                       std::string physicalStreamName,
                                       std::vector<std::string> logicalStreamName,
                                       TopologyNodePtr node)
    : sourceType(sourceType), physicalStreamName(physicalStreamName), logicalStreamName(logicalStreamName), node(node) {}

StreamCatalogEntry::StreamCatalogEntry(AbstractPhysicalStreamConfigPtr config, TopologyNodePtr node)
    : sourceType(config->getSourceType()), physicalStreamName(config->getPhysicalStreamName()),
      logicalStreamName(config->getLogicalStreamName()), node(node) {
    // nop
}

StreamCatalogEntryPtr StreamCatalogEntry::create(std::string sourceType,
                                                 std::string physicalStreamName,
                                                 std::vector<std::string> logicalStreamName,
                                                 TopologyNodePtr node) {
    return std::make_shared<StreamCatalogEntry>(sourceType, physicalStreamName, logicalStreamName, node);
}

StreamCatalogEntryPtr StreamCatalogEntry::create(AbstractPhysicalStreamConfigPtr config, TopologyNodePtr node) {
    return std::make_shared<StreamCatalogEntry>(config, node);
}

std::string StreamCatalogEntry::getSourceType() { return sourceType; }

TopologyNodePtr StreamCatalogEntry::getNode() { return node;}

std::string StreamCatalogEntry::getPhysicalName() { return physicalStreamName; }

std::vector<std::string> StreamCatalogEntry::getLogicalName() { return logicalStreamName; }

std::vector<std::string> StreamCatalogEntry::getMissmappedLogicalName() { return mismappedLogicalStreamName; }

bool StreamCatalogEntry::addLogicalStreamName(std::string newLogicalStreamName){
    for(std::string name : logicalStreamName){
        if(name==newLogicalStreamName){
            NES_ERROR("StreamCatalogEntry::addLogicalStreamName logicalStreamName="<<newLogicalStreamName<<" could not be added to StreamCatalogEntry as it already exists");
            return false;
        }
    }
    logicalStreamName.push_back(newLogicalStreamName);
    physicalStreamState.removeReason(noLogicalStream);
    return true;
}

bool StreamCatalogEntry::addLogicalStreamNameToMismapped(std::string newLogicalStreamName){
    for(std::string &name : mismappedLogicalStreamName){
        if(name==newLogicalStreamName){
            NES_ERROR("StreamCatalogEntry::addLogicalStreamNameToMismapped logicalStreamName="<<newLogicalStreamName<<" could not be added to StreamCatalogEntry as it already exists");
            return false;
        }
    }
    mismappedLogicalStreamName.push_back(newLogicalStreamName);
    physicalStreamState.addReason(logicalStreamWithoutSchema, "("+UtilityFunctions::combineStringsWithDelimiter(mismappedLogicalStreamName, ",")+")");
    return true;
}

void StreamCatalogEntry::moveLogicalStreamFromMismappedToRegular(std::string logicalStreamName){
    auto it = std::remove(mismappedLogicalStreamName.begin(), mismappedLogicalStreamName.end(), logicalStreamName);
    mismappedLogicalStreamName.erase(it, mismappedLogicalStreamName.end());

    this->logicalStreamName.push_back(logicalStreamName);
    if(mismappedLogicalStreamName.empty()){
        physicalStreamState.removeReason(logicalStreamWithoutSchema);
    }else{
        physicalStreamState.addReason(logicalStreamWithoutSchema, "("+UtilityFunctions::combineStringsWithDelimiter(mismappedLogicalStreamName, ",")+")");
    }

}

bool StreamCatalogEntry::removeLogicalStreamFromMismapped(std::string oldLogicalStreamName){
    for(std::string &name : mismappedLogicalStreamName){
        if(name==oldLogicalStreamName){
            auto it = std::remove(mismappedLogicalStreamName.begin(), mismappedLogicalStreamName.end(), oldLogicalStreamName);
            mismappedLogicalStreamName.erase(it, mismappedLogicalStreamName.end());
            NES_DEBUG("StreamCatalogEntry::removeLogicalStream logicalStreamName="<<oldLogicalStreamName<<" was removed.");
            if(mismappedLogicalStreamName.empty()){
                physicalStreamState.removeReason(logicalStreamWithoutSchema);
            }else{
                physicalStreamState.addReason(logicalStreamWithoutSchema, "("+UtilityFunctions::combineStringsWithDelimiter(mismappedLogicalStreamName, ",")+")");
            }
            return true;
        }
    }
    NES_ERROR("StreamCatalogEntry::removeLogicalStream logicalStreamName="<<oldLogicalStreamName<<" could not be removed from StreamCatalogEntry as it does not exists.");
    return false;
}

bool StreamCatalogEntry::removeLogicalStream(std::string oldLogicalStreamName){
    for(std::string &name : logicalStreamName){
        if(name==oldLogicalStreamName){
            auto it = std::remove(logicalStreamName.begin(), logicalStreamName.end(), oldLogicalStreamName);
            logicalStreamName.erase(it, logicalStreamName.end());
            NES_DEBUG("StreamCatalogEntry::removeLogicalStream logicalStreamName="<<oldLogicalStreamName<<" was removed.");
            if(logicalStreamName.empty()){
                physicalStreamState.addReason(noLogicalStream, "no valid logical stream");
            }
            return true;
        }
    }
    NES_ERROR("StreamCatalogEntry::removeLogicalStream logicalStreamName="<<oldLogicalStreamName<<" could not be removed from StreamCatalogEntry as it does not exists.");
    return false;
}

void StreamCatalogEntry::setStateToWithoutLogicalStream(){
    physicalStreamState.addReason(noLogicalStream, "no valid logical stream");
    return;
}

PhysicalStreamState StreamCatalogEntry::getPhysicalStreamState(){
    return physicalStreamState;
}

// BDAPRO : Put changes on Node but not persistent, persistence only after validation
void StreamCatalogEntry::changePhysicalStreamName(std::string newName){
    physicalStreamState.addReason(duplicatePhysicalStreamName, "Physical stream was renamed from "+physicalStreamName+" to "+newName+", as the old name already existed.");
    physicalStreamName=newName;
}

std::string StreamCatalogEntry::toString() {
    std::stringstream ss;
    ss << "physicalName=" << physicalStreamName << " logicalStreamName=(" << UtilityFunctions::combineStringsWithDelimiter(logicalStreamName,",") << ") sourceType=" << sourceType
       << " on node=" + std::to_string(node->getId());
    return ss.str();
}
}// namespace NES

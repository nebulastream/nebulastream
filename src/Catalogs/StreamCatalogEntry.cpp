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
                                       std::vector<std::string> logicalStreamNames,
                                       TopologyNodePtr node)
    : sourceType(sourceType), physicalStreamName(physicalStreamName), logicalStreamNames(logicalStreamNames), node(node) {}

StreamCatalogEntry::StreamCatalogEntry(AbstractPhysicalStreamConfigPtr config, TopologyNodePtr node)
    : sourceType(config->getSourceType()), physicalStreamName(config->getPhysicalStreamName()),
      logicalStreamNames(config->getLogicalStreamNames()), node(node) {
    // nop
}

StreamCatalogEntryPtr StreamCatalogEntry::create(std::string sourceType,
                                                 std::string physicalStreamName,
                                                 std::vector<std::string> logicalStreamNames,
                                                 TopologyNodePtr node) {
    return std::make_shared<StreamCatalogEntry>(sourceType, physicalStreamName, logicalStreamNames, node);
}

StreamCatalogEntryPtr StreamCatalogEntry::create(AbstractPhysicalStreamConfigPtr config, TopologyNodePtr node) {
    return std::make_shared<StreamCatalogEntry>(config, node);
}

std::string StreamCatalogEntry::getSourceType() { return sourceType; }

TopologyNodePtr StreamCatalogEntry::getNode() { return node; }

std::string StreamCatalogEntry::getPhysicalName() { return physicalStreamName; }

std::vector<std::string> StreamCatalogEntry::getLogicalNames() { return logicalStreamNames; }

std::vector<std::string> StreamCatalogEntry::getMissmappedLogicalName() { return logicalStreamNames; }

bool StreamCatalogEntry::addLogicalStreamName(std::string newLogicalStreamName) {
    for (std::string name : logicalStreamNames) {
        if (name == newLogicalStreamName) {
            NES_ERROR("StreamCatalogEntry::addLogicalStreamName logicalStreamName="
                      << newLogicalStreamName << " could not be added to StreamCatalogEntry as it already exists");
            return false;
        }
    }
    logicalStreamNames.push_back(newLogicalStreamName);
    physicalStreamState.removeReason(NO_LOGICAL_STREAM);
    return true;
}

bool StreamCatalogEntry::addLogicalStreamNameToMismapped(std::string newLogicalStreamName) {
    for (std::string& name : mismappedLogicalStreamNames) {
        if (name == newLogicalStreamName) {
            NES_ERROR("StreamCatalogEntry::addLogicalStreamNameToMismapped logicalStreamName="
                      << newLogicalStreamName << " could not be added to StreamCatalogEntry as it already exists");
            return false;
        }
    }
    mismappedLogicalStreamNames.push_back(newLogicalStreamName);
    physicalStreamState.addReason(LOGICAL_STREAM_WITHOUT_SCHEMA,
                                  "(" + UtilityFunctions::combineStringsWithDelimiter(mismappedLogicalStreamNames, ",") + ")");
    return true;
}

void StreamCatalogEntry::moveLogicalStreamFromMismappedToRegular(std::string logicalStreamName) {
    auto it = std::remove(mismappedLogicalStreamNames.begin(), mismappedLogicalStreamNames.end(), logicalStreamName);
    mismappedLogicalStreamNames.erase(it, mismappedLogicalStreamNames.end());

    this->logicalStreamNames.push_back(logicalStreamName);
    if (mismappedLogicalStreamNames.empty()) {
        physicalStreamState.removeReason(LOGICAL_STREAM_WITHOUT_SCHEMA);
    } else {
        physicalStreamState.addReason(LOGICAL_STREAM_WITHOUT_SCHEMA,
                                      "(" + UtilityFunctions::combineStringsWithDelimiter(mismappedLogicalStreamNames, ",") + ")");
    }
}

bool StreamCatalogEntry::removeLogicalStreamFromMismapped(std::string oldLogicalStreamName) {
    for (std::string& name : mismappedLogicalStreamNames) {
        if (name == oldLogicalStreamName) {
            auto it = std::remove(mismappedLogicalStreamNames.begin(), mismappedLogicalStreamNames.end(), oldLogicalStreamName);
            mismappedLogicalStreamNames.erase(it, mismappedLogicalStreamNames.end());
            NES_DEBUG("StreamCatalogEntry::removeLogicalStream logicalStreamName=" << oldLogicalStreamName << " was removed.");
            if (mismappedLogicalStreamNames.empty()) {
                physicalStreamState.removeReason(LOGICAL_STREAM_WITHOUT_SCHEMA);
            } else {
                physicalStreamState.addReason(LOGICAL_STREAM_WITHOUT_SCHEMA,
                                              "(" + UtilityFunctions::combineStringsWithDelimiter(mismappedLogicalStreamNames, ",")
                                                  + ")");
            }
            return true;
        }
    }
    NES_ERROR("StreamCatalogEntry::removeLogicalStream logicalStreamName="
              << oldLogicalStreamName << " could not be removed from StreamCatalogEntry as it does not exists.");
    return false;
}

bool StreamCatalogEntry::removeLogicalStream(std::string oldLogicalStreamName) {
    for (std::string& name : logicalStreamNames) {
        if (name == oldLogicalStreamName) {
            auto it = std::remove(logicalStreamNames.begin(), logicalStreamNames.end(), oldLogicalStreamName);
            logicalStreamNames.erase(it, logicalStreamNames.end());
            NES_DEBUG("StreamCatalogEntry::removeLogicalStream logicalStreamName=" << oldLogicalStreamName << " was removed.");
            if (logicalStreamNames.empty()) {
                physicalStreamState.addReason(NO_LOGICAL_STREAM, "no valid logical stream");
            }
            return true;
        }
    }
    NES_ERROR("StreamCatalogEntry::removeLogicalStream logicalStreamName="
              << oldLogicalStreamName << " could not be removed from StreamCatalogEntry as it does not exists.");
    return false;
}

void StreamCatalogEntry::setStateToWithoutLogicalStream() {
    physicalStreamState.addReason(NO_LOGICAL_STREAM, "no valid logical stream");
    return;
}

PhysicalStreamState StreamCatalogEntry::getPhysicalStreamState() { return physicalStreamState; }

void StreamCatalogEntry::changePhysicalStreamName(std::string newName) {
    physicalStreamState.addReason(DUPLICATE_PHYSICAL_STREAM_NAME,
                                  "Physical stream was renamed from " + physicalStreamName + " to " + newName
                                      + ", as the old name already existed.");
    physicalStreamName = newName;
}

void StreamCatalogEntry::getAllLogicalName(std::vector<std::string>& all) {
    all.reserve(logicalStreamNames.size() + mismappedLogicalStreamNames.size());// preallocate memory
    all.insert(all.end(), logicalStreamNames.begin(), logicalStreamNames.end());
    all.insert(all.end(), mismappedLogicalStreamNames.begin(), mismappedLogicalStreamNames.end());
}

std::string StreamCatalogEntry::toString() {
    std::stringstream ss;
    ss << "physicalName=" << physicalStreamName << " logicalStreamNames=("
       << UtilityFunctions::combineStringsWithDelimiter(logicalStreamNames, ",") << ") sourceType=" << sourceType
       << " on node=" + std::to_string(node->getId());
    return ss.str();
}
}// namespace NES

/*
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
#include "Util/Logger/Logger.hpp"
#include <Util/Experimental/NodeType.hpp>
#include <Util/Experimental/NodeTypeUtilities.hpp>
namespace NES::Spatial::Util {

Index::Experimental::NodeType NodeTypeUtilities::stringToNodeType(const std::string nodeTypeString) {
    if (nodeTypeString == "NO_LOCATION") {
        return Index::Experimental::NodeType::NO_LOCATION;
    } else if (nodeTypeString == "FIXED_LOCATION") {
        return Index::Experimental::NodeType::FIXED_LOCATION;
    } else if (nodeTypeString == "MOBILE_NODE") {
        return Index::Experimental::NodeType::MOBILE_NODE;
    }
    return Index::Experimental::NodeType::INVALID;
}

Index::Experimental::NodeType NodeTypeUtilities::protobufEnumToNodeType(NodeType nodeType) {
    switch (nodeType) {
        case NodeType::NO_LOCATION: return Index::Experimental::NodeType::NO_LOCATION;
        case NodeType::FIXED_LOCATION: return Index::Experimental::NodeType::FIXED_LOCATION;
        case NodeType::MOBILE_NODE: return Index::Experimental::NodeType::MOBILE_NODE;
        case NodeType_INT_MIN_SENTINEL_DO_NOT_USE_: return Index::Experimental::NodeType::INVALID;
        case NodeType_INT_MAX_SENTINEL_DO_NOT_USE_: return Index::Experimental::NodeType::INVALID;
    }
    return Index::Experimental::NodeType::INVALID;
}

std::string NodeTypeUtilities::toString(const Index::Experimental::NodeType nodeType) {
    switch (nodeType) {
        case Index::Experimental::NodeType::NO_LOCATION: return "NO_LOCATION";
        case Index::Experimental::NodeType::FIXED_LOCATION: return "FIXED_LOCATION";
        case Index::Experimental::NodeType::MOBILE_NODE: return "MOBILE_NODE";
        case Index::Experimental::NodeType::INVALID: return "INVALID";
    }
}

NodeType NodeTypeUtilities::toProtobufEnum(Index::Experimental::NodeType nodeType) {
    switch (nodeType) {
        case Index::Experimental::NodeType::NO_LOCATION: return NodeType::NO_LOCATION;
        case Index::Experimental::NodeType::FIXED_LOCATION: return NodeType::FIXED_LOCATION;
        case Index::Experimental::NodeType::MOBILE_NODE: return NodeType::MOBILE_NODE;
        case Index::Experimental::NodeType::INVALID:
            NES_FATAL_ERROR("cannot construct protobuf enum from invalid spatial type, exiting")
            exit(EXIT_FAILURE);
    }
}
}// namespace NES::Spatial::Util
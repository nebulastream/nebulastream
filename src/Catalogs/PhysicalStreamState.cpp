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

#include <Catalogs/PhysicalStreamState.hpp>

namespace NES {
PhysicalStreamState::PhysicalStreamState() { this->state = State::REGULAR; }

std::string PhysicalStreamState::getStateDescription() {
    if (state == REGULAR) {
        return "REGULAR";
    }
    std::string desc = "MISCONFIGURED";

    std::map<Reason, std::string>::iterator it = description.begin();
    // Iterate over the map using Iterator till end.
    while (it != description.end()) {
        desc = desc + "\n" + getStringForReasonEnum(it->first) + ": " + it->second;
        it++;
    }

    return desc;
}

void PhysicalStreamState::addReason(Reason reason, std::string desc) {
    this->description[reason] = desc;
    this->state = MISCONFIGURED;
}
void PhysicalStreamState::removeReason(Reason reason) {
    this->description.erase(reason);
    if (description.empty()) {
        this->state = REGULAR;
    }
}

bool PhysicalStreamState::isNameValid() {
    if (description.find(DUPLICATE_PHYSICAL_STREAM_NAME) != description.end()) {
        return false;
    }
    return true;
}

std::string PhysicalStreamState::getStringForReasonEnum(Reason reason) {
    if (reason == NO_LOGICAL_STREAM) {
        return "NO_LOGICAL_STREAM";
    } else if (reason == LOGICAL_STREAM_WITHOUT_SCHEMA) {
        return "LOGICAL_STREAM_WITHOUT_SCHEMA";
    } else {
        return "DUPLICATE_PHYSICAL_STREAM_NAME";
    }
}

}// namespace NES
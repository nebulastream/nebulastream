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

// BDAPRO ADD states as enum
// BDAPRO add addLog() function, removeLog(), getState(), setState()
// BDAPRO detect miscongig on count == 0


#ifndef NES_PHYSICALSTREAMSTATE_HPP
#define NES_PHYSICALSTREAMSTATE_HPP

#include <string>
#include <map>

namespace NES {
enum State {misconfigured, regular};
enum Reason {noLogicalStream, logicalStreamWithoutSchema,duplicatePhysicalStreamName, fileNotFound};

class PhysicalStreamState {
  public:
    PhysicalStreamState();

    /**
     * @brief changes the State in case count has been increased/lowered to a significant value (0 => misconfigured, > 0 => regular)
     *
     */
    void changeState();

    /**
     * @brief getter for description of state
     *
     */
    std::string getStateDescription();

    /**
     * adds a reason to the reason-mapping
     * @param reason to add
     * @param desc the description of the reason
     */
    void addReason(Reason reason, std::string desc);

    /**
     * Removes a reason from the reason-mapping
     * @param reason to remove
     */
    void removeReason(Reason reason);

    /**
     * Tests whether the StreamCatalogEntry's name is valid
     * @return true if the name is valid, false otherwise
     */
    bool isNameValid();

    /**
     * Get string for enum
     * @param reason
     * @return string description of enum
     */
    std::string getStringForReasonEnum(Reason reason);

    State state;
  private:
    std::map<Reason, std::string> description;
};



}// namespace NES

#endif //NES_PHYSICALSTREAMSTATE_HPP

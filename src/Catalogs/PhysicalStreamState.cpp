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
//BDAPRO create constructor
PhysicalStreamState::PhysicalStreamState(){
    this->state = State::misconfigured;
    this->description = "";
}
void PhysicalStreamState::increaseLogicalStreamCount() {
    count++;
    if (count == 1) changeState();
}

void PhysicalStreamState::lowerLogicalStreamCount() {
    count--;
    if (count == 0) changeState();
}
// BDAPRO add description / remove description
void PhysicalStreamState::changeState() {
    if (count == 0) state = misconfigured;
    else state = regular;
}

std::string PhysicalStreamState::getStateDescription(){ return description; }

} // namespace NES
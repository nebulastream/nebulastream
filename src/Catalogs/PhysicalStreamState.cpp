//
// Created by Vincent Szlang on 16.06.21.
//
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
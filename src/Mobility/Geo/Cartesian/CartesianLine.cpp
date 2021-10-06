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

#include <Mobility/Geo/Cartesian/CartesianLine.h>

namespace NES {

CartesianLine::CartesianLine(double gradient, double constant) : gradient(gradient), constant(constant) {}

[[maybe_unused]] double CartesianLine::getGradient() const { return gradient; }

double CartesianLine::getConstant() const { return constant; }

void CartesianLine::setGradient(double gradientValue) { CartesianLine::gradient = gradientValue; }

void CartesianLine::setConstant(double constantValue) { CartesianLine::constant = constantValue; }

bool CartesianLine::contains(const CartesianPointPtr& point) {
    double expectedYValue = gradient * point->getX() + constant;
    return (expectedYValue == point->getY());
}
double CartesianLine::getY(double x) {
    return gradient * x + constant;
}

}

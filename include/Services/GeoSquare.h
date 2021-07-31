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

#ifndef NES_GEOSQUARE_H
#define NES_GEOSQUARE_H

#include "Catalogs/GeoLocation.h"

namespace NES {

class GeoSquare {

  private:
    GeoLocation center;
    double area;

  public:
    GeoSquare(const GeoLocation& center, double area);
    [[nodiscard]] const GeoLocation& getCenter() const;

    bool contains(GeoLocation location);
    [[nodiscard]] double getDistanceToBound() const;
    GeoLocation getNorthBound();
    GeoLocation getSouthBound();
    GeoLocation getEastBound();
    GeoLocation getWestBound();
};

}

#endif//NES_GEOSQUARE_H

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

#ifndef NES_GEOAREA_H
#define NES_GEOAREA_H

#include <memory>

namespace NES {

class GeoPoint;
using GeoPointPtr = std::shared_ptr<GeoPoint>;

class GeoArea {

  private:
    GeoPointPtr center;
    double area;

  public:
    GeoArea(GeoPointPtr  center, double area);
    [[nodiscard]] const std::shared_ptr<GeoPoint>& getCenter() const;
    [[nodiscard]] double getArea() const;
    void setCenter(const GeoPointPtr& location);

    virtual bool contains(const GeoPointPtr& location) = 0;
    [[nodiscard]] virtual double getDistanceToBound() const = 0;
};

using GeoAreaPtr = std::shared_ptr<GeoArea>;

}

#endif//NES_GEOAREA_H

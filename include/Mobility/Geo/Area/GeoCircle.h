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

#ifndef NES_GEOCIRCLE_H
#define NES_GEOCIRCLE_H

#include <Mobility/Geo/Area/GeoArea.h>

namespace NES {

class GeoPoint;
using GeoPointPtr = std::shared_ptr<GeoPoint>;

class GeoCircle : public GeoArea {

  public:
    GeoCircle(const GeoPointPtr& center, const CartesianPointPtr& cartesianCenter, double area);
    [[nodiscard]] double getDistanceToBound() const override;
    bool contains(const GeoAreaPtr & area) override;
    bool contains(const GeoPointPtr& location) override;

    bool isCircle() override;
    bool isSquare() override;
    virtual ~GeoCircle();
};

using GeoCirclePtr = std::shared_ptr<GeoCircle>;

}

#endif//NES_GEOCIRCLE_H

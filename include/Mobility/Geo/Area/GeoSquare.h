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

#include <Mobility/Geo/Area/GeoArea.h>
#include <Mobility/Geo/CartesianPoint.h>
#include <Mobility/Geo/GeoPoint.h>

namespace NES {

class GeoSquare;
using GeoSquarePtr = std::shared_ptr<GeoSquare>;

template <typename T>
class SquareEdges {
  private:
    T northWest;
    T northEast;
    T southWest;
    T southEast;

  public:
    SquareEdges() = default;
    SquareEdges(T northWest, T northEast, T southWest, T southEast)
        : northWest(northWest), northEast(northEast), southWest(southWest), southEast(southEast) {}
    ~SquareEdges() = default;

    T getNorthWest() const { return northWest; }
    T getNorthEast() const { return northEast; }
    T getSouthWest() const { return southWest; }
    T getSouthEast() const { return southEast; }
};

using SquareCartesianEdgesPtr = std::shared_ptr<SquareEdges<CartesianPointPtr>>;
using SquareGeoEdgesPtr = std::shared_ptr<SquareEdges<GeoPointPtr>>;

class GeoSquare : public GeoArea {

  private:
    SquareCartesianEdgesPtr cartesianEdges;
    SquareGeoEdgesPtr geoEdges;

  public:
    GeoSquare(const GeoPointPtr& center,
              const CartesianPointPtr& cartesianCenter,
              double area,
              SquareCartesianEdgesPtr  cartesianEdges,
              SquareGeoEdgesPtr  geoEdges);

    [[nodiscard]] const SquareCartesianEdgesPtr& getCartesianEdges() const;
    [[nodiscard]] const SquareGeoEdgesPtr& getGeoEdges() const;

    [[nodiscard]] double getDistanceToBound() const override;
    bool contains(const GeoAreaPtr & area) override;
    bool contains(const GeoPointPtr& location) override;

    bool isCircle() override;
    bool isSquare() override;

    virtual ~GeoSquare();
};

}

#endif//NES_GEOSQUARE_H

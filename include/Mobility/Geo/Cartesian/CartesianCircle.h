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

#ifndef NES_CARTESIANCIRCLE_H
#define NES_CARTESIANCIRCLE_H

#include <Mobility/Geo/CartesianPoint.h>

namespace NES {

class CartesianCircle {
  private:
    CartesianPointPtr center;
    double radius;

  public:
    CartesianCircle(const CartesianPointPtr& center, double radius);
    CartesianCircle(double radius);
    const CartesianPointPtr& getCenter() const;
    [[nodiscard]] double getRadius() const;
    void setCenter(const CartesianPointPtr& center);
};

using CartesianCirclePtr = std::shared_ptr<CartesianCircle>;

}

#endif//NES_CARTESIANCIRCLE_H

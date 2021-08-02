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

#ifndef NES_LOCATION_H
#define NES_LOCATION_H

#include <string>
#include <cpprest/json.h>

namespace NES {


class GeoPoint {

  private:
    double latitude;
    double longitude;

  public:
    GeoPoint();
    GeoPoint(double  latitude, double longitude);
    [[nodiscard]] double getLatitude() const;
    [[nodiscard]] double getLongitude() const;
    [[nodiscard]] bool isValid() const;

    bool operator==(const GeoPoint& rhs) const;
    bool operator!=(const GeoPoint& rhs) const;
};

using GeoPointPtr = std::shared_ptr<GeoPoint>;

}

#endif//NES_LOCATION_H

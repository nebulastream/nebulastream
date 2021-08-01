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

#include <Mobility/Geo/GeoPoint.h>

namespace NES::Mobility {

GeoPoint::GeoPoint() : latitude(0), longitude(0) {}
GeoPoint::GeoPoint(double latitude, double longitude) : latitude(latitude), longitude(longitude) {}

double GeoPoint::getLatitude() const { return latitude; }
double GeoPoint::getLongitude() const { return longitude; }

bool GeoPoint::isValid() const { return latitude != 0 && longitude !=0; }

bool GeoPoint::operator==(const GeoPoint& rhs) const {
    return latitude == rhs.latitude && longitude == rhs.longitude;
}
bool GeoPoint::operator!=(const GeoPoint& rhs) const { return !(rhs == *this); }

}

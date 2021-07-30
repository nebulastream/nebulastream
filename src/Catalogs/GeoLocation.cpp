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

#include <Catalogs/GeoLocation.h>

NES::GeoLocation::GeoLocation() : latitude(0), longitude(0) {}
NES::GeoLocation::GeoLocation(double latitude, double longitude) : latitude(latitude), longitude(longitude) {}

double NES::GeoLocation::getLatitude() { return latitude; }
double NES::GeoLocation::getLongitude() { return longitude; }

bool NES::GeoLocation::isValid() { return latitude != 0 && longitude !=0; }
bool NES::GeoLocation::operator==(const NES::GeoLocation& rhs) const {
    return latitude == rhs.latitude && longitude == rhs.longitude;
}
bool NES::GeoLocation::operator!=(const NES::GeoLocation& rhs) const { return !(rhs == *this); }

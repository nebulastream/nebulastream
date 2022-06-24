/*
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
#ifndef NES_S2CONVERSION_HPP
#define NES_S2CONVERSION_HPP

#ifdef S2DEF
#include <Common/Location.hpp>
#include <s2/s2point.h>
#include <s2/s2latlng.h>
namespace NES::Spatial::Index::Experimental {
S2Point locationToS2Point(Location location) {
    return {S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude())};
}

Location s2pointToLocation(S2Point point) {
    S2LatLng latLng(point);
    return {latLng.lat().degrees(), latLng.lng().degrees()};
}
}
#endif
#endif//NES_S2CONVERSION_HPP

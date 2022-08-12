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
#ifdef S2DEF
#include <Spatial/Index/Location.hpp>
#include <Util/Experimental/S2Utilities.hpp>
#include <s2/s2latlng.h>
#include <s2/s2point.h>
namespace NES::Spatial::Util {

S2Point S2Utilities::locationToS2Point(Index::Experimental::Location location) {
    return {S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude())};
}

Index::Experimental::Location S2Utilities::s2pointToLocation(S2Point point) {
    S2LatLng latLng(point);
    return {latLng.lat().degrees(), latLng.lng().degrees()};
}
}// namespace NES::Spatial::Util
#endif

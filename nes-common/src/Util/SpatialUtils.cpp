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

#include <Util/SpatialUtils.hpp>

namespace NES {

double SpatialUtils::degreesToRadians(double degrees) { return (degrees * M_PI / 180.0); }

double SpatialUtils::radiansToDegrees(double radians) { return (radians * 180.0 / M_PI); }

double SpatialUtils::haversineDistance(double lat1, double lng1, double lat2, double lng2) {
    double lat1r = degreesToRadians(lat1);
    double lon1r = degreesToRadians(lng1);
    double lat2r = degreesToRadians(lat2);
    double lon2r = degreesToRadians(lng2);
    double u = sin((lat2r - lat1r) / 2);
    double v = sin((lon2r - lon1r) / 2);
    return 2.0 * earthRadiusMeters * asin(sqrt(u * u + cos(lat1r) * cos(lat2r) * v * v));
}

}// namespace NES
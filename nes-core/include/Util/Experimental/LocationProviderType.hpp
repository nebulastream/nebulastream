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

#ifndef NES_UTIL_EXPERIMENTAL_LOCATIONPROVIDERTYPE_HPP
#define NES_UTIL_EXPERIMENTAL_LOCATIONPROVIDERTYPE_HPP

#include <string>
namespace NES::Spatial::Mobility::Experimental {
enum class LocationProviderType {
    BASE = 0,  // base class of location provider used for workers without a location and for field nodes
    CSV = 1,   //simulate location with coordinates read from csv
    INVALID = 2//the supplied configuration does not represent a valid provider type
};

LocationProviderType stringToLocationProviderType(std::string providerTypeString);

std::string toString(LocationProviderType providerType);

}// namespace NES::Spatial::Mobility::Experimental

#endif//NES_UTIL_EXPERIMENTAL_LOCATIONPROVIDERTYPE_HPP

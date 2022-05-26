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
#include <Util/Experimental/LocationProviderType.hpp>
namespace NES::Spatial::Mobility::Experimental {
LocationProviderType stringToLocationProviderType(const std::string providerTypeString) {
    if (providerTypeString == "BASE") {
        return LocationProviderType::BASE;
    } else if (providerTypeString == "CSV") {
        return LocationProviderType::CSV;
    }
    return LocationProviderType::INVALID;
}

std::string toString(const LocationProviderType providerType) {
    switch (providerType) {
        case LocationProviderType::BASE: return "BASE";
        case LocationProviderType::CSV: return "CSV";
        case LocationProviderType::INVALID: return "INVALID";
    }
}
}// namespace NES::Spatial::Mobility::Experimental

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
//todo: adjust path of include guards
#ifndef NES_LOCATIONSERVICE_HPP
#define NES_LOCATIONSERVICE_HPP

#include <memory>
namespace web::json {
class value;
}// namespace web::json

namespace NES::Spatial::Index {
class LocationIndex;
using LocationIndexPtr = std::shared_ptr<LocationIndex>;

class LocationService {
  public:
    LocationService(LocationIndexPtr locationWrapper);

    web::json::value requestLocationDataFromAllMobileNodesAsJson();
  private:
    LocationIndexPtr locationWrapper;
};
}

#endif//NES_LOCATIONSERVICE_HPP

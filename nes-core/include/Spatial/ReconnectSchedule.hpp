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
#ifndef NES_INCLUDE_SPATIAL_RECONNECTSCHEDULE_HPP
#define NES_INCLUDE_SPATIAL_RECONNECTSCHEDULE_HPP
#include <memory>
#include <Util/TimeMeasurement.hpp>
#include <vector>
namespace NES::Spatial::Index::Experimental {
class Location;
using LocationPtr = std::shared_ptr<Location>;
}

namespace NES::Spatial::Mobility::Experimental {

class ReconnectSchedule {
  public:
    ReconnectSchedule(Index::Experimental::LocationPtr pathBeginning, Index::Experimental::LocationPtr pathEnd,
                      std::shared_ptr<std::vector<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>>> reconnectVector);

    Index::Experimental::LocationPtr getPathStart();

    Index::Experimental::LocationPtr getPathEnd();

    std::shared_ptr<std::vector<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>>> getReconnectVector();

    static ReconnectSchedule Empty();
  private:
    Index::Experimental::LocationPtr pathStart;
    Index::Experimental::LocationPtr pathEnd;
    std::shared_ptr<std::vector<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>>> reconnectVector;
};
}

#endif//NES_INCLUDE_SPATIAL_RECONNECTSCHEDULE_HPP

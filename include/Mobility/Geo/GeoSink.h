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

#ifndef NES_GEOSINK_H
#define NES_GEOSINK_H

#include <Mobility/Geo/GeoArea.h>
#include <Mobility/Geo/GeoNode.h>

namespace NES {

class GeoSink: public GeoNode {

  private:
    double movingRangeArea;
    GeoAreaPtr movingRange;

  public:
    GeoSink(const string& id, double movingRangeArea);

    [[nodiscard]] const GeoAreaPtr& getMovingRange() const;

    void setCurrentLocation(const GeoPointPtr& currentLocation);
    virtual ~GeoSink();
};

using GeoSinkPtr = std::shared_ptr<GeoSink>;

}

#endif//NES_GEOSINK_H

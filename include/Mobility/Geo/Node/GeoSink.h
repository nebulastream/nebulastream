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

#include <vector>
#include <Mobility/Geo/Area/GeoArea.h>
#include <Mobility/Geo/Node/GeoNode.h>
#include <Mobility/Geo/Cartesian/CartesianLine.h>
#include <Mobility/Storage/PredictedSource.h>

namespace NES {

class GeoSink: public GeoNode {

  private:
    CartesianLinePtr trajectory;
    std::vector<PredictedSourcePtr> predictedSources;
    bool filterEnabled;

  public:
    explicit GeoSink(const string& id, double movingRangeArea, uint32_t storageSize);
    [[nodiscard]] const CartesianLinePtr& getTrajectory() const;
    void setCurrentLocation(const GeoPointPtr& currentLocation) override;
    void setTrajectory(const CartesianLinePtr& trajectory);
    [[nodiscard]] const std::vector<PredictedSourcePtr>& getPredictedSources() const;
    virtual ~GeoSink();
    void setPredictedSources(const std::vector<PredictedSourcePtr>& predictedSources);
    void setFilterEnabled(bool filterEnabled);
    bool isFilterEnabled() const;
};

using GeoSinkPtr = std::shared_ptr<GeoSink>;

}

#endif//NES_GEOSINK_H

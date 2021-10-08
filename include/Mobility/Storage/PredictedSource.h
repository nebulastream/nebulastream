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

#include <vector>
#include <Mobility/Geo/Node/GeoSource.h>

#ifndef NES_PREDICTEDSOURCE_H
#define NES_PREDICTEDSOURCE_H

namespace NES {

class PredictedSource {
    GeoSourcePtr source;
    bool inRange;
    std::vector<GeoSourcePtr> overlaps;

  public:
   explicit PredictedSource(const GeoSourcePtr& source);
   [[nodiscard]] const GeoSourcePtr& getSource() const;
   [[nodiscard]] bool isInRange() const;
   void setInRange(bool inRange);
   std::vector<GeoSourcePtr>& getOverlaps();
};

using PredictedSourcePtr = std::shared_ptr<PredictedSource>;

}

#endif//NES_PREDICTEDSOURCE_H

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

#ifndef NES_GEOSOURCE_H
#define NES_GEOSOURCE_H

#include <Mobility/Geo/GeoArea.h>
#include <Mobility/Geo/GeoNode.h>

namespace NES {

class GeoSource : public GeoNode {

  private:
    GeoAreaPtr range;
    bool hasRange;
    bool enabled;

  public:
    explicit GeoSource(const string& id);
    explicit GeoSource(const string& id, GeoAreaPtr  range);

    [[nodiscard]] const GeoAreaPtr& getRange() const;
    [[nodiscard]] bool isHasRange() const;
    [[nodiscard]] bool isEnabled() const;

    void setEnabled(bool flag);

    virtual ~GeoSource();
};

using GeoSourcePtr = std::shared_ptr<GeoSource>;

}

#endif//NES_GEOSOURCE_H

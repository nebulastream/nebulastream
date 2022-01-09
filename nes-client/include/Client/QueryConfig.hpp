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

#ifndef NES_INCLUDE_CLIENT_QUERYCONFIG_HPP_
#define NES_INCLUDE_CLIENT_QUERYCONFIG_HPP_

#include <Util/FaultToleranceType.hpp>
#include <Util/LineageType.hpp>
#include <Util/PlacementType.hpp>
#include <ostream>

namespace NES {

/**
 * @brief CPP client to deploy queries over the REST API
 */
class QueryConfig {
  public:
    QueryConfig(FaultToleranceType faultToleranceType = FaultToleranceType::NONE,
                LineageType lineageType = LineageType::NONE,
                PlacementType placementType = PlacementType::BottomUp);

    FaultToleranceType getFaultToleranceType() const;

    void setFaultToleranceType(FaultToleranceType faultToleranceType);

    LineageType getLineageType() const;

    void setLineageType(LineageType lineageType);

    PlacementType getPlacementType() const;

    void setPlacementType(PlacementType placementType);

  private:
    FaultToleranceType faultToleranceType;
    LineageType lineageType;
    PlacementType placementType;
};
}// namespace NES

#endif//NES_INCLUDE_CLIENT_QUERYCONFIG_HPP_
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

namespace NES::Client {

/**
 * @brief Interface to set configuration parameters for a query.
 */
class QueryConfig {
  public:
    QueryConfig(FaultToleranceType faultToleranceType = FaultToleranceType::NONE,
                LineageType lineageType = LineageType::NONE,
                PlacementType placementType = PlacementType::BottomUp);

    /**
     * @brief Returns the level of fault tolerance.
     * @return FaultToleranceType
     */
    FaultToleranceType getFaultToleranceType() const;

    /**
     * @brief Sets the level of fault tolerance.
     * @param faultToleranceType
     */
    void setFaultToleranceType(FaultToleranceType faultToleranceType);

    /**
     * @brief Returns the type of Linage.
     * @return LineageType
     */
    LineageType getLineageType() const;

    /**
     * @brief Sets the linage type
     * @param lineageType
     */
    void setLineageType(LineageType lineageType);

    /**
     * @brief Returns the placement type.
     * @return PlacementType
     */
    PlacementType getPlacementType() const;

    /**
     * @brief Sets the placement type
     * @param placementType
     */
    void setPlacementType(PlacementType placementType);

  private:
    FaultToleranceType faultToleranceType;
    LineageType lineageType;
    PlacementType placementType;
};
}// namespace NES

#endif//NES_INCLUDE_CLIENT_QUERYCONFIG_HPP_
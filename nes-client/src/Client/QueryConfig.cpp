#include <Client/QueryConfig.hpp>

namespace NES {


QueryConfig::QueryConfig(FaultToleranceType faultToleranceType, LineageType lineageType, PlacementType placementType)
    : faultToleranceType(faultToleranceType), lineageType(lineageType), placementType(placementType) {}


FaultToleranceType QueryConfig::getFaultToleranceType() const { return faultToleranceType; }

void QueryConfig::setFaultToleranceType(FaultToleranceType faultToleranceType) {
    QueryConfig::faultToleranceType = faultToleranceType;
}

LineageType QueryConfig::getLineageType() const { return lineageType; }

void QueryConfig::setLineageType(LineageType lineageType) { QueryConfig::lineageType = lineageType; }

PlacementType QueryConfig::getPlacementType() const { return placementType; }

void QueryConfig::setPlacementType(PlacementType placementType) { QueryConfig::placementType = placementType; }

}// namespace NES
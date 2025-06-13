#pragma once

#include <string>
#include <string_view>
#include <memory>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Serialization/SerializableFunction.pb.h>
#include <Runtime/Record.hpp>
#include <Runtime/Arena.hpp>
#include <MEOSWrapper.hpp>

namespace NES {

class TemporalIntersectsFunction final : public LogicalFunctionConcept {
public:
    static constexpr std::string_view NAME = "TemporalIntersects";

    TemporalIntersectsFunction(
        LogicalFunction lon,  // longitude
        LogicalFunction lat,  // latitude
        LogicalFunction ts  // timestamp
    );

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] DataType getDataType() const override;
    [[nodiscard]] LogicalFunction withDataType(const DataType& dataType) const override;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override;
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;
    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] SerializableFunction serialize() const override;
    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;
    [[nodiscard]] bool execute(Record& record, Arena& arena) const;
    
    // Spatial-temporal intersection method  
    [[nodiscard]] bool spatialIntersects(const double lon, const double lat, const double ts) const;

private:
    DataType dataType;
    LogicalFunction lon;
    LogicalFunction lat;
    LogicalFunction ts;
    MEOS::Meos meosWrapper;
}; 
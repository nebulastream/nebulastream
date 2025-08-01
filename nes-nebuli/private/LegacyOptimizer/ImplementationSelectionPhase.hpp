#pragma once
#include <Plans/LogicalPlan.hpp>
#include <QueryOptimizer.hpp>

namespace NES::LegacyOptimizer
{
/**
 * @brief This rule removes redundant projection, which project everything
 */
class ImplementationSelectionPhase
{
    NES::QueryExecutionConfiguration configuration;

public:
    explicit ImplementationSelectionPhase(const NES::QueryExecutionConfiguration& configuration) : configuration(configuration) { }
    void apply(LogicalPlan& queryPlan) const;
};
}

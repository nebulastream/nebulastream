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
#pragma once

#include <string>
#include <unordered_set>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{
/**
 * A library of utilities to make the validation of implemented rules easier.
 */
class OptimizerTestUtils
{
public:
    /// Returns a schema for testing. One field per name is generated, each type UINT64
    Schema<UnqualifiedUnboundField, Ordered> createSchema(const std::vector<std::string>& names);

    TypedLogicalOperator<SourceDescriptorLogicalOperator>
    createSource(std::string name, const Schema<UnqualifiedUnboundField, Ordered>& schema);
    TypedLogicalOperator<SourceDescriptorLogicalOperator> createSource(std::string name, const std::vector<std::string>& fieldNames);
    SourceDescriptor createSourceDescriptor(const Identifier& identifier, const Schema<UnqualifiedUnboundField, Ordered>& schema);


    TypedLogicalOperator<SinkLogicalOperator>
    createSink(LogicalOperator child, std::string name, const Schema<UnqualifiedUnboundField, Ordered>& schema);
    TypedLogicalOperator<SinkLogicalOperator>
    createSink(LogicalOperator child, std::string name, const std::vector<std::string>& fieldNames);
    LogicalPlan createPlan(LogicalOperator sink);
    /// Creates a plan with multiple sink roots. Sinks sharing (parts of) their subtrees form a DAG-shaped plan.
    LogicalPlan createPlan(std::vector<LogicalOperator> sinks);
    SinkDescriptor createSinkDescriptor(const Identifier& sinkName, const Schema<UnqualifiedUnboundField, Ordered>& schema);

    /// Collects all unique operators reachable from the plan roots. Operators shared between multiple parents (in a
    /// DAG-shaped plan) are visited once, so the size of the result detects accidental duplication of shared subtrees.
    static std::vector<LogicalOperator> collectOperators(const LogicalPlan& plan);

    /// Collects the ids of the operators returned by collectOperators.
    static std::unordered_set<OperatorId> collectOperatorIds(const LogicalPlan& plan);

    /// Builds a `producer.<fieldName> == 0` predicate over a UINT64 field; a common building block for rule tests.
    static LogicalFunction equalsZero(const LogicalOperator& producer, const std::string& fieldName);

private:
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
};
}

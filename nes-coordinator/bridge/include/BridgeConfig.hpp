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

#include <Configurations/ConfigLiteral.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

/// Helpers for the configuration that a frontend passes on to the coordinator and its workers.
/// A frontend reads worker and optimizer options together, for example from its command line.
/// The optimizer options go to the coordinator as JSON,
/// and the worker options become options of a CREATE WORKER statement.
namespace NES::Bridge
{

struct SplitConfig
{
    Schema<LiteralConfigValue, Ordered> optimizer;
    Schema<LiteralConfigValue, Ordered> worker;
};

/// Splits options into those under `optimizer` and all others, which belong to the worker.
SplitConfig splitOptimizerConfig(const Schema<LiteralConfigValue, Ordered>& literals);

/// Reads options from a JSON object of dotted names and values,
/// for example `{"worker.query_engine.number_of_worker_threads": "4"}`.
Schema<LiteralConfigValue, Ordered> literalsFromJson(const std::string& json);

/// Writes options as a JSON object of dotted names and values.
std::string literalsToJson(const Schema<LiteralConfigValue, Ordered>& literals);

/// Renders one option as it is written in a SET clause:
/// `'4' AS "worker"."query_engine"."number_of_worker_threads"`.
std::string configOption(const LiteralConfigValue& literal);

}

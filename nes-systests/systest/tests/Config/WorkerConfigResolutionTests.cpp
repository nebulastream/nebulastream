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

#include <utility>
#include <Config/WorkerConfigResolution.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerCatalogEntry.hpp>

namespace NES
{
namespace
{
WorkerCatalogEntry workerWith(Schema<LiteralConfigValue, Ordered> values)
{
    return {
        .host = Host{"localhost:8080"},
        .dataAddress = "localhost:9090",
        .maxOperators = CapacityKind::Unlimited{},
        .downstream = {},
        .config = std::move(values)};
}
}

TEST(WorkerConfigResolutionTest, TopologyOverridesRunAndTestOverridesTopology)
{
    const auto run = parseCommandLineConfig({"--worker.query_engine.number_of_worker_threads=1"});
    const auto topology = workerWith(parseCommandLineConfig({"--worker.query_engine.number_of_worker_threads=2"}));
    const auto test = parseCommandLineConfig({"--worker.query_engine.number_of_worker_threads=3"});
    EXPECT_EQ(makeWorkerConfigResolver(run, {}, true, "topology")(topology).workerConfiguration.queryEngine.numberOfWorkerThreads, 2);
    EXPECT_EQ(makeWorkerConfigResolver(run, test, true, "test")(topology).workerConfiguration.queryEngine.numberOfWorkerThreads, 3);
}

TEST(WorkerConfigResolutionTest, RejectsOverlapsByDefaultEvenWhenValuesAreEqual)
{
    const auto run = parseCommandLineConfig({"--worker.query_engine.number_of_worker_threads=2"});
    const auto topology = workerWith(run);
    EXPECT_THROW(makeWorkerConfigResolver(run, {}, false, "same values")(topology), Exception);
    EXPECT_THROW(makeWorkerConfigResolver({}, run, false, "same values")(topology), Exception);
}

TEST(WorkerConfigResolutionTest, DisjointLayersRetainEveryValue)
{
    const auto run = parseCommandLineConfig({"--worker.query_engine.number_of_worker_threads=2"});
    const auto topology = workerWith(parseCommandLineConfig({"--worker.query_engine.admission_queue_size=2048"}));
    const auto test = parseCommandLineConfig({"--enable_event_trace=true"});
    const auto result = makeWorkerConfigResolver(run, test, false, "disjoint")(topology);
    EXPECT_EQ(result.workerConfiguration.queryEngine.numberOfWorkerThreads, 2);
    EXPECT_EQ(result.workerConfiguration.queryEngine.admissionQueueSize, 2048);
    EXPECT_TRUE(result.enableGoogleEventTrace);
}

TEST(WorkerConfigResolutionTest, EveryWorkerUsesItsOwnTopology)
{
    const auto resolver = makeWorkerConfigResolver({}, {}, false, "per worker");
    EXPECT_EQ(
        resolver(workerWith(parseCommandLineConfig({"--worker.query_engine.number_of_worker_threads=2"})))
            .workerConfiguration.queryEngine.numberOfWorkerThreads,
        2);
    EXPECT_EQ(
        resolver(workerWith(parseCommandLineConfig({"--worker.query_engine.number_of_worker_threads=3"})))
            .workerConfiguration.queryEngine.numberOfWorkerThreads,
        3);
}

TEST(WorkerConfigResolutionTest, PermissionToOverwriteDoesNotPermitInvalidSettings)
{
    const auto test = parseCommandLineConfig({"--worker.query_engine.number_of_worker_threads=0"});
    EXPECT_THROW(makeWorkerConfigResolver({}, test, true, "invalid")(workerWith({})), Exception);
}
}

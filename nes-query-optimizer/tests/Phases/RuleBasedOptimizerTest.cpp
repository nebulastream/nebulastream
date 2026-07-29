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

#include <Phases/RuleBasedOptimizer.hpp>

#include <algorithm>
#include <memory>
#include <ranges>
#include <string_view>
#include <vector>

#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ModelCatalog.hpp>
#include <OptimizerTestUtils.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include "Configurations/Util.hpp"

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class RuleBasedOptimizerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("PlanVisitorTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

TEST_F(RuleBasedOptimizerTest, ValidateDisablingOfIndividualRules)
{
    const std::shared_ptr<const SourceCatalog> sourceCatalog;
    const std::shared_ptr<const SinkCatalog> sinkCatalog;
    const std::shared_ptr<const ModelCatalog> modelCatalog;

    const auto configuration1 = defaultConfiguration<QueryOptimizerConfiguration>();
    const RuleBasedOptimizer optimizer1{configuration1, sourceCatalog, sinkCatalog, modelCatalog};
    auto sequence1 = optimizer1.getRuleSequence();
    auto ruleNames1 = sequence1 | std::views::transform([](const auto& rule) { return rule.getName(); })
        | std::ranges::to<std::vector<std::string_view>>();
    EXPECT_TRUE(std::ranges::find(ruleNames1, "PredicatePushdownRule") != ruleNames1.end());

    const auto countAllRules = sequence1.size();

    auto configuration2 = defaultConfiguration<QueryOptimizerConfiguration>();
    configuration2.disabledRules.emplace_back("PredicatePushdown");
    const RuleBasedOptimizer optimizer2{configuration2, sourceCatalog, sinkCatalog, modelCatalog};
    auto sequence2 = optimizer2.getRuleSequence();
    auto ruleNames2 = sequence2 | std::views::transform([](const auto& rule) { return rule.getName(); })
        | std::ranges::to<std::vector<std::string_view>>();
    EXPECT_TRUE(std::ranges::find(ruleNames2, "PredicatePushdownRule") == ruleNames2.end());

    EXPECT_EQ(countAllRules - 1, sequence2.size());
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}

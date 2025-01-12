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

#include <typeinfo>
#include <unordered_map>

#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <private/Optimizer/Cost/CostFunctions.hpp>

#include <BaseUnitTest.hpp>

namespace NES::Optimizer {

class CostTest : public Testing::BaseUnitTest
{

public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("CostTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup CostTest test class.");
    }
};

TEST_F(CostTest, testHarcodedFunctions)
{
    auto statCatalog = StatisticsCatalog{};
    auto cardEstimator = RateEstimator<TupleTraitSet<QueryForSubtree>, StatisticsCatalog::RateStore>{statCatalog.getRateStore()};
    auto placementCost = PlacementCost<TupleTraitSet<QueryForSubtree>, decltype(cardEstimator)>{cardEstimator};

    // auto invalidCost = OuterCost<RecursiveTupleTraitSet<QueryForSubtree, Placement, Children>, decltype(placementCost)>{placementCost};
    // using TestType = RecursiveTupleTraitSet<QueryForSubtree, Children>;

    // auto testTS = TestType{TestType::EdgeTuple{std::vector<TestType>{}}, QueryForSubtree{"invalid"}};
    // invalidCost(testTS);

    // invalidCost(op);

    using TraitSetType = RecursiveTupleTraitSet<QueryForSubtree, Children, Parents, Placement>;
    const auto child
        = TraitSetType{TraitSetType::EdgeTuple{std::vector<TraitSetType>{}, std::vector<TraitSetType>{}}, QueryForSubtree{"child"}, Placement{4}};
    getEdges<Parents, TraitSetType>(child);
    //TODO add mutability of traits/edges

    const EdgeContainerTuple<TraitSetType, Children> children{std::vector{child}};

    const auto ts = TraitSetType{TraitSetType::EdgeTuple{std::vector{child}, std::vector<TraitSetType>{}}, QueryForSubtree{"test"}, Placement{5}};


    auto cost = placementCost(ts);

    NES_INFO("Placement Cost {}", cost);
}
}

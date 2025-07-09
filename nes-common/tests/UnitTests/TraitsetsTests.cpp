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

#include <Traitsets/Traitsets.hpp>

#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

template <NES::Traitsets::Trait... T>
struct std::hash<NES::Traitsets::TupleTraitSet<T...>>
{
    std::size_t operator()(const NES::Traitsets::TupleTraitSet<T...>) const { return 2; }
};

namespace NES::Traitsets{

class QueryForSubtree
{
    const std::string str;

public:
    explicit QueryForSubtree(std::string str) : str(std::move(str)) { }
    bool operator==(const QueryForSubtree&) const { return true; }
    static bool atNode() { return false; }

    static std::string getName() { return "QueryForSubtree"; }

    const std::string& getQuery() const { return str; }
};

static_assert(Trait<QueryForSubtree>);

class Placement
{
    const int nodeID;

public:
    explicit Placement(int nodeID) : nodeID(nodeID) { }
    bool operator==(const Placement& other) const { return nodeID == other.nodeID; }
    static constexpr bool atNode() { return true; }
    static std::string getName() { return "Placement"; }
};

static_assert(Trait<Placement>);

struct TestOperator
{
    const Placement placement;

    template <typename T>
    T get() = delete;

    // template <>
    // Placement get<Placement>()
    // {
    //     return placement;
    // }

    template <>
    QueryForSubtree get<QueryForSubtree>()
    {
        return QueryForSubtree{"Test Operator"};
    }

    // Placement get() { return Placement{1}; };
    // explicit operator Placement() const {
    //     return placement;
    // }
};

// template <>
// inline Placement get<TestOperator, Placement>(TestOperator op)
// {
//     return op.placement;
// }

// static_assert(hasGetter<TestOperator, Placement>);
//
//
// static_assert(TraitSet<RecursiveTupleTraitSet<Placement, Children>, Placement, Children>);
// static_assert(TraitSet<TestOperator, Placement>);

// static_assert(hasGetter<TupleTraitSet<Placement>, Placement>::value::value);

class Operator
{
public:
    bool operator==(const Operator&) const { return true; }
    static constexpr bool atNode() { return true; }
    static std::string getName() { return "Operator"; }
};

static_assert(Trait<Operator>);

// template <typename T, typename... Ts>
// struct PackContains
// {
//     static constexpr bool value{(std::is_same_v<T, Ts> || ...)};
// };
//
// //Needs to be more controllable
// template <typename TI, typename TO>
// TO derive(TI);
//
// template <Trait... TIs, Trait... TOs>
// TraitSet<TOs...> derive(TIs... tis)
// {
//     return TraitSet<TOs...>{(PackContains<TOs, TIs...>::value, ...)};
// }
//
//
// template <StatisticsValue SV, Trait... TIs, CostFunction<SV, TIs...> C, Trait... TOs>
// SV C::operator()(TraitSet<TIs...> tis)
// {
//     C(derive(tis));
// }


class StatisticsCatalog
{
    std::unordered_map<TupleTraitSet<QueryForSubtree>, int> rates;
    std::unordered_map<TupleTraitSet<Placement>, float> memoryUsage;

public:
    explicit StatisticsCatalog() { };


    class RateStore
    {
        friend StatisticsCatalog;
        std::unordered_map<TupleTraitSet<QueryForSubtree>, int>& rates;
        explicit RateStore(std::unordered_map<TupleTraitSet<QueryForSubtree>, int>& rates) : rates(rates) { }

    public:
        std::optional<int> operator()(TupleTraitSet<QueryForSubtree> ts) const
        {
            if (rates.contains(ts))
            {
                return rates.at(ts);
            }
            return std::nullopt;
        }
    };

private:
    RateStore rateStore = RateStore{rates};

public:
    RateStore& getRateStore() { return rateStore; }


    std::optional<float> getMemoryUsage(TupleTraitSet<Placement> ts)
    {
        if (memoryUsage.contains(ts))
        {
            return memoryUsage.at(ts);
        }
        return std::nullopt;
    }
};

static_assert(OptionalCostFunction<StatisticsCatalog::RateStore, int, TupleTraitSet<QueryForSubtree>>);


template <TraitSet<QueryForSubtree> TS, OptionalCostFunction<int, TS> StatisticsCatalogCost>
class RateEstimator
{
    StatisticsCatalogCost statisticsFunction;

public:
    explicit RateEstimator(StatisticsCatalogCost statisticsBaseFunction) : statisticsFunction(statisticsBaseFunction) { }

    int operator()(TS ts)
    {
        if (const std::optional<int> statistic = statisticsFunction(ts))
        {
            return *statistic;
        }
        return 20;
    }
};

template <TraitSet<QueryForSubtree> TS, CostFunction<int, TS> RateEstimator>
class PlacementCost
{
    RateEstimator rateEstimator;

public:
    explicit PlacementCost(RateEstimator rateEstimator) : rateEstimator(rateEstimator) { }

    // template <TraitSet<QueryForSubtree, Placement, Children> TSI>
    int operator()(TraitSet<QueryForSubtree, Placement, Children> auto ts)
    {
        for (auto child : getEdges<Children>(ts))
        {
            NES_INFO("Child query: {}", get<QueryForSubtree>(child).getQuery());
        }
        auto derived = TupleTraitSet<QueryForSubtree>{get<QueryForSubtree>(ts)};
        auto subtree = get<QueryForSubtree>(ts);
        const int rate = rateEstimator(derived);
        return rate;
    }
};

//just for testing and playing around
template <TraitSet<QueryForSubtree, Placement> TS, CostFunction<int, TS> RateEstimator>
class OuterCost
{
    RateEstimator rateEstimator;

public:
    explicit OuterCost(RateEstimator rateEstimator) : rateEstimator(rateEstimator) { }

    // template <TraitSet<QueryForSubtree, Children> TSI>
    // int operator()(TSI ts)
    // {
    //     for (TSI child : getEdges<TSI, Children>(ts))
    //     {
    //         NES_INFO("Child query: {}", get<TSI, QueryForSubtree>(child).getQuery());
    //     }
    //     auto derived = TupleTraitSet<QueryForSubtree>{get<TSI, QueryForSubtree>(ts)};
    //     auto subtree = get<TSI, QueryForSubtree>(ts);
    //     const int rate = rateEstimator(derived);
    //     return rate;
    // }
    // template <TraitSet<QueryForSubtree, Children> TSI>
    int operator()(const TraitSet<QueryForSubtree, Children> auto& ts)
    {
        for (auto child : getEdges<Children>(ts))
        {
            NES_INFO("Child query: {}", get<QueryForSubtree>(child).getQuery());
        }
        auto derived = TupleTraitSet<QueryForSubtree>{get<QueryForSubtree>(ts)};
        auto subtree = get<QueryForSubtree>(ts);
        const int rate = rateEstimator(derived);
        return rate;
    }
};

class AbstractRewriteRule
{
    virtual VirtualTraitSet* apply(VirtualTraitSet*);
};

template <Trait... T>
class TypedAbstractRewriteRule : AbstractRewriteRule
{
    VirtualTraitSet* apply(VirtualTraitSet* inputTS) override { applyTyped(DynamicTraitSet<T...>{inputTS}); };

    virtual DynamicTraitSet<T...>* applyTyped(DynamicTraitSet<T...>*) = 0;
};

class LowerToPhysicalSelection : TypedAbstractRewriteRule<QueryForSubtree, Operator>
{
    DynamicTraitSet<QueryForSubtree, Operator>* applyTyped(DynamicTraitSet<QueryForSubtree, Operator>*) override { return nullptr; };
};


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

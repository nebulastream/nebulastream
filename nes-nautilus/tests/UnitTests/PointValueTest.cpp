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

#include <map>
#include <string>
#include <utility>
#include <Nautilus/DataTypes/Value.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <val.hpp>
#include <val_arith.hpp>

namespace NES
{

/// Prototype-level test of the compound-Value abstraction using a hypothetical
/// Point(x, y, z) datatype. Demonstrates that the unified physical-level Value
/// type supports both scalar and compound values via a suffix-keyed component
/// map, and that primitive arithmetic flows through a compound's components
/// independently — which is what a decomposable lowering of a Point function
/// (Point.add, Point.sub, Point.scale) would emit at runtime.
class PointValueTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("PointValueTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup PointValueTest class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down PointValueTest class."); }

    /// Build a compound Value with components "_x", "_y", "_z" from FLOAT64
    /// scalars — the lowered shape of a `Point(x, y, z)` constructor function.
    static Value makePoint(const double x, const double y, const double z)
    {
        std::map<std::string, VarVal> components;
        components.emplace("_x", VarVal{nautilus::val<double>(x)});
        components.emplace("_y", VarVal{nautilus::val<double>(y)});
        components.emplace("_z", VarVal{nautilus::val<double>(z)});
        return Value{std::move(components)};
    }

    /// `nautilus::val<T>` has `operator==` against raw `T`, but no public way
    /// to extract the raw value as `T` directly. Tests assert via that operator.
};

TEST_F(PointValueTest, ScalarValueRoundTrip)
{
    /// A scalar Value is the degenerate single-component case keyed by SCALAR_KEY.
    const Value scalarValue = Value::scalar(VarVal{nautilus::val<double>(3.14)});
    EXPECT_TRUE(scalarValue.isScalar());
    EXPECT_EQ(scalarValue.components().size(), 1U);
    EXPECT_EQ(scalarValue.components().begin()->first, std::string{Value::SCALAR_KEY});
    EXPECT_EQ(scalarValue.getRawValueAs<nautilus::val<double>>(), 3.14);
}

TEST_F(PointValueTest, CompoundPointConstruction)
{
    /// A Point Value carries three suffix-keyed components, none of them scalar.
    const Value point = makePoint(1.0, 2.0, 3.0);
    EXPECT_FALSE(point.isScalar());
    EXPECT_EQ(point.components().size(), 3U);

    EXPECT_EQ(point.component("_x").getRawValueAs<nautilus::val<double>>(), 1.0);
    EXPECT_EQ(point.component("_y").getRawValueAs<nautilus::val<double>>(), 2.0);
    EXPECT_EQ(point.component("_z").getRawValueAs<nautilus::val<double>>(), 3.0);
}

TEST_F(PointValueTest, PointComponentsIterateInSuffixOrder)
{
    /// std::map ordering means components iterate by suffix; this is the order
    /// Record::write spreads them into primitive record fields.
    const Value point = makePoint(10.0, 20.0, 30.0);
    auto it = point.components().begin();
    EXPECT_EQ(it->first, "_x");
    ++it;
    EXPECT_EQ(it->first, "_y");
    ++it;
    EXPECT_EQ(it->first, "_z");
}

TEST_F(PointValueTest, PointAddIsComponentWiseScalarAdd)
{
    /// What a decomposable lowering of Point.add(P1, P2) emits at runtime:
    /// for component i, run scalar AddPhysicalFunction on the i-th components.
    const Value left = makePoint(1.0, 2.0, 3.0);
    const Value right = makePoint(4.0, 5.0, 6.0);

    std::map<std::string, VarVal> sumComponents;
    for (const auto& [suffix, leftComponent] : left.components())
    {
        const auto& rightComponent = right.component(suffix);
        sumComponents.emplace(suffix, (leftComponent + rightComponent).castToType(DataType::Type::FLOAT64));
    }
    const Value sum{std::move(sumComponents)};

    EXPECT_EQ(sum.component("_x").getRawValueAs<nautilus::val<double>>(), 5.0);
    EXPECT_EQ(sum.component("_y").getRawValueAs<nautilus::val<double>>(), 7.0);
    EXPECT_EQ(sum.component("_z").getRawValueAs<nautilus::val<double>>(), 9.0);
}

TEST_F(PointValueTest, PointSubIsComponentWiseScalarSub)
{
    const Value left = makePoint(10.0, 10.0, 10.0);
    const Value right = makePoint(1.0, 2.0, 3.0);

    std::map<std::string, VarVal> diffComponents;
    for (const auto& [suffix, leftComponent] : left.components())
    {
        diffComponents.emplace(suffix, (leftComponent - right.component(suffix)).castToType(DataType::Type::FLOAT64));
    }
    const Value diff{std::move(diffComponents)};

    EXPECT_EQ(diff.component("_x").getRawValueAs<nautilus::val<double>>(), 9.0);
    EXPECT_EQ(diff.component("_y").getRawValueAs<nautilus::val<double>>(), 8.0);
    EXPECT_EQ(diff.component("_z").getRawValueAs<nautilus::val<double>>(), 7.0);
}

TEST_F(PointValueTest, PointScaleBroadcastsScalarFactor)
{
    /// Point.scale(p, 2.0) lowers to: for each i, MulPhysicalFunction(p_i, 2.0).
    const Value point = makePoint(1.0, 2.0, 3.0);
    const VarVal factor{nautilus::val<double>(2.0)};

    std::map<std::string, VarVal> scaledComponents;
    for (const auto& [suffix, component] : point.components())
    {
        scaledComponents.emplace(suffix, (component * factor).castToType(DataType::Type::FLOAT64));
    }
    const Value scaled{std::move(scaledComponents)};

    EXPECT_EQ(scaled.component("_x").getRawValueAs<nautilus::val<double>>(), 2.0);
    EXPECT_EQ(scaled.component("_y").getRawValueAs<nautilus::val<double>>(), 4.0);
    EXPECT_EQ(scaled.component("_z").getRawValueAs<nautilus::val<double>>(), 6.0);
}

TEST_F(PointValueTest, PointDistanceIsCompoundInputScalarOutput)
{
    /// Point.distance(p, q) is an atomic compound function: it reads all 3
    /// components of each operand and emits a single scalar Value.
    /// Acceptance criterion in the prototype plan: distance from origin of
    /// (1, 2, 3) is sqrt(14).
    const Value point = makePoint(1.0, 2.0, 3.0);
    const Value origin = makePoint(0.0, 0.0, 0.0);

    VarVal sumOfSquares{nautilus::val<double>(0.0)};
    for (const auto& [suffix, leftComponent] : point.components())
    {
        const auto delta = (leftComponent - origin.component(suffix)).castToType(DataType::Type::FLOAT64);
        sumOfSquares = (sumOfSquares + (delta * delta)).castToType(DataType::Type::FLOAT64);
    }

    const Value distance = Value::scalar(sumOfSquares);
    EXPECT_TRUE(distance.isScalar());
    /// We didn't run sqrt through nautilus here — assert on the squared sum.
    EXPECT_EQ(distance.getRawValueAs<nautilus::val<double>>(), 14.0);
}

TEST_F(PointValueTest, ScalarValueImplicitVarValBoundary)
{
    /// The migration boundary helper: a scalar Value implicitly converts back
    /// to a const VarVal&, so existing VarVal-typed memory I/O and aggregation
    /// state machinery continues to work without per-line surgery. Compound
    /// Values must not flow through this boundary.
    const Value scalarValue = Value::scalar(VarVal{nautilus::val<double>(7.0)});
    const VarVal& asVarVal = scalarValue;
    EXPECT_EQ(asVarVal.getRawValueAs<nautilus::val<double>>(), 7.0);
}

}

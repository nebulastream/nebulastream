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

#include <ConditionalPhysicalFunction.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/BufferManager.hpp>
#include <gtest/gtest.h>
#include <Arena.hpp>
#include <Engine.hpp>

namespace NES
{

class ConditionalPhysicalFunctionTest : public testing::TestWithParam<bool>
{
public:
    void SetUp() override
    {
        compilation = GetParam();
        nautilus::engine::Options options;
        options.setOption("engine.Compilation", compilation);
        engine = std::make_unique<nautilus::engine::NautilusEngine>(options);
        bm = BufferManager::create();
    }

    /// Executes any PhysicalFunction within the nautilus engine and returns the result cast to T.
    template <typename T = int64_t>
    T execute(const PhysicalFunction& fn)
    {
        Arena arena{bm};
        auto function = engine->registerFunction(std::function(
            [&]()
            {
                ArenaRef arenaRef(&arena);
                const Record r{};
                return fn.execute(r, arenaRef).cast<nautilus::val<T>>();
            }));
        return function();
    }

    /// Builds a ConditionalPhysicalFunction from raw PhysicalFunction inputs.
    /// Layout: [cond1, then1, cond2, then2, ..., elseVal]
    static PhysicalFunction conditional(std::vector<PhysicalFunction> fns) { return ConditionalPhysicalFunction(std::move(fns)); }

    bool compilation{};
    std::unique_ptr<nautilus::engine::NautilusEngine> engine;
    std::shared_ptr<BufferManager> bm;
};

/// WHEN true THEN 42 ELSE 99 -> returns int64_t THEN value.
TEST_P(ConditionalPhysicalFunctionTest, Int64ConditionTrue)
{
    auto fn = conditional({ConstantBooleanValueFunction(true), ConstantInt64ValueFunction(42), ConstantInt64ValueFunction(99)});
    EXPECT_EQ(execute(fn), 42);
}

/// WHEN false THEN 3.14 ELSE 2.72 -> returns double ELSE value.
TEST_P(ConditionalPhysicalFunctionTest, DoubleFallsToElse)
{
    auto fn = conditional({ConstantBooleanValueFunction(false), ConstantDoubleValueFunction(3.14), ConstantDoubleValueFunction(2.72)});
    EXPECT_DOUBLE_EQ(execute<double>(fn), 2.72);
}

/// WHEN true THEN 1.5f ELSE 0.0f -> returns float THEN value.
TEST_P(ConditionalPhysicalFunctionTest, FloatConditionTrue)
{
    auto fn = conditional({ConstantBooleanValueFunction(true), ConstantFloatValueFunction(1.5f), ConstantFloatValueFunction(0.0f)});
    EXPECT_FLOAT_EQ(execute<float>(fn), 1.5f);
}

/// Multi-branch with int32_t: second condition matches.
TEST_P(ConditionalPhysicalFunctionTest, Int32SecondBranchMatches)
{
    auto fn = conditional(
        {ConstantBooleanValueFunction(false),
         ConstantInt32ValueFunction(10),
         ConstantBooleanValueFunction(true),
         ConstantInt32ValueFunction(20),
         ConstantInt32ValueFunction(30)});
    EXPECT_EQ(execute<int32_t>(fn), 20);
}

/// Five branches, middle matches -> verifies short-circuit with many static_val iterations.
TEST_P(ConditionalPhysicalFunctionTest, ManyBranchesMiddleMatches)
{
    auto fn = conditional(
        {ConstantBooleanValueFunction(false),
         ConstantInt64ValueFunction(100),
         ConstantBooleanValueFunction(false),
         ConstantInt64ValueFunction(200),
         ConstantBooleanValueFunction(true),
         ConstantInt64ValueFunction(300),
         ConstantBooleanValueFunction(true),
         ConstantInt64ValueFunction(400),
         ConstantBooleanValueFunction(true),
         ConstantInt64ValueFunction(500),
         ConstantInt64ValueFunction(-1)});
    EXPECT_EQ(execute(fn), 300);
}

/// The condition itself is a nested conditional that returns bool.
/// condition = (WHEN true THEN true ELSE false), value -> THEN 42 ELSE 99
TEST_P(ConditionalPhysicalFunctionTest, NestedCondition)
{
    auto nestedCond
        = conditional({ConstantBooleanValueFunction(true), ConstantBooleanValueFunction(true), ConstantBooleanValueFunction(false)});

    auto fn = conditional({std::move(nestedCond), ConstantInt64ValueFunction(42), ConstantInt64ValueFunction(99)});
    EXPECT_EQ(execute(fn), 42);
}

/// The condition is a nested conditional that evaluates to false.
/// condition = (WHEN false THEN true ELSE false) -> false, so returns ELSE.
TEST_P(ConditionalPhysicalFunctionTest, NestedConditionEvaluatesToFalse)
{
    auto nestedCond
        = conditional({ConstantBooleanValueFunction(false), ConstantBooleanValueFunction(true), ConstantBooleanValueFunction(false)});

    auto fn = conditional({std::move(nestedCond), ConstantDoubleValueFunction(1.0), ConstantDoubleValueFunction(2.0)});
    EXPECT_DOUBLE_EQ(execute<double>(fn), 2.0);
}

/// Both condition and result are nested conditionals.
/// condition = (WHEN true THEN true ELSE false) -> true
/// then = (WHEN false THEN 10 ELSE 20) -> 20
/// else = 30
TEST_P(ConditionalPhysicalFunctionTest, NestedConditionAndNestedResult)
{
    auto nestedCond
        = conditional({ConstantBooleanValueFunction(true), ConstantBooleanValueFunction(true), ConstantBooleanValueFunction(false)});
    auto nestedThen = conditional({ConstantBooleanValueFunction(false), ConstantInt64ValueFunction(10), ConstantInt64ValueFunction(20)});

    auto fn = conditional({std::move(nestedCond), std::move(nestedThen), ConstantInt64ValueFunction(30)});
    EXPECT_EQ(execute(fn), 20);
}

/// Deeply nested: condition is a conditional whose own condition is also a conditional.
/// innerCond = (WHEN true THEN true ELSE false) -> true
/// outerCond = (WHEN innerCond THEN false ELSE true) -> false
/// top-level: WHEN outerCond THEN 1 ELSE 2 -> 2
TEST_P(ConditionalPhysicalFunctionTest, DeeplyNestedCondition)
{
    auto innerCond
        = conditional({ConstantBooleanValueFunction(true), ConstantBooleanValueFunction(true), ConstantBooleanValueFunction(false)});
    auto outerCond = conditional({std::move(innerCond), ConstantBooleanValueFunction(false), ConstantBooleanValueFunction(true)});

    auto fn = conditional({std::move(outerCond), ConstantInt64ValueFunction(1), ConstantInt64ValueFunction(2)});
    EXPECT_EQ(execute(fn), 2);
}

INSTANTIATE_TEST_SUITE_P(
    ConditionalPhysicalFunctionTest,
    ConditionalPhysicalFunctionTest,
    ::testing::Values(false, true),
    [](const testing::TestParamInfo<bool>& info) { return info.param ? "Compiled" : "Interpreted"; });

}

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

#include <Rules/Static/ConstantFoldingRule.hpp>

#include <DataTypes/DataType.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>

#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/PowLogicalFunction.hpp>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class ConstantFoldingRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ConstantFoldingRuleTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

TEST_F(ConstantFoldingRuleTest, simpleAdditionInSelection)
{
    /// BEFORE: Sink < Selection(a = 2+2) < Source
    /// AFTER: Sink < Selection(a = 4) < Source

    auto source = utils.createSource("simpleAdditionSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        AddLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}
    });

    auto sink = utils.createSink(select, "simpleAdditionSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "4");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());

}

TEST_F(ConstantFoldingRuleTest, nestedAdditionInSelection)
{
    /// BEFORE: Sink < Selection(a = (2 + (1+2))) < Source
    /// AFTER: Sink < Selection(a = 5) < Source

    auto source = utils.createSource("nestedAdditionSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        AddLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"},
            AddLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "1"},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}}
    });

    auto sink = utils.createSink(select, "nestedAdditionSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "5");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(ConstantFoldingRuleTest, noFoldablePredicate)
{
    /// BEFORE: Sink < Selection(a = 2) < Source
    /// AFTER: Sink < Selection(a = 2) < Source
    auto source = utils.createSource("noFoldingSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}
    });

    auto sink = utils.createSink(select, "noFoldingSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(0).tryGetAs<FieldAccessLogicalFunction>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
}

TEST_F(ConstantFoldingRuleTest, simpleMultiplication)
{
    /// BEFORE: Sink < Selection(a = 2*2) < Source
    /// AFTER: Sink < Selection(a = 4) < Source
    auto source = utils.createSource("simpleMultSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        MulLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}
    });

    auto sink = utils.createSink(select, "simpleMultSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "4");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(ConstantFoldingRuleTest, divisionWithNull)
{
    /// BEFORE: Sink < Selection(a = 1/0) < Source
    /// AFTER: Error
    auto source = utils.createSource("simpleDivSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        DivLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "1"},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}}
    });

    auto sink = utils.createSink(select, "simpleDivSink", {"a"});
    auto plan = utils.createPlan(sink);

    EXPECT_ANY_THROW(auto cleaned = ConstantFoldingRule{}.apply(plan));
}

TEST_F(ConstantFoldingRuleTest, integerDivWouldResultInFloat)
{
    /// BEFORE: Sink < Selection(a = 1/2) < Source
    /// AFTER: Sink < Selection(a = 0) < Source
    auto source = utils.createSource("integerDivToFloatSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        DivLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "1"},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}
    });
    auto sink = utils.createSink(select, "integerDivToFloatSink", {"a"});
    auto plan = utils.createPlan(sink);
    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "0");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(ConstantFoldingRuleTest, floatDivision)
{
    /// BEFORE: Sink < Selection(a = 1/2) < Source
    /// AFTER: Sink < Selection(a = 0.500000) < Source
    auto source = utils.createSource("floatDivSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        DivLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE}, "1"},
            ConstantValueLogicalFunction{DataType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}
    });
    auto sink = utils.createSink(select, "floatDivSink", {"a"});
    auto plan = utils.createPlan(sink);
    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "0.500000");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(ConstantFoldingRuleTest, simpleSubtraction)
{
    /// BEFORE: Sink < Selection(a = 2-1) < Source
    /// AFTER: Sink < Selection(a = 1) < Source
    auto source = utils.createSource("simpleSubSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        SubLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "1"}}
    });

    auto sink = utils.createSink(select, "simpleSubSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "1");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

// in NES there are no checks to prevent underflow when subtracting differently sized integers,
// this test is analogue to the behaviour in the systests
TEST_F(ConstantFoldingRuleTest, integerSubResultsInUnderflow)
{
    /// BEFORE: Sink < Selection(a = 1-2) < Source
    /// AFTER: Sink < Selection(a = 18446744073709551615) < Source
    auto source = utils.createSource("underflowSubSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        SubLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT32, DataType::NULLABLE::NOT_NULLABLE}, "1"},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT32, DataType::NULLABLE::NOT_NULLABLE}, "2"}}
    });

    auto sink = utils.createSink(select, "underflowSubSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "18446744073709551615");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(ConstantFoldingRuleTest, moduloWithFloat)
{
    /// BEFORE: Sink < Selection(a = 1.0 MOD 1.0) < Source
    /// AFTER: Error
    auto source = utils.createSource("moduloFloatSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        ModuloLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}, "1.0"},
            ConstantValueLogicalFunction{DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}, "1.0"}}
    });

    auto sink = utils.createSink(select, "moduloFloatSink", {"a"});
    auto plan = utils.createPlan(sink);

    EXPECT_ANY_THROW(auto cleaned = ConstantFoldingRule{}.apply(plan));
}

TEST_F(ConstantFoldingRuleTest, integerModulo)
{
    /// BEFORE: Sink < Selection(a = 6 MOD 2) < Source
    /// AFTER: Sink < Selection(a = 0) < Source
    auto source = utils.createSource("moduloIntSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        ModuloLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "6"},
            ConstantValueLogicalFunction{DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "2"}}
    });

    auto sink = utils.createSink(select, "moduloIntSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "0");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(ConstantFoldingRuleTest, intPow)
{
    /// BEFORE: Sink < Selection(a = 6²) < Source
    /// AFTER: Sink < Selection(a = 36) < Source
    auto source = utils.createSource("powIntSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        PowLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "6"},
            ConstantValueLogicalFunction{DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "2"}}
    });

    auto sink = utils.createSink(select, "powIntSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "36");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(ConstantFoldingRuleTest, floatPow)
{
    /// BEFORE: Sink < Selection(a = 4.0²) < Source
    /// AFTER: Sink < Selection(a = 16.000000) < Source
    auto source = utils.createSource("powFloatSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        PowLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}, "4.0"},
            ConstantValueLogicalFunction{DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "2"}}
    });

    auto sink = utils.createSink(select, "powFloatSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "16.000000");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}


//TODO float 16 + 32

/*
 // already throws error when creating the select operator in the original query
TEST_F(ConstantFoldingRuleTest, noneNumericValue)
{
    auto source = utils.createSource("noneNumericValue", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        AddLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "a"},
            ConstantValueLogicalFunction{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "b"}}
    });
    auto sink = utils.createSink(select, "Sink", {"a"});
    auto plan = utils.createPlan(sink);
    EXPECT_ANY_THROW(auto cleaned = ConstantFoldingRule{}.apply(plan));
}*/

/// NOLINTEND(bugprone-unchecked-optional-access)
}

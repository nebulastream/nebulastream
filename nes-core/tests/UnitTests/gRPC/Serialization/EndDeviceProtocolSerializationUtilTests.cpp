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
#include <EndDeviceProtocol.pb.h>
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <NesBaseTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Sources/LoRaWANProxySource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LoRaWANProxySourceType.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LoRaWANProxySourceDescriptor.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>

#include <GRPC/Serialization/EndDeviceProtocolSerializationUtil.hpp>

namespace NES {
using namespace EndDeviceProtocol;
class EndDeviceProtocolSerializationUtilTests : public Testing::NESBaseTest {
    //friend class EndDeviceProtocolSerializationUtil;

  public:
    /***
     * Helper function to quickly create strings out of bytes. Used to create expected expressions.
     * @param list list of 8 bit ints
     * @return string corresponding to the binary encoding of the given list
     */
    static std::string intsToString(std::initializer_list<char> list) {
        auto expected_instructions = std::string();
        for (char c : list) {
            expected_instructions.push_back(c);
        }
        return expected_instructions;
    }

  protected:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("EndDeviceProtocolSerializationUtilTests.log", NES::LogLevel::LOG_DEBUG);
    }
};

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValue) {
    // Test constant Value serialisation. Right now we only support 8bit numbers, so we only test those.
    // This should be changed in later versions
    auto int8 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 0)));

    std::string ser_int8 = EndDeviceProtocolSerializationUtil::serializeConstantValue(int8);

    EXPECT_TRUE(ser_int8 == "\0\0\0"s);
}

TEST_F(EndDeviceProtocolSerializationUtilTests, FieldAccess) {
    auto fax = FieldAccessExpressionNode::create("x");
    auto fay = FieldAccessExpressionNode::create("y");

    auto registers = std::make_shared<std::vector<std::string>>(1, "x");

    auto expected = intsToString({ExpressionInstructions::VAR, 0});
    EXPECT_EQ(EndDeviceProtocolSerializationUtil::serializeFieldAccessExpression(fax, registers), expected);

    EXPECT_THROW(auto ignored = EndDeviceProtocolSerializationUtil::serializeFieldAccessExpression(fay, registers),
                 EndDeviceProtocolSerializationUtil::UnsupportedEDSerialisationException);
}

TEST_F(EndDeviceProtocolSerializationUtilTests, Arithmetic) {
    //Test serialization of arithmetic operators.
    //First set up Query
    auto left = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 2)));
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 10)));
    auto add = AddExpressionNode::create(left, right);

    //Set up registers
    auto registers = std::make_shared<std::vector<std::string>>();

    //Serialize
    auto res = EndDeviceProtocolSerializationUtil::serializeArithmeticalExpression(add, registers);

    //build expected string
    auto expected = intsToString({ExpressionInstructions::CONST,
                                  DataTypes::INT8,
                                  2,
                                  ExpressionInstructions::CONST,
                                  DataTypes::INT8,
                                  10,
                                  ExpressionInstructions::ADD});

    EXPECT_EQ(res, expected);
}

TEST_F(EndDeviceProtocolSerializationUtilTests, Logical) {
    auto registers = std::make_shared<std::vector<std::string>>(std::vector<std::string>{"x", "y"});

    auto left1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, "\x12"s));
    auto right1 = FieldAccessExpressionNode::create("x");
    auto eq = EqualsExpressionNode::create(left1, right1);
    auto neg = NegateExpressionNode::create(eq);

    auto left2 = FieldAccessExpressionNode::create("y");
    auto right2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, "\x01"));
    auto gt = GreaterExpressionNode::create(left2, right2);

    auto bor = OrExpressionNode::create(neg, gt);

    auto expected = intsToString({ExpressionInstructions::CONST,
                                  DataTypes::INT8,
                                  0x12,
                                  ExpressionInstructions::VAR,
                                  0,
                                  ExpressionInstructions::EQ,
                                  ExpressionInstructions::NOT,
                                  ExpressionInstructions::VAR,
                                  1,
                                  ExpressionInstructions::CONST,
                                  DataTypes::INT8,
                                  0x01,
                                  ExpressionInstructions::GT,
                                  ExpressionInstructions::OR});

    auto res = EndDeviceProtocolSerializationUtil::serializeLogicalExpression(bor, registers);
    EXPECT_EQ(res, expected);
}

TEST_F(EndDeviceProtocolSerializationUtilTests, MapOperation) {
    //set up query. Note that we do a field access read
    auto left = FieldAccessExpressionNode::create("var");
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 10)));
    auto add = AddExpressionNode::create(left, right);

    //set up mapexpression. Note that we save result to a new variable, which should be reflected as a new field in the register-map
    auto mapexpr =
        FieldAssignmentExpressionNode::create(FieldAccessExpressionNode::create("x")->as<FieldAccessExpressionNode>(), add);

    auto map = LogicalOperatorFactory::createMapOperator(mapexpr);

    //set up registers with pre-defined "var" there for the field access
    auto registers = std::make_shared<std::vector<std::string>>();
    registers->push_back("var");

    //serialize
    auto ser_map = EndDeviceProtocolSerializationUtil::serializeMapOperator(map, registers);

    //build expected expression...
    auto expected_instructions = intsToString(
        {ExpressionInstructions::VAR, 0, ExpressionInstructions::CONST, DataTypes::INT8, 10, ExpressionInstructions::ADD});

    //... and MapOperation
    auto expected_map = MapOperation();
    expected_map.set_attribute(1);
    expected_map.mutable_function()->set_instructions(expected_instructions);

    //compare fields
    EXPECT_EQ(ser_map->attribute(), expected_map.attribute());
    EXPECT_EQ(ser_map->function().instructions(), expected_map.function().instructions());

    EXPECT_EQ(registers->at(1), "x");
}

TEST_F(EndDeviceProtocolSerializationUtilTests, FilterOperation) {
    //build expression and filter operator
    auto left = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, "\x08"s));
    auto right = FieldAccessExpressionNode::create("var");
    auto lt = LessExpressionNode::create(left, right);
    auto filter = LogicalOperatorFactory::createFilterOperator(lt);

    auto registers = std::make_shared<std::vector<std::string>>();
    registers->push_back("var");

    auto ser_filter = EndDeviceProtocolSerializationUtil::serializeFilterOperator(filter, registers);

    //build expected protobuf
    auto expected_expr = intsToString(
        {ExpressionInstructions::CONST, DataTypes::INT8, 8, ExpressionInstructions::VAR, 0, ExpressionInstructions::LT});
    auto expected_filter = FilterOperation();
    expected_filter.mutable_predicate()->set_instructions(expected_expr);

    EXPECT_EQ(ser_filter->predicate().instructions(), expected_filter.predicate().instructions());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, QueryPlan) {

    auto expected = EndDeviceProtocol::Query();
    auto op = expected.mutable_operations()->Add();
    op->mutable_map()->set_attribute(0);
    op->mutable_map()->mutable_function()->set_instructions(intsToString({ExpressionInstructions::VAR, 0}));


    auto schema = Schema::create()->addField("x", BasicType::INT8);
    auto loraSourceType = LoRaWANProxySourceType::create();
    loraSourceType->setSensorFields(std::vector<std::string> {"x"});
    auto loraDesc = LoRaWANProxySourceDescriptor::create(schema,loraSourceType,"default_logical","default_physical");
    auto logicalSource = LogicalSource::create("default_logical", schema);

    auto query = TestQuery::from(loraDesc).map(Attribute("x") = Attribute("x")).sink(NullOutputSinkDescriptor::create());
    auto actual = EndDeviceProtocolSerializationUtil::serializeQueryPlanToEndDevice(query.getQueryPlan());

    EXPECT_EQ(actual->operations(0).map().attribute(), expected.operations(0).map().attribute());
    EXPECT_EQ(actual->operations(0).map().function().instructions(), expected.operations(0).map().function().instructions());

    EXPECT_EQ(query.getQueryPlan()->getOperatorByType<MapLogicalOperatorNode>().size(),0);
    EXPECT_EQ(query.getQueryPlan()->getSourceOperators().size(), 1);
    EXPECT_EQ(query.getQueryPlan()->getSinkOperators().size(), 1);
    EXPECT_TRUE(query.getQueryPlan()->getSourceOperators().at(0)->getSourceDescriptor()->equal(loraDesc));
    EXPECT_TRUE(query.getQueryPlan()->getSinkOperators().at(0)->getSinkDescriptor()->instanceOf<NullOutputSinkDescriptor>());
}

//std::string serializeNumeric(std::variant<int8_t,uint8_t,int16_t,uint16_t,int32_t,uint32_t,int64_t,uint64_t,float_t,double_t> in){
//    size_t size;
//    const char * source;
//    std::string res;
//    switch (in.index()) {
//        case 0: break;
//        case 1:{
//            size = sizeof(int8_t);
//            source = reinterpret_cast<const char*>(std::get<int8_t>(in));
//            break;
//        }
//        case 2:{
//            size = sizeof(uint8_t);
//            source = reinterpret_cast<const char*>(std::get<uint8_t>(in));
//            break;
//        }
//        case 3: {
//            size = sizeof(int16_t);
//            source = reinterpret_cast<const char*>(std::get<int16_t>(in));
//            break;
//        }
//        case 4: {
//            size = sizeof(uint16_t);
//            source = reinterpret_cast<const char*>(std::get<uint16_t>(in));
//            break;
//        }
//        case 5: {
//            size = sizeof(int32_t);
//            source = reinterpret_cast<const char*>(std::get<int32_t>(in));
//            break;
//        }
//        case 6: {
//            size = sizeof(uint32_t);
//            source = reinterpret_cast<const char*>(std::get<uint32_t>(in));
//            break;
//        }
//        case 7:{
//            size = sizeof(int64_t);
//            source = reinterpret_cast<const char*>(std::get<int64_t>(in));
//            break;
//        }
//        case 8:{
//            size = sizeof(uint64_t);
//            source = reinterpret_cast<const char*>(std::get<uint64_t>(in));
//            break;
//        }
//        case 9:{
//            size = sizeof(float_t);
//            source = reinterpret_cast<const char*>(std::get<float_t>(in));
//            break;
//        }
//        case 10: {
//            size = sizeof(int64_t);
//            source = reinterpret_cast<const char*>(std::get<int64_t>(in));
//            break;
//        }
//    }
//    res.assign(&source[0], &source[0]+size);
//
//    return res;
//
//}
}// namespace NES

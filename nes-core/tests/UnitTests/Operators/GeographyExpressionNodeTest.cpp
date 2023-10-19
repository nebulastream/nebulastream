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

#include <memory>

#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include  <Exceptions/TypeInferenceException.hpp>
#include <Operators/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/GeographyExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/GeographyFieldsAccessExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/STDWithinExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/STKnnExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/STWithinExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/CircleExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/PointExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/PolygonExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/RectangleExpressionNode.hpp>
#include <Optimizer/Phases/TypeInferencePhaseContext.hpp>
#include <Services/QueryParsingService.hpp>
#include <gtest/gtest.h>

namespace NES {

class GeographyExpressionNodeTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Compiler::JITCompiler> jitCompiler;
    std::shared_ptr<QueryParsingService> queryParsingService;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto cppCompiler = Compiler::CPPCompiler::create();
        jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    static void SetUpTestCase() { setupLogging(); }

  protected:
    static void setupLogging() {
        NES::Logger::setupLogging("GeographyExpressionNodeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup GeographyExpressionNodeTest test class.");
    }
};

// This tests the GeographyFieldsAccessExpressionNode
TEST_F(GeographyExpressionNodeTest, testGeographyFieldAccessExpressionNode) {
    auto typeInferencePhaseContext = Optimizer::TypeInferencePhaseContext(sourceCatalog, udfCatalog);
    auto schema = Schema::create()->addField("test$latitude", BasicType::FLOAT64)->addField("test$longitude", BasicType::FLOAT64);

    auto fieldLatitude = FieldAccessExpressionNode::create(DataTypeFactory::createDouble(), "latitude");
    auto fieldLongitude = FieldAccessExpressionNode::create(DataTypeFactory::createDouble(), "longitude");
    auto fieldLatitudeExpressionNode = fieldLatitude->as<FieldAccessExpressionNode>();
    auto fieldLongitudeExpressionNode = fieldLongitude->as<FieldAccessExpressionNode>();

    // create a geography field access expression node
    auto geographyExpressionNode =
        GeographyFieldsAccessExpressionNode::create(fieldLatitudeExpressionNode, fieldLongitudeExpressionNode);
    auto geographyFieldAccessExpressionNode = geographyExpressionNode->as<GeographyFieldsAccessExpressionNode>();

    // test accessor methods
    EXPECT_TRUE(geographyFieldAccessExpressionNode->getStamp()->isFloat());
    EXPECT_TRUE(geographyFieldAccessExpressionNode->getLatitude()->instanceOf<FieldAccessExpressionNode>());
    EXPECT_TRUE(geographyFieldAccessExpressionNode->getLongitude()->instanceOf<FieldAccessExpressionNode>());
    EXPECT_EQ(geographyFieldAccessExpressionNode->toString(),
              "Geography(FieldAccessNode(latitude[Float(64 bits)]), FieldAccessNode(longitude[Float(64 bits)]))");

    // create a copy of the geography field access expression node
    auto copyGeographyExpressionNode = geographyFieldAccessExpressionNode->copy();
    auto copyGeographyFieldAccessExpressionNode = copyGeographyExpressionNode->as<GeographyFieldsAccessExpressionNode>();

    // test accessor methods on the copy of the geography field access expression node
    EXPECT_TRUE(copyGeographyFieldAccessExpressionNode->getStamp()->isFloat());
    EXPECT_TRUE(copyGeographyFieldAccessExpressionNode->getLatitude()->instanceOf<FieldAccessExpressionNode>());
    EXPECT_TRUE(copyGeographyFieldAccessExpressionNode->getLongitude()->instanceOf<FieldAccessExpressionNode>());
    EXPECT_EQ(copyGeographyFieldAccessExpressionNode->toString(),
              "Geography(FieldAccessNode(latitude[Float(64 bits)]), FieldAccessNode(longitude[Float(64 bits)]))");

    // check for the equality of the original and the copy
    EXPECT_TRUE(geographyFieldAccessExpressionNode->equal(copyGeographyFieldAccessExpressionNode));

    // create a geography field access expression node from the attributes in the schema
    auto attributeLatitude = Attribute("latitude").getExpressionNode();
    auto attributeLongitude = Attribute("longitude").getExpressionNode();
    auto fieldAccessExpressionNode1 = attributeLatitude->as<FieldAccessExpressionNode>();
    auto fieldAccessExpressionNode2 = attributeLatitude->as<FieldAccessExpressionNode>();

    auto geographyExpressionNode1 =
        GeographyFieldsAccessExpressionNode::create(fieldAccessExpressionNode1, fieldAccessExpressionNode2);
    auto geographyFieldAccessExpressionNode1 = geographyExpressionNode1->as<GeographyFieldsAccessExpressionNode>();
    geographyFieldAccessExpressionNode1->inferStamp(typeInferencePhaseContext, schema);
    auto copyGeographyExpressionNode1 = geographyFieldAccessExpressionNode1->copy();
    auto copyGeographyFieldAccessExpressionNode1 = copyGeographyExpressionNode1->as<GeographyFieldsAccessExpressionNode>();
    copyGeographyFieldAccessExpressionNode1->inferStamp(typeInferencePhaseContext, schema);

    // test accessor methods on both the original and the copy
    EXPECT_TRUE(geographyFieldAccessExpressionNode1->getStamp()->isFloat());
    EXPECT_TRUE(geographyFieldAccessExpressionNode1->getLatitude()->instanceOf<FieldAccessExpressionNode>());
    EXPECT_TRUE(geographyFieldAccessExpressionNode1->getLongitude()->instanceOf<FieldAccessExpressionNode>());
    EXPECT_EQ(geographyFieldAccessExpressionNode1->toString(),
              "Geography(FieldAccessNode(test$latitude[Float(64 bits)]), FieldAccessNode(test$latitude[Float(64 bits)]))");

    EXPECT_TRUE(copyGeographyFieldAccessExpressionNode1->getStamp()->isFloat());
    EXPECT_TRUE(copyGeographyFieldAccessExpressionNode1->getLatitude()->instanceOf<FieldAccessExpressionNode>());
    EXPECT_TRUE(copyGeographyFieldAccessExpressionNode1->getLongitude()->instanceOf<FieldAccessExpressionNode>());
    EXPECT_EQ(copyGeographyFieldAccessExpressionNode1->toString(),
              "Geography(FieldAccessNode(test$latitude[Float(64 bits)]), FieldAccessNode(test$latitude[Float(64 bits)]))");

    // check for equality
    EXPECT_TRUE(geographyFieldAccessExpressionNode1->equal(copyGeographyFieldAccessExpressionNode1));

    // create an empty geography field access expression node
    auto dataTypePtr = DataTypeFactory::createDouble();
    auto emptyGeographyExpression = std::make_shared<GeographyFieldsAccessExpressionNode>(dataTypePtr);

    // accessing children via various accessors should throw exception
    EXPECT_THROW(emptyGeographyExpression->getLatitude(), InvalidArgumentException);
    EXPECT_THROW(emptyGeographyExpression->getLongitude(), InvalidArgumentException);
    EXPECT_THROW(emptyGeographyExpression->inferStamp(typeInferencePhaseContext, schema), InvalidArgumentException);
    EXPECT_THROW(const auto str = emptyGeographyExpression->toString(), InvalidArgumentException);

    // set children of the geography field access expression node
    auto fieldLatitudeExpressionNodeCopy = fieldLatitude->as<FieldAccessExpressionNode>();
    auto fieldLongitudeExpressionNodeCopy = fieldLongitude->as<FieldAccessExpressionNode>();
    emptyGeographyExpression->setChildren(fieldLatitudeExpressionNodeCopy, fieldLongitudeExpressionNodeCopy);

    // children set expect no throw for accessor methods
    EXPECT_NO_THROW(emptyGeographyExpression->getLatitude());
    EXPECT_NO_THROW(emptyGeographyExpression->getLongitude());
    EXPECT_NO_THROW(emptyGeographyExpression->inferStamp(typeInferencePhaseContext, schema));
    EXPECT_NO_THROW(const auto str = emptyGeographyExpression->toString());
}

// This tests the ST_DWithin expression node
TEST_F(GeographyExpressionNodeTest, testSTDWithinExpressionNode) {
    auto typeInferencePhaseContext = Optimizer::TypeInferencePhaseContext(sourceCatalog, udfCatalog);
    auto schema = Schema::create()->addField("test$latitude", BasicType::FLOAT64)->addField("test$longitude", BasicType::FLOAT64);

    auto fieldLatitude = FieldAccessExpressionNode::create(DataTypeFactory::createDouble(), "latitude");
    auto fieldLongitude = FieldAccessExpressionNode::create(DataTypeFactory::createDouble(), "longitude");
    auto fieldLatitudeExpressionNode = fieldLatitude->as<FieldAccessExpressionNode>();
    auto fieldLongitudeExpressionNode = fieldLongitude->as<FieldAccessExpressionNode>();

    // create a geography field access expression node
    auto geographyExpressionNode =
        GeographyFieldsAccessExpressionNode::create(fieldLatitudeExpressionNode, fieldLongitudeExpressionNode);
    auto geographyFieldAccessExpressionNode = geographyExpressionNode->as<GeographyFieldsAccessExpressionNode>();

    // create shape expression
    auto circle = CIRCLE(52.5153, 13.3267, 100.0);
    auto differentCircle = CIRCLE(52.5154, 13.3267, 100.0);
    auto point = POINT(52.5153, 13.3267);
    auto polygon = POLYGON({52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274});
    auto rectangle = RECTANGLE(52.5138, 13.3265, 52.5157, 13.3284);

    // create a st_dwithin expression node
    auto expressionNode = STDWithinExpressionNode::create(geographyFieldAccessExpressionNode, circle);
    auto stDWithinExpressionNode = expressionNode->as<STDWithinExpressionNode>();

    // test accessor methods
    EXPECT_FALSE(stDWithinExpressionNode->getStamp()->isFloat());
    EXPECT_TRUE(stDWithinExpressionNode->getCircle()->instanceOf<CircleExpressionNode>());
    EXPECT_TRUE(stDWithinExpressionNode->getPoint()->instanceOf<GeographyFieldsAccessExpressionNode>());
    EXPECT_EQ(stDWithinExpressionNode->toString(),
              "ST_DWITHIN(Geography(FieldAccessNode(latitude[Float(64 bits)]), FieldAccessNode(longitude[Float(64 bits)])), "
              "CIRCLE(lat: "
              "52.5153, lon: 13.3267, radius: 100))");

    // create a copy of the ST_DWithin Expression Node
    auto copyExpressionNode = stDWithinExpressionNode->copy();

    // test for equality
    EXPECT_TRUE(stDWithinExpressionNode->equal(copyExpressionNode));

    // create a not equal st_dwithin expression node
    auto notEqualExpressionNode = STDWithinExpressionNode::create(geographyFieldAccessExpressionNode, differentCircle);
    auto notEqualSTDWithinExpressionNode = notEqualExpressionNode->as<STDWithinExpressionNode>();

    // test for non-equality
    EXPECT_FALSE(stDWithinExpressionNode->equal(notEqualExpressionNode));

    // create and empty std_dwithin expression node
    auto emptyExpressionNode = std::make_shared<STDWithinExpressionNode>();
    auto emptySTDWithinExpressionNode = emptyExpressionNode->as<STDWithinExpressionNode>();

    // accessor methods should throw exceptions
    EXPECT_THROW(const auto str = emptySTDWithinExpressionNode->toString(), InvalidArgumentException);
    EXPECT_THROW(emptySTDWithinExpressionNode->getCircle(), InvalidArgumentException);
    EXPECT_THROW(emptySTDWithinExpressionNode->getPoint(), InvalidArgumentException);
    EXPECT_THROW(const auto isEqual = emptySTDWithinExpressionNode->equal(notEqualExpressionNode), InvalidArgumentException);

    // set children of empty expression
    emptySTDWithinExpressionNode->setChildren(geographyFieldAccessExpressionNode, circle);

    EXPECT_NO_THROW(const auto str = emptySTDWithinExpressionNode->toString());
    EXPECT_NO_THROW(emptySTDWithinExpressionNode->getCircle());
    EXPECT_NO_THROW(emptySTDWithinExpressionNode->getPoint());
    EXPECT_NO_THROW(const auto isEqual = emptySTDWithinExpressionNode->equal(notEqualExpressionNode));

    // creating st_dwithin expression nodes with wrong shape expressions should throw exceptions
    EXPECT_THROW(STDWithinExpressionNode::create(geographyFieldAccessExpressionNode, point);, InvalidArgumentException);
    EXPECT_THROW(STDWithinExpressionNode::create(geographyFieldAccessExpressionNode, polygon);, InvalidArgumentException);
    EXPECT_THROW(STDWithinExpressionNode::create(geographyFieldAccessExpressionNode, rectangle);, InvalidArgumentException);

    // create a geography field access expression node from the attributes in the schema
    auto attributeLatitude = Attribute("latitude").getExpressionNode();
    auto attributeLongitude = Attribute("longitude").getExpressionNode();
    auto fieldAccessExpressionNode1 = attributeLatitude->as<FieldAccessExpressionNode>();
    auto fieldAccessExpressionNode2 = attributeLatitude->as<FieldAccessExpressionNode>();

    auto geographyExpressionNode1 =
        GeographyFieldsAccessExpressionNode::create(fieldAccessExpressionNode1, fieldAccessExpressionNode2);
    auto geographyFieldAccessExpressionNode1 = geographyExpressionNode1->as<GeographyFieldsAccessExpressionNode>();
    geographyFieldAccessExpressionNode1->inferStamp(typeInferencePhaseContext, schema);

    // create a STDWithinExpressionNode from the lat/lng fields from the schema
    auto expressionNode1 = STDWithinExpressionNode::create(geographyFieldAccessExpressionNode1, circle);
    auto stDWithinExpressionNode1 = expressionNode1->as<STDWithinExpressionNode>();
    stDWithinExpressionNode1->inferStamp(typeInferencePhaseContext, schema);
    auto copyExpressionNode1 = stDWithinExpressionNode1->copy();
    auto differentExpressionNode1 = STDWithinExpressionNode::create(geographyFieldAccessExpressionNode1, differentCircle);

    // check for equalities and non-equalities
    EXPECT_TRUE(stDWithinExpressionNode1->equal(copyExpressionNode1));
    EXPECT_FALSE(stDWithinExpressionNode1->equal(differentExpressionNode1));
}

// This tests the STKnnExpression Node
TEST_F(GeographyExpressionNodeTest, testSTKnnExpressionNode) {
    auto typeInferencePhaseContext = Optimizer::TypeInferencePhaseContext(sourceCatalog, udfCatalog);
    auto schema = Schema::create()->addField("test$latitude", BasicType::FLOAT64)->addField("test$longitude", BasicType::FLOAT64);

    auto fieldLatitude = FieldAccessExpressionNode::create(DataTypeFactory::createDouble(), "latitude");
    auto fieldLongitude = FieldAccessExpressionNode::create(DataTypeFactory::createDouble(), "longitude");
    auto fieldLatitudeExpressionNode = fieldLatitude->as<FieldAccessExpressionNode>();
    auto fieldLongitudeExpressionNode = fieldLongitude->as<FieldAccessExpressionNode>();

    // create a geography field access expression node
    auto geographyExpressionNode =
        GeographyFieldsAccessExpressionNode::create(fieldLatitudeExpressionNode, fieldLongitudeExpressionNode);
    auto geographyFieldAccessExpressionNode = geographyExpressionNode->as<GeographyFieldsAccessExpressionNode>();

    // create constant
    auto constant = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "10"));
    auto k = constant->as<ConstantValueExpressionNode>();

    // create shape expression
    auto circle = CIRCLE(52.5153, 13.3267, 100.0);
    auto point = POINT(52.5153, 13.3267);
    auto polygon = POLYGON({52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274});
    auto rectangle = RECTANGLE(52.5138, 13.3265, 52.5157, 13.3284);
    auto differentPoint = POINT(52.5154, 13.3267);
    auto differentConstant = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT32, "48"));
    auto differentK = constant->as<ConstantValueExpressionNode>();

    // create a STKnnExpression
    auto expressionNode = STKnnExpressionNode::create(geographyFieldAccessExpressionNode, point, k);
    auto sTKnnExpressionNode = expressionNode->as<STKnnExpressionNode>();

    // test accessor methods
    EXPECT_FALSE(sTKnnExpressionNode->getStamp()->isFloat());
    EXPECT_TRUE(sTKnnExpressionNode->getQueryPoint()->instanceOf<PointExpressionNode>());
    EXPECT_TRUE(sTKnnExpressionNode->getPoint()->instanceOf<GeographyFieldsAccessExpressionNode>());
    EXPECT_TRUE(sTKnnExpressionNode->getK()->instanceOf<ConstantValueExpressionNode>());
    EXPECT_EQ(sTKnnExpressionNode->toString(),
              "ST_KNN(Geography(FieldAccessNode(latitude[Float(64 bits)]), FieldAccessNode(longitude[Float(64 bits)])), "
              "POINT(lat: 52.5153, "
              "lon: 13.3267), ConstantValue(BasicValue(10)))");

    // create a copy of the STKnnExpression Expression Node
    auto copyExpressionNode = sTKnnExpressionNode->copy();

    // test for equality
    EXPECT_TRUE(sTKnnExpressionNode->equal(copyExpressionNode));

    // create not equal STKnnExpression node
    auto notEqualExpressionNode1 = STKnnExpressionNode::create(geographyFieldAccessExpressionNode, differentPoint, k);
    auto notEqualSTKnnExpressionNode1 = notEqualExpressionNode1->as<STKnnExpressionNode>();

    // test for non-equality
    EXPECT_FALSE(sTKnnExpressionNode->equal(notEqualExpressionNode1));

    // create and empty STKnnExpression node
    auto emptyExpressionNode = std::make_shared<STKnnExpressionNode>();
    auto emptySTKnnExpressionNode = emptyExpressionNode->as<STKnnExpressionNode>();

    // accessor methods should throw exceptions
    EXPECT_THROW(const auto str = emptySTKnnExpressionNode->toString(), InvalidArgumentException);
    EXPECT_THROW(emptySTKnnExpressionNode->getQueryPoint(), InvalidArgumentException);
    EXPECT_THROW(emptySTKnnExpressionNode->getPoint(), InvalidArgumentException);
    EXPECT_THROW(emptySTKnnExpressionNode->getK(), InvalidArgumentException);
    EXPECT_THROW(const auto isEqual = emptySTKnnExpressionNode->equal(notEqualExpressionNode1), InvalidArgumentException);

    // set children of empty STKnnExpression node
    emptySTKnnExpressionNode->setChildren(geographyFieldAccessExpressionNode, point, k);

    EXPECT_NO_THROW(const auto str = emptySTKnnExpressionNode->toString());
    EXPECT_NO_THROW(emptySTKnnExpressionNode->getQueryPoint());
    EXPECT_NO_THROW(emptySTKnnExpressionNode->getPoint());
    EXPECT_NO_THROW(emptySTKnnExpressionNode->getK());
    EXPECT_NO_THROW(const auto isEqual = emptySTKnnExpressionNode->equal(notEqualExpressionNode1));

    // creating STKnnExpression expression nodes with wrong shape expressions should throw exceptions
    EXPECT_THROW(STKnnExpressionNode::create(geographyFieldAccessExpressionNode, circle, k);, InvalidArgumentException);
    EXPECT_THROW(STKnnExpressionNode::create(geographyFieldAccessExpressionNode, polygon, k);, InvalidArgumentException);
    EXPECT_THROW(STKnnExpressionNode::create(geographyFieldAccessExpressionNode, rectangle, k);, InvalidArgumentException);

    // create a geography field access expression node from the attributes in the schema
    auto attributeLatitude = Attribute("latitude").getExpressionNode();
    auto attributeLongitude = Attribute("longitude").getExpressionNode();
    auto fieldAccessExpressionNode1 = attributeLatitude->as<FieldAccessExpressionNode>();
    auto fieldAccessExpressionNode2 = attributeLatitude->as<FieldAccessExpressionNode>();

    auto geographyExpressionNode1 =
        GeographyFieldsAccessExpressionNode::create(fieldAccessExpressionNode1, fieldAccessExpressionNode2);
    auto geographyFieldAccessExpressionNode1 = geographyExpressionNode1->as<GeographyFieldsAccessExpressionNode>();
    geographyFieldAccessExpressionNode1->inferStamp(typeInferencePhaseContext, schema);

    // create a STKnnExpressionNode from the lat/lng fields from the schema
    auto expressionNode1 = STKnnExpressionNode::create(geographyFieldAccessExpressionNode1, point, k);
    auto STKnnExpressionNode1 = expressionNode1->as<STKnnExpressionNode>();
    STKnnExpressionNode1->inferStamp(typeInferencePhaseContext, schema);
    auto copyExpressionNode1 = STKnnExpressionNode1->copy();
    auto differentExpressionNode1 = STKnnExpressionNode::create(geographyFieldAccessExpressionNode1, differentPoint, k);

    // check for equalities and non-equalities
    EXPECT_TRUE(STKnnExpressionNode1->equal(copyExpressionNode1));
    EXPECT_FALSE(STKnnExpressionNode1->equal(differentExpressionNode1));
}

// This tests the STWithinExpressionNode
TEST_F(GeographyExpressionNodeTest, testSTWithinExpressionNode) {
    auto typeInferencePhaseContext = Optimizer::TypeInferencePhaseContext(sourceCatalog, udfCatalog);
    auto schema = Schema::create()->addField("test$latitude", BasicType::FLOAT64)->addField("test$longitude", BasicType::FLOAT64);

    auto fieldLatitude = FieldAccessExpressionNode::create(DataTypeFactory::createDouble(), "latitude");
    auto fieldLongitude = FieldAccessExpressionNode::create(DataTypeFactory::createDouble(), "longitude");
    auto fieldLatitudeExpressionNode = fieldLatitude->as<FieldAccessExpressionNode>();
    auto fieldLongitudeExpressionNode = fieldLongitude->as<FieldAccessExpressionNode>();

    // create a geography field access expression node
    auto geographyExpressionNode =
        GeographyFieldsAccessExpressionNode::create(fieldLatitudeExpressionNode, fieldLongitudeExpressionNode);
    auto geographyFieldAccessExpressionNode = geographyExpressionNode->as<GeographyFieldsAccessExpressionNode>();

    // create constant
    auto constant = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "10"));
    auto k = constant->as<ConstantValueExpressionNode>();

    // create valid shape expressions for STWithinExpressionNode
    auto polygon = POLYGON({52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274});
    auto rectangle = RECTANGLE(52.5138, 13.3265, 52.5157, 13.3284);

    // create invalid shape expressions for STWithinExpressionNode
    auto circle = CIRCLE(52.5153, 13.3267, 100.0);
    auto point = POINT(52.5153, 13.3267);

    // auto shapes different to valid shape expressions
    auto differentPolygon = POLYGON({52.5158, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274});
    auto differentRectangle = RECTANGLE(52.5139, 13.3265, 52.5157, 13.3284);

    // create valid STWithinExpressionNode nodes
    auto rectangleExpressionNode = STWithinExpressionNode::create(geographyFieldAccessExpressionNode, rectangle);
    auto rectangleSTWithinExpressionNode = rectangleExpressionNode->as<STWithinExpressionNode>();
    auto polygonExpressionNode = STWithinExpressionNode::create(geographyFieldAccessExpressionNode, polygon);
    auto polygonSTWithinExpressionNode = polygonExpressionNode->as<STWithinExpressionNode>();

    // test accessor methods
    EXPECT_FALSE(rectangleSTWithinExpressionNode->getStamp()->isFloat());
    EXPECT_TRUE(rectangleSTWithinExpressionNode->getPoint()->instanceOf<GeographyFieldsAccessExpressionNode>());
    EXPECT_TRUE(rectangleSTWithinExpressionNode->getShape()->instanceOf<RectangleExpressionNode>());
    EXPECT_EQ(rectangleSTWithinExpressionNode->toString(),
              "ST_WITHIN(Geography(FieldAccessNode(latitude[Float(64 bits)]), FieldAccessNode(longitude[Float(64 bits)])), "
              "RECTANGLE(lat_low: "
              "52.5138, lon_low: 13.3265, lat_high: 52.5157, lon_high: 13.3284))");

    EXPECT_FALSE(polygonSTWithinExpressionNode->getStamp()->isFloat());
    EXPECT_TRUE(polygonSTWithinExpressionNode->getPoint()->instanceOf<GeographyFieldsAccessExpressionNode>());
    EXPECT_TRUE(polygonSTWithinExpressionNode->getShape()->instanceOf<PolygonExpressionNode>());
    EXPECT_EQ(polygonSTWithinExpressionNode->toString(),
              "ST_WITHIN(Geography(FieldAccessNode(latitude[Float(64 bits)]), FieldAccessNode(longitude[Float(64 bits)])), "
              "POLYGON(52.5155, "
              "13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274))");

    // create a copy of the STWithinExpressionNode Expression Node
    auto copyExpressionNode1 = rectangleSTWithinExpressionNode->copy();
    auto copyExpressionNode2 = polygonSTWithinExpressionNode->copy();

    // test for equality
    EXPECT_TRUE(rectangleSTWithinExpressionNode->equal(copyExpressionNode1));
    EXPECT_TRUE(polygonSTWithinExpressionNode->equal(copyExpressionNode2));

    // create not equal STWithinExpressionNode node
    auto notEqualExpressionNode1 = STWithinExpressionNode::create(geographyFieldAccessExpressionNode, differentRectangle);
    auto notEqualExpressionNode2 = STWithinExpressionNode::create(geographyFieldAccessExpressionNode, differentPolygon);

    // test for non-equality
    EXPECT_FALSE(rectangleSTWithinExpressionNode->equal(notEqualExpressionNode1));
    EXPECT_FALSE(rectangleSTWithinExpressionNode->equal(notEqualExpressionNode2));
    EXPECT_FALSE(polygonSTWithinExpressionNode->equal(notEqualExpressionNode1));
    EXPECT_FALSE(polygonSTWithinExpressionNode->equal(notEqualExpressionNode2));

    // create and empty STWithinExpressionNode node
    auto emptyExpressionNode = std::make_shared<STWithinExpressionNode>();
    auto emptySTWithinExpressionNode = emptyExpressionNode->as<STWithinExpressionNode>();

    // accessor methods should throw exceptions
    EXPECT_THROW(const auto str = emptySTWithinExpressionNode->toString(), InvalidArgumentException);
    EXPECT_THROW(emptySTWithinExpressionNode->getPoint(), InvalidArgumentException);
    EXPECT_THROW(emptySTWithinExpressionNode->getShape(), InvalidArgumentException);
    EXPECT_THROW(const auto isEqual = emptySTWithinExpressionNode->equal(notEqualExpressionNode1), InvalidArgumentException);

    // set children of empty STKnnExpression node
    emptySTWithinExpressionNode->setChildren(geographyFieldAccessExpressionNode, polygon);

    EXPECT_NO_THROW(const auto str = emptySTWithinExpressionNode->toString());
    EXPECT_NO_THROW(emptySTWithinExpressionNode->getPoint());
    EXPECT_NO_THROW(emptySTWithinExpressionNode->getShape());
    EXPECT_NO_THROW(const auto isEqual = emptySTWithinExpressionNode->equal(notEqualExpressionNode1));

    // creating STKnnExpression expression nodes with wrong shape expressions should throw exceptions
    EXPECT_THROW(STWithinExpressionNode::create(geographyFieldAccessExpressionNode, circle);, InvalidArgumentException);
    EXPECT_THROW(STWithinExpressionNode::create(geographyFieldAccessExpressionNode, point);, InvalidArgumentException);

    // create a geography field access expression node from the attributes in the schema
    auto attributeLatitude = Attribute("latitude").getExpressionNode();
    auto attributeLongitude = Attribute("longitude").getExpressionNode();
    auto fieldAccessExpressionNode1 = attributeLatitude->as<FieldAccessExpressionNode>();
    auto fieldAccessExpressionNode2 = attributeLatitude->as<FieldAccessExpressionNode>();

    auto geographyExpressionNode1 =
        GeographyFieldsAccessExpressionNode::create(fieldAccessExpressionNode1, fieldAccessExpressionNode2);
    auto geographyFieldAccessExpressionNode1 = geographyExpressionNode1->as<GeographyFieldsAccessExpressionNode>();
    geographyFieldAccessExpressionNode1->inferStamp(typeInferencePhaseContext, schema);

    // create a STWithinExpressionNode from the lat/lng fields from the schema
    auto expressionNode3 = STWithinExpressionNode::create(geographyFieldAccessExpressionNode1, polygon);
    auto stWithinExpressionNode3 = expressionNode3->as<STWithinExpressionNode>();
    stWithinExpressionNode3->inferStamp(typeInferencePhaseContext, schema);
    auto copyExpressionNode3 = stWithinExpressionNode3->copy();
    auto differentExpressionNode1 = STWithinExpressionNode::create(geographyFieldAccessExpressionNode1, differentPolygon);

    // check for equalities and non-equalities
    EXPECT_TRUE(stWithinExpressionNode3->equal(copyExpressionNode3));
    EXPECT_FALSE(stWithinExpressionNode3->equal(differentExpressionNode1));
}

}// namespace NES

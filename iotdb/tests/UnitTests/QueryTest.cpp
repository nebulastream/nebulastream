#include "gtest/gtest.h"

#include <cassert>
#include <iostream>
#include <Util/Logger.hpp>
#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>

#include <API/Query.hpp>
#include <API/Types/DataTypes.hpp>
#include <Nodes/Expressions/FieldReadExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/LessThenExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
namespace NES {

class SelectionDataGenFunctor {
  public:
    SelectionDataGenFunctor() {
    }

    struct __attribute__((packed)) InputTuple {
        uint32_t id;
        uint32_t value;
    };

    TupleBufferPtr operator()() {
        // 10 tuples of size one
        TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
        uint64_t tupleCnt = buf->getNumberOfTuples();

        assert(buf->getBuffer() != NULL);

        InputTuple* tuples = (InputTuple*) buf->getBuffer();
        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].id = i;
            tuples[i].value = i*2;
        }
        buf->setTupleSizeInBytes(sizeof(InputTuple));
        buf->setNumberOfTuples(tupleCnt);
        return buf;
    }
};

class QueryTest : public testing::Test {
  public:

    static void SetUpTestCase() {
        NES::setupLogging("QueryTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down QueryTest test class." << std::endl;
    }

    void TearDown() {
    }
};

TEST_F(QueryTest, testQueryFilter) {

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField(
        "value", BasicType::UINT64);

    Stream def = Stream("default_logical", schema);

    auto fieldRead = FieldReadExpressionNode::create(createDataType(INT64), "field_1");
    auto constant = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT64, "10"));
    auto filterPredicate = LessThenExpressionNode::create(fieldRead, constant);
    Query& query = Query::from(def).filter(filterPredicate).print(std::cout);

    const std::vector<SourceLogicalOperatorNodePtr>& sourceOperators = query.getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1);

    const std::vector<SinkLogicalOperatorNodePtr>& sinkOperators = query.getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);
}

}  // namespace NES


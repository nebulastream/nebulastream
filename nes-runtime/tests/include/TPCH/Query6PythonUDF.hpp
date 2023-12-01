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
#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY6_PYTHON_UDF_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY6_PYTHON_UDF_HPP_

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessEqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Relational/PythonUDF/MapPythonUDF.hpp>
#include <Execution/Operators/Relational/PythonUDF/FlatMapPythonUDF.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <TPCH/PipelinePlan.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Timer.hpp>

namespace NES::Runtime::Execution {
using namespace Expressions;
using namespace Operators;
class TPCH_Query6_Python {
  public:
    /**
     * Initializes the map handler for the given pipeline.
     * @param className python class name of the udf
     * @param methodName method name of the udf
     * @param inputProxyName input proxy class name
     * @param outputProxyName output proxy class name
     * @param schema schema of the input and output tuples
     * @param testDataPath path to the test data containing the udf jar
     * @return operator handler
     */
    auto static initMapHandler(std::string function,
                        std::string functionName,
                        std::map<std::string, std::string> modulesToImport,
                        std::string pythonCompiler,
                        SchemaPtr inputSchema,
                        SchemaPtr outputSchema) {
        return std::make_shared<Operators::PythonUDFOperatorHandler>(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema);
    }

    static PipelinePlan getPipelinePlan(std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                                        Runtime::BufferManagerPtr bm, std::string pythonCompiler) {
        const SchemaPtr inputSchema = Schema::create()
                                          ->addField("l_shipdate", BasicType::INT32)
                                          ->addField("l_discount", BasicType::FLOAT32)
                                          ->addField("l_quantity", BasicType::INT32)
                                          ->addField("l_extendedprice", BasicType::FLOAT32);
        const SchemaPtr outputSchema = Schema::create()
                                           ->addField("revenue_before_aggregation", BasicType::FLOAT32);
        // cannot do aggregations with a Map UDF bc it has nothing to aggregate with

        auto function = "def tpch_query_6(l_shipdate, l_discount, l_quantity, l_extendedprice):\n"
                        "  if (l_shipdate >= 19940101) and (l_shipdate < 19950101) and (l_discount >= 0.05) and (l_discount <= 0.07) and (l_quantity < 24):\n"
                        "    return l_extendedprice * l_discount\n"
                        "  else:\n"
                        "    return 0";
        auto functionName = "tpch_query_6";
        std::map<std::string, std::string> modulesToImport;
        auto pythonUdfOperatorHandler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema);

        PipelinePlan plan;
        auto& lineitems = tables[TPCHTable::LineItem];

        auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineitems->getLayout()));
        std::vector<std::string> projections = {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"};
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr), projections);

        auto udfOperator = std::make_shared<Operators::MapPythonUDF>(1, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(udfOperator);

        // sum(l_extendedprice * l_discount)
        auto l_revenue_before_aggregation = std::make_shared<Expressions::ReadFieldExpression>("revenue_before_aggregation");
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());
        std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
            std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, l_revenue_before_aggregation, "revenue")};
        auto aggregation = std::make_shared<Operators::BatchAggregation>(0 /*handler index*/, aggregationFunctions);
        udfOperator->setChild(aggregation);

        // create aggregation pipeline
        auto aggregationPipeline = std::make_shared<PhysicalOperatorPipeline>();
        aggregationPipeline->setRootOperator(scanOperator);
        std::vector<OperatorHandlerPtr> aggregationHandler = {std::make_shared<Operators::BatchAggregationHandler>(), pythonUdfOperatorHandler};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        auto pipelineContext = MockedPipelineExecutionContext(aggregationHandler);
        plan.appendPipeline(aggregationPipeline, pipeline1Context);

        auto aggScan = std::make_shared<BatchAggregationScan>(0 /*handler index*/, aggregationFunctions);
        // emit operator
        auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        resultSchema->addField("revenue", BasicType::FLOAT32);
        auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultLayout);
        auto emit = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        aggScan->setChild(emit);

        // create aggregation pipeline
        auto emitPipeline = std::make_shared<PhysicalOperatorPipeline>();
        emitPipeline->setRootOperator(aggScan);
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        plan.appendPipeline(emitPipeline, pipeline2Context);
        return plan;
    }
};

class TPCH_Query6_Python_Pandas {
  public:
    /**
     * Initializes the map handler for the given pipeline.
     * @param className python class name of the udf
     * @param methodName method name of the udf
     * @param inputProxyName input proxy class name
     * @param outputProxyName output proxy class name
     * @param schema schema of the input and output tuples
     * @param testDataPath path to the test data containing the udf jar
     * @return operator handler
     */
    auto static initMapHandler(std::string function,
                               std::string functionName,
                               std::map<std::string, std::string> modulesToImport,
                               std::string pythonCompiler,
                               SchemaPtr inputSchema,
                               SchemaPtr outputSchema) {
        return std::make_shared<Operators::PythonUDFOperatorHandler>(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema);
    }

    static PipelinePlan getPipelinePlan(std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                                        Runtime::BufferManagerPtr bm, std::string pythonCompiler) {
        const SchemaPtr inputSchema = Schema::create()
                                          ->addField("l_shipdate", BasicType::INT32)
                                          ->addField("l_discount", BasicType::FLOAT32)
                                          ->addField("l_quantity", BasicType::INT32)
                                          ->addField("l_extendedprice", BasicType::FLOAT32);
        const SchemaPtr outputSchema = Schema::create()
                                           ->addField("revenue_before_aggregation", BasicType::FLOAT32);
        // cannot do aggregations with a Map UDF bc it has nothing to aggregate with

        auto function = "def tpch_query_6_pandas(l_extendedprice, l_shipdate, l_discount, l_quantity):\n"
                        "   df = pd.DataFrame([[l_extendedprice, l_shipdate, l_discount, l_quantity]], columns=[\"l_extendedprice\", \"l_shipdate\", \"l_discount\", \"l_quantity\"])\n"
                        "   res = df[(df['l_shipdate'] >= 19940101) &\n"
                        "           (df['l_shipdate'] < 19950101) &\n"
                        "           (0.05 <= df['l_discount']) & (df['l_discount'] <= 0.07) &\n"
                        "           (df['l_quantity'] < 24)]\n"
                        "   if res.empty:\n"
                        "    return 0.0\n"
                        "   else:\n"
                        "    return res[\"l_extendedprice\"].item() * res[\"l_discount\"].item()";
        auto functionName = "tpch_query_6_pandas";
        std::map<std::string, std::string> modulesToImport;
        modulesToImport.insert({"pandas", "pd"});
        auto pythonUdfOperatorHandler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema);

        PipelinePlan plan;
        auto& lineitems = tables[TPCHTable::LineItem];

        auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineitems->getLayout()));
        std::vector<std::string> projections = {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"};
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr), projections);

        auto udfOperator = std::make_shared<Operators::MapPythonUDF>(1, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(udfOperator);

        // sum(l_extendedprice * l_discount)
        auto l_revenue_before_aggregation = std::make_shared<Expressions::ReadFieldExpression>("revenue_before_aggregation");
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());
        std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
            std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, l_revenue_before_aggregation, "revenue")};
        auto aggregation = std::make_shared<Operators::BatchAggregation>(0 /*handler index*/, aggregationFunctions);
        udfOperator->setChild(aggregation);

        // create aggregation pipeline
        auto aggregationPipeline = std::make_shared<PhysicalOperatorPipeline>();
        aggregationPipeline->setRootOperator(scanOperator);
        std::vector<OperatorHandlerPtr> aggregationHandler = {std::make_shared<Operators::BatchAggregationHandler>(), pythonUdfOperatorHandler};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        auto pipelineContext = MockedPipelineExecutionContext(aggregationHandler);
        plan.appendPipeline(aggregationPipeline, pipeline1Context);

        auto aggScan = std::make_shared<BatchAggregationScan>(0 /*handler index*/, aggregationFunctions);
        // emit operator
        auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        resultSchema->addField("revenue", BasicType::FLOAT32);
        auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultLayout);
        auto emit = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        aggScan->setChild(emit);

        // create aggregation pipeline
        auto emitPipeline = std::make_shared<PhysicalOperatorPipeline>();
        emitPipeline->setRootOperator(aggScan);
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        plan.appendPipeline(emitPipeline, pipeline2Context);
        return plan;
    }
};

class TPCH_Query6_Python_Multiple_UDFs {
  public:
    /**
     * Initializes the map handler for the given pipeline.
     * @param className python class name of the udf
     * @param methodName method name of the udf
     * @param inputProxyName input proxy class name
     * @param outputProxyName output proxy class name
     * @param schema schema of the input and output tuples
     * @param testDataPath path to the test data containing the udf jar
     * @return operator handler
     */
    auto static initMapHandler(std::string function,
                               std::string functionName,
                               std::map<std::string, std::string> modulesToImport,
                               std::string pythonCompiler,
                               SchemaPtr inputSchema,
                               SchemaPtr outputSchema) {
        return std::make_shared<Operators::PythonUDFOperatorHandler>(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema);
    }

    static PipelinePlan getPipelinePlan(std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                                        Runtime::BufferManagerPtr bm, std::string pythonCompiler) {
        const SchemaPtr inputSchema = Schema::create()
                                          ->addField("l_shipdate", BasicType::INT32)
                                          ->addField("l_discount", BasicType::FLOAT32)
                                          ->addField("l_quantity", BasicType::INT32)
                                          ->addField("l_extendedprice", BasicType::FLOAT32);
        const SchemaPtr outputSchema = Schema::create()
                                           ->addField("revenue_before_aggregation", BasicType::FLOAT32);
        // cannot do aggregations with a Map UDF bc it has nothing to aggregate with

        std::map<std::string, std::string> modulesToImport;
        auto functionShipdate = "def tpch_query_6_shipdate(l_extendedprice, l_shipdate, l_discount, l_quantity):\n"
                                "\tif (l_shipdate >= 19940101) and (l_shipdate < 19950101):\n"
                                "\t\treturn (l_extendedprice, l_shipdate, l_discount, l_quantity)\n"
                                "\telse:\n"
                                "\t\treturn (0, 0, 0, 0)";

        auto functionNameShipdate = "tpch_query_6_shipdate";
        auto pythonUdfOperatorHandlerShipdate = initMapHandler(functionShipdate, functionNameShipdate, modulesToImport, pythonCompiler, inputSchema, inputSchema);

        auto functionDiscount = "def tpch_query_6_discount(l_extendedprice, l_shipdate, l_discount, l_quantity):\n"
                                 "\tif l_discount >= 0.05 and l_discount <= 0.07:\n"
                                 "\t\treturn (l_extendedprice, l_shipdate, l_discount, l_quantity)\n"
                                 "\telse:\n"
                                 "\t\treturn (0, 0, 0, 0)";

        auto functionNameDiscount = "tpch_query_6_discount";
        auto pythonUdfOperatorHandlerDiscount = initMapHandler(functionDiscount, functionNameDiscount, modulesToImport, pythonCompiler, inputSchema, inputSchema);

        auto functionQuantity = "def tpch_query_6_quantity(l_extendedprice, l_shipdate, l_discount, l_quantity):\n"
                                 "\tif l_quantity < 24:\n"
                                 "\t\treturn l_extendedprice * l_discount\n"
                                 "\telse:\n"
                                 "\t\treturn 0";

        auto functionNameQuantity = "tpch_query_6_quantity";
        auto pythonUdfOperatorHandlerQuantity = initMapHandler(functionQuantity, functionNameQuantity, modulesToImport, pythonCompiler, inputSchema, outputSchema);


        PipelinePlan plan;
        auto& lineitems = tables[TPCHTable::LineItem];

        // scan Operator
        auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineitems->getLayout()));
        std::vector<std::string> projections = {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"};
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr), projections);

        auto udfOperatorShipdate = std::make_shared<Operators::MapPythonUDF>(1, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(udfOperatorShipdate);
        auto udfOperatorDiscount = std::make_shared<Operators::MapPythonUDF>(2, inputSchema, outputSchema, pythonCompiler);
        udfOperatorShipdate->setChild(udfOperatorDiscount);
        auto udfOperatorQuantity = std::make_shared<Operators::MapPythonUDF>(3, inputSchema, outputSchema, pythonCompiler);
        udfOperatorDiscount->setChild(udfOperatorQuantity);

        // sum(l_extendedprice * l_discount)
        auto l_revenue_before_aggregation = std::make_shared<Expressions::ReadFieldExpression>("revenue_before_aggregation");
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());
        std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
            std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, l_revenue_before_aggregation, "revenue")};
        auto aggregation = std::make_shared<Operators::BatchAggregation>(0 /*handler index*/, aggregationFunctions);
        udfOperatorQuantity->setChild(aggregation);

        // create aggregation pipeline
        auto aggregationPipeline = std::make_shared<PhysicalOperatorPipeline>();
        aggregationPipeline->setRootOperator(scanOperator);
        std::vector<OperatorHandlerPtr> aggregationHandler = {std::make_shared<Operators::BatchAggregationHandler>(), pythonUdfOperatorHandlerShipdate, pythonUdfOperatorHandlerDiscount, pythonUdfOperatorHandlerQuantity};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        auto pipelineContext = MockedPipelineExecutionContext(aggregationHandler);
        plan.appendPipeline(aggregationPipeline, pipeline1Context);

        auto aggScan = std::make_shared<BatchAggregationScan>(0 /*handler index*/, aggregationFunctions);
        // emit operator
        auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        resultSchema->addField("revenue", BasicType::FLOAT32);
        auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultLayout);
        auto emit = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        aggScan->setChild(emit);

        // create aggregation pipeline
        auto emitPipeline = std::make_shared<PhysicalOperatorPipeline>();
        emitPipeline->setRootOperator(aggScan);
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        plan.appendPipeline(emitPipeline, pipeline2Context);
        return plan;
    }
};

class TPCH_Query6_Python_FlatMap {
  public:
    /**
     * Initializes the map handler for the given pipeline.
     * @param className python class name of the udf
     * @param methodName method name of the udf
     * @param inputProxyName input proxy class name
     * @param outputProxyName output proxy class name
     * @param schema schema of the input and output tuples
     * @param testDataPath path to the test data containing the udf jar
     * @return operator handler
     */
    auto static initMapHandler(std::string function,
                               std::string functionName,
                               std::map<std::string, std::string> modulesToImport,
                               std::string pythonCompiler,
                               SchemaPtr inputSchema,
                               SchemaPtr outputSchema) {
        return std::make_shared<Operators::PythonUDFOperatorHandler>(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema);
    }

    static PipelinePlan getPipelinePlan(std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                                        Runtime::BufferManagerPtr bm, std::string pythonCompiler) {
        const SchemaPtr inputSchema = Schema::create()
                                          ->addField("l_shipdate", BasicType::INT32)
                                          ->addField("l_discount", BasicType::FLOAT32)
                                          ->addField("l_quantity", BasicType::INT32)
                                          ->addField("l_extendedprice", BasicType::FLOAT32);
        const SchemaPtr outputSchema = Schema::create()
                                           ->addField("revenue", BasicType::FLOAT32);
        // cannot do aggregations with a Map UDF bc it has nothing to aggregate with

        auto function = "def tpch_query_6_flatmap(l_shipdate, l_discount, l_quantity, l_extendedprice):\n"
                        "  global collector\n"
                        "\n"
                        "  if (l_shipdate >= 19940101) and (l_shipdate < 19950101) and (l_discount >= 0.05) and (l_discount <= 0.07) and (l_quantity < 24):\n"
                        "    revenue = l_extendedprice * l_discount\n"
                        "    collector.append(revenue)\n"
                        "  \n"
                        "  if len(collector) > 0:\n"
                        "    total_revenue = sum(collector)\n"
                        "    return [total_revenue]\n"
                        "  else:\n"
                        "    return [0]";
        auto functionName = "tpch_query_6_flatmap";
        std::map<std::string, std::string> modulesToImport;
        auto pythonUdfOperatorHandler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema);

        PipelinePlan plan;
        auto& lineitems = tables[TPCHTable::LineItem];

        auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineitems->getLayout()));
        std::vector<std::string> projections = {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"};
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr), projections);

        auto udfOperator = std::make_shared<Operators::FlatMapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(udfOperator);

        // emit operator
        auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        resultSchema->addField("revenue", BasicType::FLOAT32);
        auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultLayout);
        auto emit = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        udfOperator->setChild(emit);

        // create aggregation pipeline
        auto aggregationPipeline = std::make_shared<PhysicalOperatorPipeline>();
        aggregationPipeline->setRootOperator(scanOperator);
        std::vector<OperatorHandlerPtr> aggregationHandler = {pythonUdfOperatorHandler};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        auto pipelineContext = MockedPipelineExecutionContext(aggregationHandler);
        plan.appendPipeline(aggregationPipeline, pipeline1Context);

        return plan;
    }
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY6_PYTHON_UDF_HPP_

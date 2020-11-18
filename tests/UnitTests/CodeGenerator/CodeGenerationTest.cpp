/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <API/UserAPIExpression.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Definitions/FunctionDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ReturnStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarDeclStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <State/StateVariable.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/WindowHandlerFactoryDetails.hpp>
#include <cassert>
#include <cmath>
#include <gtest/gtest.h>
#include <iostream>
#include <utility>

#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnRecordTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>

using std::cout;
using std::endl;
namespace NES {

class CodeGenerationTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("CodeGenerationTest.log", NES::LOG_DEBUG);
        std::cout << "Setup CodeGenerationTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        std::cout << "Setup CodeGenerationTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down CodeGenerationTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down CodeGenerationTest test class." << std::endl;
    }
};

class TestPipelineExecutionContext : public PipelineExecutionContext {
  public:
    TestPipelineExecutionContext(BufferManagerPtr bufferManager, AbstractWindowHandlerPtr windowHandler, AbstractWindowHandlerPtr joinHandler)
        : PipelineExecutionContext(
            0, std::move(bufferManager), [this](TupleBuffer& buffer, WorkerContextRef) {
                this->buffers.emplace_back(std::move(buffer));
            },
            std::move(windowHandler), std::move(joinHandler), nullptr, nullptr, nullptr) {
            // nop
        };

    std::vector<TupleBuffer> buffers;
};

/**
 * @brief This test checks the behavior of the code generation API
 */
TEST_F(CodeGenerationTest, codeGenerationApiTest) {
    auto tf = CompilerTypesFactory();
    auto varDeclI = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()), "i", DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));
    auto varDeclJ =
        VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()), "j", DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "5"));
    auto varDeclK = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()), "k",
                                                DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "7"));

    auto varDeclL = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()), "l", DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "2"));

    {
        // Generate Arithmetic Operation
        BinaryOperatorStatement binOp(VarRefStatement(varDeclI), PLUS_OP,
                                      VarRefStatement(varDeclJ));
        EXPECT_EQ(binOp.getCode()->code_, "i+j");
        BinaryOperatorStatement binOp2 = binOp.addRight(MINUS_OP,
                                                        VarRefStatement(varDeclK));
        EXPECT_EQ(binOp2.getCode()->code_, "i+j-k");
    }
    {
        // Generate Array Operation
        std::vector<std::string> vals = {"a", "b", "c"};
        auto varDeclM = VariableDeclaration::create(
            tf.createDataType(DataTypeFactory::createFixedChar(12)), "m",
            DataTypeFactory::createFixedCharValue(vals));
        // declaration of m
        EXPECT_EQ(VarRefStatement(varDeclM).getCode()->code_, "m");

        // Char Array initialization
        auto varDeclN = VariableDeclaration::create(
            tf.createDataType(DataTypeFactory::createFixedChar(12)), "n",
            DataTypeFactory::createFixedCharValue(vals));
        EXPECT_EQ(varDeclN.getCode(), "char n[12] = {'a', 'b', 'c'}");

        // Int Array initialization
        auto varDeclO = VariableDeclaration::create(
            tf.createDataType(DataTypeFactory::createArray(4, DataTypeFactory::createUInt8())), "o",
            DataTypeFactory::createArrayValue(DataTypeFactory::createUInt8(), {"2", "3", "4"}));
        EXPECT_EQ(varDeclO.getCode(), "uint8_t o[4] = {2, 3, 4}");

        /**
         todo currently no support for strings
        // String Array initialization
        auto stringValueType = createStringValueType(
                                   "DiesIstEinZweiterTest\0dwqdwq")
                                   ->getCodeExpression()
                                   ->code_;
        EXPECT_EQ(stringValueType, "\"DiesIstEinZweiterTest\"");

        auto charValueType = createBasicTypeValue(BasicType::CHAR,
                                                  "DiesIstEinDritterTest")
                                 ->getCodeExpression()
                                 ->code_;
        EXPECT_EQ(charValueType, "DiesIstEinDritterTest");
         */
    }

    {
        auto code = BinaryOperatorStatement(VarRefStatement(varDeclI), PLUS_OP,
                                            VarRefStatement(varDeclJ))
                        .addRight(
                            PLUS_OP, VarRefStatement(varDeclK))
                        .addRight(MULTIPLY_OP,
                                  VarRefStatement(varDeclI),
                                  BRACKETS)
                        .addRight(
                            GREATER_THAN_OP, VarRefStatement(varDeclL))
                        .getCode();

        EXPECT_EQ(code->code_, "(i+j+k*i)>l");

        // We have two ways to generate code for arithmetical operations, we check here if they result in the same code
        auto plusOperatorCode = BinaryOperatorStatement(VarRefStatement(varDeclI),
                                                        PLUS_OP,
                                                        VarRefStatement(varDeclJ))
                                    .getCode()
                                    ->code_;
        auto plusOperatorCodeOp = (VarRefStatement(varDeclI)
                                   + VarRefStatement(varDeclJ))
                                      .getCode()
                                      ->code_;
        EXPECT_EQ(plusOperatorCode, plusOperatorCodeOp);

        // Prefix and postfix increment
        auto postfixIncrement = UnaryOperatorStatement(VarRefStatement(varDeclI),
                                                       POSTFIX_INCREMENT_OP);
        EXPECT_EQ(postfixIncrement.getCode()->code_, "i++");
        auto prefixIncrement = (++VarRefStatement(varDeclI));
        EXPECT_EQ(prefixIncrement.getCode()->code_, "++i");

        // Comparision
        auto comparision =
            (VarRefStatement(varDeclI) >= VarRefStatement(varDeclJ))[VarRefStatement(
                varDeclJ)];
        EXPECT_EQ(comparision.getCode()->code_, "i>=j[j]");

        // Negation
        auto negate =
            ((~VarRefStatement(varDeclI)
              >= VarRefStatement(varDeclJ)
                  << ConstantExpressionStatement(tf.createValueType(
                         DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))))[VarRefStatement(varDeclJ)]);
        EXPECT_EQ(negate.getCode()->code_, "~i>=j<<0[j]");

        auto addition = VarRefStatement(varDeclI).assign(
            VarRefStatement(varDeclI) + VarRefStatement(varDeclJ));
        EXPECT_EQ(addition.getCode()->code_, "i=i+j");

        auto sizeOfStatement = (sizeOf(VarRefStatement(varDeclI)));
        EXPECT_EQ(sizeOfStatement.getCode()->code_, "sizeof(i)");

        auto assignStatement = assign(VarRef(varDeclI), VarRef(varDeclI));
        EXPECT_EQ(assignStatement.getCode()->code_, "i=i");

        // if statements
        auto ifStatement = IF(
            VarRef(varDeclI) < VarRef(varDeclJ),
            assign(VarRef(varDeclI), VarRef(varDeclI) * VarRef(varDeclK)));
        EXPECT_EQ(ifStatement.getCode()->code_, "if(i<j){\ni=i*k;\n\n}\n");

        auto ifStatementReturn = IFStatement(
            BinaryOperatorStatement(VarRefStatement(varDeclI), GREATER_THAN_OP,
                                    VarRefStatement(varDeclJ)),
            ReturnStatement(VarRefStatement(varDeclI)));
        EXPECT_EQ(ifStatementReturn.getCode()->code_,
                  "if(i>j){\nreturn i;;\n\n}\n");

        auto compareWithOne = IFStatement(VarRefStatement(varDeclJ),
                                          VarRefStatement(varDeclI));
        EXPECT_EQ(compareWithOne.getCode()->code_, "if(j){\ni;\n\n}\n");
    }

    {
        auto compareAssign = BinaryOperatorStatement(
            VarRefStatement(varDeclK),
            ASSIGNMENT_OP,
            BinaryOperatorStatement(VarRefStatement(varDeclJ), GREATER_THAN_OP,
                                    VarRefStatement(varDeclI)));
        EXPECT_EQ(compareAssign.getCode()->code_, "k=j>i");
    }

    {
        // check declaration types
        auto variableDeclaration = VariableDeclaration::create(
            tf.createDataType(DataTypeFactory::createInt32()), "num_tuples",
            DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));

        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   ADDRESS_OF_OP)
                .getCode()
                ->code_,
            "&num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   DEREFERENCE_POINTER_OP)
                .getCode()
                ->code_,
            "*num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   PREFIX_INCREMENT_OP)
                .getCode()
                ->code_,
            "++num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   PREFIX_DECREMENT_OP)
                .getCode()
                ->code_,
            "--num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   POSTFIX_INCREMENT_OP)
                .getCode()
                ->code_,
            "num_tuples++");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   POSTFIX_DECREMENT_OP)
                .getCode()
                ->code_,
            "num_tuples--");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   BITWISE_COMPLEMENT_OP)
                .getCode()
                ->code_,
            "~num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   LOGICAL_NOT_OP)
                .getCode()
                ->code_,
            "!num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   SIZE_OF_TYPE_OP)
                .getCode()
                ->code_,
            "sizeof(num_tuples)");
    }

    {
        // check code generation for loops
        auto varDeclQ = VariableDeclaration::create(
            tf.createDataType(DataTypeFactory::createInt32()), "q",
            DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));
        auto varDeclNumTuple = VariableDeclaration::create(
            tf.createDataType(DataTypeFactory::createInt32()), "num_tuples",
            DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));

        auto varDeclSum = VariableDeclaration::create(
            tf.createDataType(DataTypeFactory::createInt32()), "sum",
            DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));

        ForLoopStatement loopStmt(
            varDeclQ.copy(),
            BinaryOperatorStatement(VarRefStatement(varDeclQ), LESS_THAN_OP,
                                    VarRefStatement(varDeclNumTuple))
                .copy(),
            UnaryOperatorStatement(VarRefStatement(varDeclQ), PREFIX_INCREMENT_OP).copy());

        loopStmt.addStatement(
            BinaryOperatorStatement(
                VarRefStatement(varDeclSum),
                ASSIGNMENT_OP,
                BinaryOperatorStatement(VarRefStatement(varDeclSum), PLUS_OP,
                                        VarRefStatement(varDeclQ)))
                .copy());

        EXPECT_EQ(loopStmt.getCode()->code_,
                  "for(int32_t q = 0;q<num_tuples;++q){\nsum=sum+q;\n\n}\n");

        auto forLoop = ForLoopStatement(
            varDeclQ.copy(),
            BinaryOperatorStatement(VarRefStatement(varDeclQ), LESS_THAN_OP,
                                    VarRefStatement(varDeclNumTuple))
                .copy(),
            UnaryOperatorStatement(VarRefStatement(varDeclQ), PREFIX_INCREMENT_OP).copy());

        EXPECT_EQ(forLoop.getCode()->code_,
                  "for(int32_t q = 0;q<num_tuples;++q){\n\n}\n");

        auto compareAssignment = BinaryOperatorStatement(
            VarRefStatement(varDeclK),
            ASSIGNMENT_OP,
            BinaryOperatorStatement(VarRefStatement(varDeclJ), GREATER_THAN_OP,
                                    ConstantExpressionStatement(tf.createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "5")))));

        EXPECT_EQ(compareAssignment.getCode()->code_, "k=j>5");
    }

    {
        /* check code generation of pointers */
        auto val = tf.createPointer(tf.createDataType(DataTypeFactory::createInt32()));
        assert(val != nullptr);
        auto variableDeclarationI = VariableDeclaration::create(
            tf.createDataType(DataTypeFactory::createInt32()), "i",
            DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));
        auto variableDeclarationP = VariableDeclaration::create(val, "array");
        EXPECT_EQ(variableDeclarationP.getCode(), "int32_t* array");

        /* new String Type */
        /**
         * @brief Todo add string

        auto charPointerDataType = tf.createPointer(tf.createDataType(DataBasicType(CHAR));
        auto var_decl_temp = VariableDeclaration::create(
            charPointerDataType, "i", createStringValueType("Hello World"));
        EXPECT_EQ(var_decl_temp.getCode(), "char* i = \"Hello World\"");
   */
        auto tupleBufferStructDecl = StructDeclaration::create("TupleBuffer",
                                                               "buffer")
                                         .addField(
                                             VariableDeclaration::create(
                                                 tf.createDataType(DataTypeFactory::createUInt64()), "num_tuples",
                                                 DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0")))
                                         .addField(
                                             variableDeclarationP);

        // check code generation for different assignment type
        auto varDeclTupleBuffer = VariableDeclaration::create(
            tf.createUserDefinedType(tupleBufferStructDecl), "buffer");
        EXPECT_EQ(varDeclTupleBuffer.getCode(), "TupleBuffer");

        auto varDeclTupleBufferPointer = VariableDeclaration::create(
            tf.createPointer(tf.createUserDefinedType(tupleBufferStructDecl)),
            "buffer");
        EXPECT_EQ(varDeclTupleBufferPointer.getCode(), "TupleBuffer* buffer");

        auto pointerDataType = tf.createPointer(tf.createUserDefinedType(tupleBufferStructDecl));
        EXPECT_EQ(pointerDataType->getCode()->code_, "TupleBuffer*");

        auto typeDefinition = VariableDeclaration::create(
                                  tf.createPointer(tf.createUserDefinedType(tupleBufferStructDecl)),
                                  "buffer")
                                  .getTypeDefinitionCode();
        EXPECT_EQ(
            typeDefinition,
            "struct TupleBuffer{\nuint64_t num_tuples = 0;\nint32_t* array;\n}buffer");
    }
}

/**
 * @brief Simple test that generates code to process a input buffer and calculate a running sum.
 */
TEST_F(CodeGenerationTest, codeGenRunningSum) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    NodeEnginePtr nodeEngine = NodeEngine::create("127.0.0.1", 6262, streamConf);
    auto tf = CompilerTypesFactory();
    auto tupleBufferType = tf.createAnonymusDataType("NES::TupleBuffer");
    auto pipelineExecutionContextType = tf.createAnonymusDataType("NES::PipelineExecutionContext");
    auto workerContextType = tf.createAnonymusDataType("NES::WorkerContext");
    auto getNumberOfTupleBuffer = FunctionCallStatement("getNumberOfTuples");
    auto allocateTupleBuffer = FunctionCallStatement("allocateTupleBuffer");

    auto getBufferOfTupleBuffer = FunctionCallStatement("getBuffer");

    /* struct definition for input tuples */
    auto structDeclTuple = StructDeclaration::create("Tuple", "").addField(VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt64()), "campaign_id"));

    /* struct definition for result tuples */

    auto structDeclResultTuple = StructDeclaration::create("ResultTuple", "")
                                     .addField(
                                         VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt64()), "sum"));

    /* === declarations === */
    auto varDeclTupleBuffers = VariableDeclaration::create(tf.createReference(tupleBufferType),
                                                           "input_buffer");
    auto varDeclPipelineExecutionContext =
        VariableDeclaration::create(tf.createReference(pipelineExecutionContextType),
                                    "pipelineExecutionContext");
    auto varDeclWorkerContext =
        VariableDeclaration::create(tf.createReference(workerContextType),
                                    "workerContext");
    /* Tuple *tuples; */
    auto varDeclTuple = VariableDeclaration::create(
        tf.createPointer(tf.createUserDefinedType(structDeclTuple)), "tuples");

    auto varDeclResultTuple = VariableDeclaration::create(
        tf.createPointer(tf.createUserDefinedType(structDeclResultTuple)),
        "resultTuples");

    /* variable declarations for fields inside structs */
    auto declFieldCampaignId = structDeclTuple.getVariableDeclaration(
        "campaign_id");

    auto varDeclFieldResultTupleSum = structDeclResultTuple.getVariableDeclaration("sum");

    /* === generating the query function === */

    /* variable declarations */

    /* uint64_t id = 0; */
    auto varDeclId = VariableDeclaration::create(
        tf.createDataType(DataTypeFactory::createUInt64()), "id",
        DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), "0"));
    /* int32_t ret = 0; */
    auto varDeclReturn = VariableDeclaration::create(
        tf.createDataType(DataTypeFactory::createInt32()), "ret",
        DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));
    /* int32_t sum = 0;*/
    auto varDeclSum = VariableDeclaration::create(
        tf.createDataType(DataTypeFactory::createInt64()), "sum",
        DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"));

    /* init statements before for loop */

    /*  tuples = (Tuple *)tuple_buffer.getBuffer();*/
    BinaryOperatorStatement initTuplePtr(
        VarRef(varDeclTuple)
            .assign(TypeCast(VarRefStatement(varDeclTupleBuffers).accessRef(getBufferOfTupleBuffer), tf.createPointer(tf.createUserDefinedType(structDeclTuple)))));

    /* result_tuples = (ResultTuple *)output_tuple_buffer->data;*/
    auto resultTupleBufferDeclaration = VariableDeclaration::create(tupleBufferType, "resultTupleBuffer");
    BinaryOperatorStatement initResultTupleBufferPtr(VarDeclStatement(resultTupleBufferDeclaration).assign(VarRef(varDeclPipelineExecutionContext).accessRef(allocateTupleBuffer)));

    BinaryOperatorStatement initResultTuplePtr(
        VarRef(varDeclResultTuple).assign(TypeCast(VarRef(resultTupleBufferDeclaration).accessRef(getBufferOfTupleBuffer), tf.createPointer(tf.createUserDefinedType(structDeclResultTuple)))));

    /* for (uint64_t id = 0; id < tuple_buffer_1->num_tuples; ++id) */
    FOR loopStmt(
        varDeclId.copy(),
        (VarRef(varDeclId)
         < (VarRef(varDeclTupleBuffers).accessRef(getNumberOfTupleBuffer)))
            .copy(),
        (++VarRef(varDeclId)).copy());

    /* sum = sum + tuples[id].campaign_id; */
    loopStmt.addStatement(
        VarRef(varDeclSum).assign(VarRef(varDeclSum) + VarRef(varDeclTuple)[VarRef(varDeclId)].accessRef(VarRef(declFieldCampaignId))).copy());

    /* function signature:
     * typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*);
     */
    auto emitTupleBuffer = FunctionCallStatement("emitBuffer");
    emitTupleBuffer.addParameter(VarRef(resultTupleBufferDeclaration));
    emitTupleBuffer.addParameter(VarRef(varDeclWorkerContext));
    auto mainFunction = FunctionDefinition::create("compiled_query")
                            ->returns(tf.createDataType(DataTypeFactory::createInt32()))
                            ->addParameter(varDeclTupleBuffers)
        ->addParameter(varDeclPipelineExecutionContext)
        ->addParameter(varDeclWorkerContext)
        ->addVariableDeclaration(varDeclReturn)
        ->addVariableDeclaration(varDeclTuple)
        ->addVariableDeclaration(varDeclResultTuple)
        ->addVariableDeclaration(varDeclSum)
        ->addStatement(initTuplePtr.copy())
        ->addStatement(initResultTupleBufferPtr.copy())
        ->addStatement(initResultTuplePtr.copy())
        ->addStatement(StatementPtr(new ForLoopStatement(loopStmt)))
        ->addStatement(
                                /*   result_tuples[0].sum = sum; */
                                VarRef(varDeclResultTuple)[Constant(tf.createValueType(DataTypeFactory::createBasicValue(
                                                               DataTypeFactory::createInt32(), "0")))]
                                    .accessRef(VarRef(varDeclFieldResultTupleSum))
                                    .assign(VarRef(varDeclSum))
                                    .copy())
        ->addStatement(VarRef(varDeclPipelineExecutionContext).accessRef(emitTupleBuffer).copy())
                            /* return ret; */

        ->addStatement(StatementPtr(new ReturnStatement(VarRefStatement(varDeclReturn))))
        ->getDeclaration();

    auto file = FileBuilder::create("query.cpp")
                    .addDeclaration(structDeclTuple.copy())
                    .addDeclaration(structDeclResultTuple.copy())
                    .addDeclaration(mainFunction)
                    .build();

    Compiler compiler;
    auto stage = CompiledExecutablePipeline::create(compiler.compile(file.code, true /** debugging **/));

    /* setup input and output for test */
    auto inputBuffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto recordSchema = Schema::create()->addField("id", DataTypeFactory::createInt64());
    auto layout = createRowLayout(recordSchema);

    for (uint32_t recordIndex = 0; recordIndex < 100; ++recordIndex) {
        layout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->write(inputBuffer, recordIndex);
    }
    inputBuffer.setNumberOfTuples(100);

    /* execute code */
    auto wctx = WorkerContext{0};
    auto context = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(), nullptr, nullptr);
    ASSERT_EQ(stage->execute(inputBuffer, context, wctx), 0);
    auto outputBuffer = context->buffers[0];
    NES_INFO(UtilityFunctions::prettyPrintTupleBuffer(outputBuffer, recordSchema));
    /* check result for correctness */
    auto sumGeneratedCode = layout->getValueField<int64_t>(/*recordIndex*/ 0, /*fieldIndex*/ 0)->read(outputBuffer);
    auto sum = 0;
    for (uint64_t recordIndex = 0; recordIndex < 100; ++recordIndex) {
        sum += layout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->read(inputBuffer);
    }
    EXPECT_EQ(sum, sumGeneratedCode);
}

}// namespace NES

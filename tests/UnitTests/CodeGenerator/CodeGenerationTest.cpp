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
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/ClassDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/FunctionDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/NamespaceDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ReturnStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarDeclStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <cassert>
#include <cmath>
#include <gtest/gtest.h>
#include <iostream>
#include <utility>

#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Runtime/SharedPointerGen.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipelineStage.hpp>

using std::cout;
using std::endl;
namespace NES {
using namespace QueryCompilation;

class CodeGenerationTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("CodeGenerationTest.log", NES::LOG_DEBUG);
        std::cout << "Setup CodeGenerationTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup CodeGenerationTest test case." << std::endl;
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                  6262,
                                                                  streamConf,
                                                                  1,
                                                                  4096,
                                                                  1024,
                                                                  12,
                                                                  12,
                                                                  NES::Runtime::NumaAwarenessFlag::DISABLED);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        std::cout << "Tear down CodeGenerationTest test case." << std::endl;
        nodeEngine->stop();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down CodeGenerationTest test class." << std::endl; }

    Runtime::NodeEnginePtr nodeEngine;
};

class TestPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    TestPipelineExecutionContext(Runtime::QueryManagerPtr queryManager,
                                 std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers)
        : PipelineExecutionContext(
            0,
            std::move(queryManager),
            [this](Runtime::TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->buffers.emplace_back(std::move(buffer));
            },
            [this](Runtime::TupleBuffer& buffer) {
                this->buffers.emplace_back(std::move(buffer));
            },
            std::move(operatorHandlers)){
            // nop
        };

    std::vector<Runtime::TupleBuffer> buffers;
};

/**
 * @brief This test checks the behavior of the code generation API
 */
TEST_F(CodeGenerationTest, codeGenerationApiTest) {
    auto tf = QueryCompilation::GeneratableTypesFactory();
    auto varDeclI = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()),
                                                "i",
                                                DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));
    auto varDeclJ = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()),
                                                "j",
                                                DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "5"));
    auto varDeclK = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()),
                                                "k",
                                                DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "7"));

    auto varDeclL = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()),
                                                "l",
                                                DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "2"));

    {
        // Generate Arithmetic Operation
        BinaryOperatorStatement binOp(VarRefStatement(varDeclI), PLUS_OP, VarRefStatement(varDeclJ));
        EXPECT_EQ(binOp.getCode()->code_, "i+j");
        BinaryOperatorStatement binOp2 = binOp.addRight(MINUS_OP, VarRefStatement(varDeclK));
        EXPECT_EQ(binOp2.getCode()->code_, "i+j-k");
    }
    {
        // Generate ArrayType Operation
        //std::vector<std::string> vals = {"a", "b", "c"};
        std::string vals = "abc";
        auto varDeclM = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createFixedChar(12)),
                                                    "m",
                                                    DataTypeFactory::createFixedCharValue(vals));
        // declaration of m
        EXPECT_EQ(VarRefStatement(varDeclM).getCode()->code_, "m");

        // Char Array initialization
        auto varDeclN = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createFixedChar(12)),
                                                    "n",
                                                    DataTypeFactory::createFixedCharValue(vals));
        EXPECT_EQ(
            varDeclN.getCode(),
            "NES::QueryCompilation::Array<char, 12> n = NES::QueryCompilation::Array {'a', 'b', 'c', static_cast<char>(0)}");

        auto varDeclO = VariableDeclaration::create(
            tf.createDataType(DataTypeFactory::createArray(4, DataTypeFactory::createUInt8())),
            "o",
            DataTypeFactory::createArrayValueWithContainedType(DataTypeFactory::createUInt8(), {"2", "3", "4"}));
        EXPECT_EQ(varDeclO.getCode(), "NES::QueryCompilation::Array<uint8_t, 4> o = NES::QueryCompilation::Array {2, 3, 4}");

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
        auto code = BinaryOperatorStatement(VarRefStatement(varDeclI), PLUS_OP, VarRefStatement(varDeclJ))
                        .addRight(PLUS_OP, VarRefStatement(varDeclK))
                        .addRight(MULTIPLY_OP, VarRefStatement(varDeclI), BRACKETS)
                        .addRight(GREATER_THAN_OP, VarRefStatement(varDeclL))
                        .getCode();

        EXPECT_EQ(code->code_, "(i+j+k*i)>l");

        // We have two ways to generate code for arithmetical operations, we check here if they result in the same code
        auto plusOperatorCode =
            BinaryOperatorStatement(VarRefStatement(varDeclI), PLUS_OP, VarRefStatement(varDeclJ)).getCode()->code_;
        auto plusOperatorCodeOp = (VarRefStatement(varDeclI) + VarRefStatement(varDeclJ)).getCode()->code_;
        EXPECT_EQ(plusOperatorCode, plusOperatorCodeOp);

        // Prefix and postfix increment
        auto postfixIncrement = UnaryOperatorStatement(VarRefStatement(varDeclI), POSTFIX_INCREMENT_OP);
        EXPECT_EQ(postfixIncrement.getCode()->code_, "i++");
        auto prefixIncrement = (++VarRefStatement(varDeclI));
        EXPECT_EQ(prefixIncrement.getCode()->code_, "++i");

        // Comparision
        auto comparision = (VarRefStatement(varDeclI) >= VarRefStatement(varDeclJ))[VarRefStatement(varDeclJ)];
        EXPECT_EQ(comparision.getCode()->code_, "i>=j[j]");

        // Negation
        auto negate =
            ((~VarRefStatement(varDeclI) >= VarRefStatement(varDeclJ)
                  << ConstantExpressionStatement(NES::QueryCompilation::GeneratableTypesFactory::createValueType(
                         DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))))[VarRefStatement(varDeclJ)]);
        EXPECT_EQ(negate.getCode()->code_, "~i>=j<<0[j]");

        auto addition = VarRefStatement(varDeclI).assign(VarRefStatement(varDeclI) + VarRefStatement(varDeclJ));
        EXPECT_EQ(addition.getCode()->code_, "i=i+j");

        auto sizeOfStatement = (sizeOf(VarRefStatement(varDeclI)));
        EXPECT_EQ(sizeOfStatement.getCode()->code_, "sizeof(i)");

        auto assignStatement = assign(VarRef(varDeclI), VarRef(varDeclI));
        EXPECT_EQ(assignStatement.getCode()->code_, "i=i");

        // if statements
        auto ifStatement = IF(VarRef(varDeclI) < VarRef(varDeclJ), assign(VarRef(varDeclI), VarRef(varDeclI) * VarRef(varDeclK)));
        EXPECT_EQ(ifStatement.getCode()->code_, "if(i<j){\ni=i*k;\n\n}\n");

        auto ifStatementReturn = IFStatement(
            BinaryOperatorStatement(VarRefStatement(varDeclI), GREATER_THAN_OP, VarRefStatement(varDeclJ)).createCopy(),
            ReturnStatement::create(VarRefStatement(varDeclI).createCopy()));
        EXPECT_EQ(ifStatementReturn.getCode()->code_, "if(i>j){\nreturn i;;\n\n}\n");

        auto compareWithOne = IFStatement(VarRefStatement(varDeclJ), VarRefStatement(varDeclI));
        EXPECT_EQ(compareWithOne.getCode()->code_, "if(j){\ni;\n\n}\n");
    }

    {
        auto compareAssign = BinaryOperatorStatement(
            VarRefStatement(varDeclK),
            ASSIGNMENT_OP,
            BinaryOperatorStatement(VarRefStatement(varDeclJ), GREATER_THAN_OP, VarRefStatement(varDeclI)));
        EXPECT_EQ(compareAssign.getCode()->code_, "k=j>i");
    }

    {
        // check declaration types
        auto variableDeclaration =
            VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()),
                                        "num_tuples",
                                        DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));

        EXPECT_EQ(UnaryOperatorStatement(VarRefStatement(variableDeclaration), ADDRESS_OF_OP).getCode()->code_, "&num_tuples");
        EXPECT_EQ(UnaryOperatorStatement(VarRefStatement(variableDeclaration), DEREFERENCE_POINTER_OP).getCode()->code_,
                  "*num_tuples");
        EXPECT_EQ(UnaryOperatorStatement(VarRefStatement(variableDeclaration), PREFIX_INCREMENT_OP).getCode()->code_,
                  "++num_tuples");
        EXPECT_EQ(UnaryOperatorStatement(VarRefStatement(variableDeclaration), PREFIX_DECREMENT_OP).getCode()->code_,
                  "--num_tuples");
        EXPECT_EQ(UnaryOperatorStatement(VarRefStatement(variableDeclaration), POSTFIX_INCREMENT_OP).getCode()->code_,
                  "num_tuples++");
        EXPECT_EQ(UnaryOperatorStatement(VarRefStatement(variableDeclaration), POSTFIX_DECREMENT_OP).getCode()->code_,
                  "num_tuples--");
        EXPECT_EQ(UnaryOperatorStatement(VarRefStatement(variableDeclaration), BITWISE_COMPLEMENT_OP).getCode()->code_,
                  "~num_tuples");
        EXPECT_EQ(UnaryOperatorStatement(VarRefStatement(variableDeclaration), LOGICAL_NOT_OP).getCode()->code_, "!num_tuples");
        EXPECT_EQ(UnaryOperatorStatement(VarRefStatement(variableDeclaration), SIZE_OF_TYPE_OP).getCode()->code_,
                  "sizeof(num_tuples)");
        EXPECT_EQ(UnaryOperatorStatement(VarRefStatement(variableDeclaration), ABSOLUTE_VALUE_OF_OP).getCode()->code_,
                  "abs(num_tuples)");
    }

    {
        // check code generation for loops
        auto varDeclQ = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()),
                                                    "q",
                                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));
        auto varDeclNumTuple =
            VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()),
                                        "num_tuples",
                                        DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));

        auto varDeclSum = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()),
                                                      "sum",
                                                      DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"));

        ForLoopStatement loopStmt(
            varDeclQ.copy(),
            BinaryOperatorStatement(VarRefStatement(varDeclQ), LESS_THAN_OP, VarRefStatement(varDeclNumTuple)).copy(),
            UnaryOperatorStatement(VarRefStatement(varDeclQ), PREFIX_INCREMENT_OP).copy());

        loopStmt.addStatement(
            BinaryOperatorStatement(VarRefStatement(varDeclSum),
                                    ASSIGNMENT_OP,
                                    BinaryOperatorStatement(VarRefStatement(varDeclSum), PLUS_OP, VarRefStatement(varDeclQ)))
                .copy());

        EXPECT_EQ(loopStmt.getCode()->code_, "for(int32_t q = 0;q<num_tuples;++q){\nsum=sum+q;\n\n}\n");

        auto forLoop = ForLoopStatement(
            varDeclQ.copy(),
            BinaryOperatorStatement(VarRefStatement(varDeclQ), LESS_THAN_OP, VarRefStatement(varDeclNumTuple)).copy(),
            UnaryOperatorStatement(VarRefStatement(varDeclQ), PREFIX_INCREMENT_OP).copy());

        EXPECT_EQ(forLoop.getCode()->code_, "for(int32_t q = 0;q<num_tuples;++q){\n\n}\n");

        auto compareAssignment = BinaryOperatorStatement(
            VarRefStatement(varDeclK),
            ASSIGNMENT_OP,
            BinaryOperatorStatement(VarRefStatement(varDeclJ),
                                    GREATER_THAN_OP,
                                    ConstantExpressionStatement(NES::QueryCompilation::GeneratableTypesFactory::createValueType(
                                        DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "5")))));

        EXPECT_EQ(compareAssignment.getCode()->code_, "k=j>5");
    }

    {
        /* check code generation of pointers */
        auto val =
            NES::QueryCompilation::GeneratableTypesFactory::createPointer(tf.createDataType(DataTypeFactory::createInt32()));
        assert(val != nullptr);
        auto variableDeclarationI =
            VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt32()),
                                        "i",
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
        auto tupleBufferStructDecl =
            StructDeclaration::create("TupleBuffer", "buffer")
                .addField(VariableDeclaration::create(tf.createDataType(DataTypeFactory::createUInt64()),
                                                      "num_tuples",
                                                      DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0")))
                .addField(variableDeclarationP);

        // check code generation for different assignment type
        auto varDeclTupleBuffer = VariableDeclaration::create(
            NES::QueryCompilation::GeneratableTypesFactory::createUserDefinedType(tupleBufferStructDecl),
            "buffer");
        EXPECT_EQ(varDeclTupleBuffer.getCode(), "TupleBuffer buffer");

        auto varDeclTupleBufferPointer = VariableDeclaration::create(
            NES::QueryCompilation::GeneratableTypesFactory::createPointer(
                NES::QueryCompilation::GeneratableTypesFactory::createUserDefinedType(tupleBufferStructDecl)),
            "buffer");
        EXPECT_EQ(varDeclTupleBufferPointer.getCode(), "TupleBuffer* buffer");

        auto pointerDataType = NES::QueryCompilation::GeneratableTypesFactory::createPointer(
            NES::QueryCompilation::GeneratableTypesFactory::createUserDefinedType(tupleBufferStructDecl));
        EXPECT_EQ(pointerDataType->getCode()->code_, "TupleBuffer*");

        auto typeDefinition =
            VariableDeclaration::create(
                NES::QueryCompilation::GeneratableTypesFactory::createPointer(
                    NES::QueryCompilation::GeneratableTypesFactory::createUserDefinedType(tupleBufferStructDecl)),
                "buffer")
                .getTypeDefinitionCode();
        EXPECT_EQ(typeDefinition, "struct TupleBuffer{\nuint64_t num_tuples = 0;\nint32_t* array;\n}buffer");
    }
}

/**
 * @brief Simple test that generates code to process a input buffer and calculate a running sum.
 */
TEST_F(CodeGenerationTest, codeGenRunningSum) {
    auto tf = GeneratableTypesFactory();
    auto tupleBufferType = NES::QueryCompilation::GeneratableTypesFactory::createAnonymusDataType("Runtime::TupleBuffer");
    auto pipelineExecutionContextType =
        NES::QueryCompilation::GeneratableTypesFactory::createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    auto workerContextType = NES::QueryCompilation::GeneratableTypesFactory::createAnonymusDataType("Runtime::WorkerContext");
    auto getNumberOfTupleBuffer = FunctionCallStatement("getNumberOfTuples");
    auto allocateTupleBuffer = FunctionCallStatement("allocateTupleBuffer");

    auto getBufferOfTupleBuffer = FunctionCallStatement("getBuffer");

    /* struct definition for input tuples */
    auto structDeclTuple =
        StructDeclaration::create("Tuple", "")
            .addField(VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt64()), "campaign_id"));

    /* struct definition for result tuples */

    auto structDeclResultTuple =
        StructDeclaration::create("ResultTuple", "")
            .addField(VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt64()), "sum"));

    /* === declarations === */
    auto varDeclTupleBuffers =
        VariableDeclaration::create(NES::QueryCompilation::GeneratableTypesFactory::createReference(tupleBufferType),
                                    "input_buffer");
    auto varDeclPipelineExecutionContext =
        VariableDeclaration::create(NES::QueryCompilation::GeneratableTypesFactory::createReference(pipelineExecutionContextType),
                                    "pipelineExecutionContext");
    auto varDeclWorkerContext =
        VariableDeclaration::create(NES::QueryCompilation::GeneratableTypesFactory::createReference(workerContextType),
                                    "workerContext");
    /* Tuple *tuples; */
    auto varDeclTuple =
        VariableDeclaration::create(NES::QueryCompilation::GeneratableTypesFactory::createPointer(
                                        NES::QueryCompilation::GeneratableTypesFactory::createUserDefinedType(structDeclTuple)),
                                    "tuples");

    auto varDeclResultTuple = VariableDeclaration::create(
        NES::QueryCompilation::GeneratableTypesFactory::createPointer(
            NES::QueryCompilation::GeneratableTypesFactory::createUserDefinedType(structDeclResultTuple)),
        "resultTuples");

    /* variable declarations for fields inside structs */
    auto declFieldCampaignId = structDeclTuple.getVariableDeclaration("campaign_id");

    auto varDeclFieldResultTupleSum = structDeclResultTuple.getVariableDeclaration("sum");

    /* === generating the query function === */

    /* variable declarations */

    /* uint64_t id = 0; */
    auto varDeclId = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createUInt64()),
                                                 "id",
                                                 DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), "0"));
    /* ExecutionResult ret = Ok; */
    auto varDeclReturn =
        VariableDeclaration::create(NES::QueryCompilation::GeneratableTypesFactory::createAnonymusDataType("ExecutionResult"),
                                    "ret",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "ExecutionResult::Ok"));
    /* int32_t sum = 0;*/
    auto varDeclSum = VariableDeclaration::create(tf.createDataType(DataTypeFactory::createInt64()),
                                                  "sum",
                                                  DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"));

    /* init statements before for loop */

    /*  tuples = (Tuple *)tuple_buffer.getBuffer();*/
    BinaryOperatorStatement initTuplePtr(
        VarRef(varDeclTuple)
            .assign(TypeCast(VarRefStatement(varDeclTupleBuffers).accessRef(getBufferOfTupleBuffer),
                             NES::QueryCompilation::GeneratableTypesFactory::createPointer(
                                 NES::QueryCompilation::GeneratableTypesFactory::createUserDefinedType(structDeclTuple)))));

    /* result_tuples = (ResultTuple *)output_tuple_buffer->data;*/
    auto resultTupleBufferDeclaration = VariableDeclaration::create(tupleBufferType, "resultTupleBuffer");
    BinaryOperatorStatement initResultTupleBufferPtr(
        VarDeclStatement(resultTupleBufferDeclaration).assign(VarRef(varDeclWorkerContext).accessRef(allocateTupleBuffer)));

    BinaryOperatorStatement initResultTuplePtr(
        VarRef(varDeclResultTuple)
            .assign(TypeCast(VarRef(resultTupleBufferDeclaration).accessRef(getBufferOfTupleBuffer),
                             NES::QueryCompilation::GeneratableTypesFactory::createPointer(
                                 NES::QueryCompilation::GeneratableTypesFactory::createUserDefinedType(structDeclResultTuple)))));

    /* for (uint64_t id = 0; id < tuple_buffer_1->num_tuples; ++id) */
    FOR loopStmt(varDeclId.copy(),
                 (VarRef(varDeclId) < (VarRef(varDeclTupleBuffers).accessRef(getNumberOfTupleBuffer))).copy(),
                 (++VarRef(varDeclId)).copy());

    /* sum = sum + tuples[id].campaign_id; */
    loopStmt.addStatement(
        VarRef(varDeclSum)
            .assign(VarRef(varDeclSum) + VarRef(varDeclTuple)[VarRef(varDeclId)].accessRef(VarRef(declFieldCampaignId)))
            .copy());

    /* function signature:
     * typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*);
     */
    auto emitTupleBuffer = FunctionCallStatement("emitBuffer");
    emitTupleBuffer.addParameter(VarRef(resultTupleBufferDeclaration));
    emitTupleBuffer.addParameter(VarRef(varDeclWorkerContext));
    auto mainFunction =
        FunctionDefinition::create("execute")
            //                            ->returns(tf.createDataType(DataTypeFactory::createUInt32()))
            ->returns(NES::QueryCompilation::GeneratableTypesFactory::createAnonymusDataType("ExecutionResult"))
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
                VarRef(varDeclResultTuple)[Constant(NES::QueryCompilation::GeneratableTypesFactory::createValueType(
                                               DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0")))]
                    .accessRef(VarRef(varDeclFieldResultTupleSum))
                    .assign(VarRef(varDeclSum))
                    .copy())
            ->addStatement(VarRef(varDeclPipelineExecutionContext).accessRef(emitTupleBuffer).copy())
            /* return ret; */

            ->addStatement(ReturnStatement::create(VarRefStatement(varDeclReturn).createCopy()));
    auto fileB = FileBuilder::create("query.cpp");

    fileB.addDeclaration(structDeclTuple.copy());
    fileB.addDeclaration(structDeclResultTuple.copy());

    auto executablePipeline = ClassDefinition::create("ExecutablePipelineStage1");
    executablePipeline->addBaseClass("Runtime::Execution::ExecutablePipelineStage");
    executablePipeline->addMethod(ClassDefinition::Public, mainFunction);

    auto executablePipelineDeclaration = executablePipeline->getDeclaration();
    auto pipelineNamespace = NamespaceDefinition::create("NES");
    pipelineNamespace->addDeclaration(executablePipelineDeclaration);

    auto createFunction = FunctionDefinition::create("create");
    auto returnStatement = ReturnStatement::create(SharedPointerGen::makeShared(executablePipelineDeclaration->getType()));
    createFunction->addStatement(returnStatement);
    ;
    createFunction->returns(SharedPointerGen::createSharedPtrType(
        NES::QueryCompilation::GeneratableTypesFactory::createAnonymusDataType("Runtime::Execution::ExecutablePipelineStage")));
    pipelineNamespace->addDeclaration(createFunction->getDeclaration());
    CodeFile file = fileB.addDeclaration(pipelineNamespace->getDeclaration()).build();

    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    auto sourceCode = std::make_unique<Compiler::SourceCode>("cpp", file.code);
    auto request = Compiler::CompilationRequest::create(std::move(sourceCode), "query", false, false, false, true);
    auto result = jitCompiler->compile(std::move(request));
    auto compiledCode = result.get().getDynamicObject();
    auto stage = CompiledExecutablePipelineStage::create(compiledCode, Unary);

    /* setup input and output for test */
    auto inputBuffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto recordSchema = Schema::create()->addField("id", DataTypeFactory::createInt64());

    auto layout = Runtime::MemoryLayouts::RowLayout::create(recordSchema, nodeEngine->getBufferManager()->getBufferSize());
    auto recordIndexFieldsInput = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, layout, inputBuffer);

    for (uint32_t recordIndex = 0; recordIndex < 100; ++recordIndex) {
        recordIndexFieldsInput[recordIndex] = recordIndex;
    }

    inputBuffer.setNumberOfTuples(100);

    /* execute code */
    auto wctx = Runtime::WorkerContext{0, nodeEngine->getBufferManager(), 64};
    auto context = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                  std::vector<Runtime::Execution::OperatorHandlerPtr>());
    stage->setup(*context);
    stage->start(*context);
    ASSERT_EQ(stage->execute(inputBuffer, *context.get(), wctx), ExecutionResult::Ok);
    auto outputBuffer = context->buffers[0];
    NES_INFO(Util::prettyPrintTupleBuffer(outputBuffer, recordSchema));

    /* check result for correctness */
    auto sumGeneratedCode =
        Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, layout, outputBuffer)[0];
    auto sum = 0;
    for (uint64_t recordIndex = 0; recordIndex < 100; ++recordIndex) {
        sum += recordIndexFieldsInput[recordIndex];
    }

    EXPECT_EQ(sum, sumGeneratedCode);
}

}// namespace NES
#include <API/Schema.hpp>
#include <API/Types/DataTypes.hpp>
#include <API/UserAPIExpression.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CCodeGenerator/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/IFStatement.hpp>
#include <QueryCompiler/CCodeGenerator/ReturnStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/VarDeclStatement.hpp>
#include <QueryCompiler/CCodeGenerator/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>
#include <QueryCompiler/DataTypes/AnonymousUserDefinedDataType.hpp>
#include <QueryCompiler/DataTypes/UserDefinedDataType.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <SourceSink/DefaultSource.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windows/WindowHandler.hpp>
#include <cassert>
#include <cmath>
#include <gtest/gtest.h>
#include <iostream>
#include <utility>

namespace NES {

class CodeGenerationTest : public testing::Test {
  public:

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup CodeGenerationTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("BufferManagerTest.log", NES::LOG_DEBUG);
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

class TestPipelineExecutionContext : public PipelineExecutionContext{
  public:
    TestPipelineExecutionContext(BufferManagerPtr bufferManager):
        PipelineExecutionContext(std::move(bufferManager), [this](TupleBuffer& buffer) { this->buffers.emplace_back(std::move(buffer));}){};

    std::vector<TupleBuffer> buffers;
};

const DataSourcePtr createTestSourceCodeGen(BufferManagerPtr bPtr, QueryManagerPtr dPtr) {
    return std::make_shared<DefaultSource>(
        Schema::create()->addField(createField("campaign_id", UINT64)), bPtr, dPtr, 1, 1);
}

class SelectionDataGenSource : public GeneratorSource {
  public:
    SelectionDataGenSource(SchemaPtr schema,
                           BufferManagerPtr bPtr, QueryManagerPtr dPtr,
                           const uint64_t pNum_buffers_to_process)
        :
        GeneratorSource(schema, bPtr, dPtr, pNum_buffers_to_process) {
    }

    ~SelectionDataGenSource() = default;

    std::optional<TupleBuffer> receiveData() override {
        // 10 tuples of size one
        TupleBuffer buf = bufferManager->getBufferBlocking();
        uint64_t tupleCnt = buf.getBufferSize()/sizeof(InputTuple);

        assert(buf.getBuffer() != NULL);

        InputTuple* tuples = (InputTuple*) buf.getBuffer();
        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].id = i;
            tuples[i].value = i*2;
            for (int j = 0; j < 11; ++j) {
                tuples[i].text[j] = ((j + i)%(255 - 'a')) + 'a';
            }
            tuples[i].text[12] = '\0';
        }

        //buf.setBufferSizeInBytes(sizeof(InputTuple));
        buf.setNumberOfTuples(tupleCnt);
        return buf;
    }

    struct __attribute__((packed)) InputTuple {
        uint32_t id;
        uint32_t value;
        char text[12];
    };
};

const DataSourcePtr createTestSourceCodeGenFilter(BufferManagerPtr bPtr, QueryManagerPtr dPtr) {
    DataSourcePtr source(
        std::make_shared<SelectionDataGenSource>(
            Schema::create()
                ->addField("id", BasicType::UINT32)
                ->addField("value", BasicType::UINT32)
                ->addField("text", createArrayDataType(BasicType::CHAR, 12)),
            bPtr, dPtr,
            1));

    return source;
}

class PredicateTestingDataGeneratorSource : public GeneratorSource {
  public:
    PredicateTestingDataGeneratorSource(SchemaPtr schema, BufferManagerPtr bPtr, QueryManagerPtr dPtr,
                                        const uint64_t pNum_buffers_to_process)
        :
        GeneratorSource(schema, bPtr, dPtr, pNum_buffers_to_process) {
    }

    ~PredicateTestingDataGeneratorSource() = default;

    struct __attribute__((packed)) InputTuple {
        uint32_t id;
        int16_t valueSmall;
        float valueFloat;
        double valueDouble;
        char singleChar;
        char text[12];
    };

    std::optional<TupleBuffer> receiveData() override {
        // 10 tuples of size one
        TupleBuffer buf = bufferManager->getBufferBlocking();
        uint64_t tupleCnt = buf.getBufferSize()/sizeof(InputTuple);

        assert(buf.getBuffer() != NULL);

        InputTuple* tuples = (InputTuple*) buf.getBuffer();

        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].id = i;
            tuples[i].valueSmall = -123 + (i*2);
            tuples[i].valueFloat = i*M_PI;
            tuples[i].valueDouble = i*M_PI*2;
            tuples[i].singleChar = ((i + 1)%(127 - 'A')) + 'A';
            for (int j = 0; j < 11; ++j) {
                tuples[i].text[j] = ((i + 1)%64) + 64;
            }
            tuples[i].text[12] = '\0';
        }

        //buf->setBufferSizeInBytes(sizeof(InputTuple));
        buf.setNumberOfTuples(tupleCnt);
        buf.setTupleSizeInBytes(sizeof(InputTuple));
        return buf;
    }
};

const DataSourcePtr createTestSourceCodeGenPredicate(BufferManagerPtr bPtr, QueryManagerPtr dPtr) {
    DataSourcePtr source(
        std::make_shared<PredicateTestingDataGeneratorSource>(
            Schema::create()
                ->addField("id", BasicType::UINT32)
                ->addField("valueSmall", BasicType::INT16)
                ->addField("valueFloat", BasicType::FLOAT32)
                ->addField("valueDouble", BasicType::FLOAT64)
                ->addField("valueChar", BasicType::CHAR)
                ->addField("text", createArrayDataType(BasicType::CHAR, 12)),
            bPtr, dPtr,
            1));

    return source;
}

class WindowTestingDataGeneratorSource : public GeneratorSource {
  public:
    WindowTestingDataGeneratorSource(SchemaPtr schema, BufferManagerPtr bPtr, QueryManagerPtr dPtr,
                                     const uint64_t pNum_buffers_to_process)
        :
        GeneratorSource(schema, bPtr, dPtr, pNum_buffers_to_process) {
    }

    ~WindowTestingDataGeneratorSource() = default;

    struct __attribute__((packed)) InputTuple {
        uint64_t key;
        uint64_t value;
    };

    std::optional<TupleBuffer> receiveData() override {
        // 10 tuples of size one
        TupleBuffer buf = bufferManager->getBufferBlocking();
        uint64_t tupleCnt = 10;

        assert(buf.getBuffer() != NULL);

        InputTuple* tuples = (InputTuple*) buf.getBuffer();

        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].key = i%2;
            tuples[i].value = 1;
        }

        //bufsetBufferSizeInBytes(sizeof(InputTuple));
        buf.setNumberOfTuples(tupleCnt);
        return buf;
    }
};

const DataSourcePtr createWindowTestDataSource(BufferManagerPtr bPtr, QueryManagerPtr dPtr) {
    DataSourcePtr source(
        std::make_shared<WindowTestingDataGeneratorSource>(
            Schema::create()
                ->addField("key", BasicType::UINT64)
                ->addField("value", BasicType::UINT64),
            bPtr, dPtr,
            10));
    return source;
}

/**
 * @brief This test checks the behavior of the code generation API
 */
TEST_F(CodeGenerationTest, codeGenerationApiTest) {

    auto varDeclI = VariableDeclaration::create(
        createDataType(BasicType(INT32)), "i",
        createBasicTypeValue(BasicType(INT32), "0"));
    auto varDeclJ = VariableDeclaration::create(
        createDataType(BasicType(INT32)), "j",
        createBasicTypeValue(BasicType(INT32), "5"));
    auto varDeclK = VariableDeclaration::create(
        createDataType(BasicType(INT32)), "k",
        createBasicTypeValue(BasicType(INT32), "7"));
    auto varDeclL = VariableDeclaration::create(
        createDataType(BasicType(INT32)), "l",
        createBasicTypeValue(BasicType(INT32), "2"));

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
            createArrayDataType(BasicType(CHAR), 12), "m",
            createArrayValueType(BasicType(CHAR), vals));
        // declaration of m
        EXPECT_EQ(VarRefStatement(varDeclM).getCode()->code_, "m");

        // Char Array initialization
        auto varDeclN = VariableDeclaration::create(
            createArrayDataType(BasicType(CHAR), 12), "n",
            createArrayValueType(BasicType(CHAR), vals));
        EXPECT_EQ(varDeclN.getCode(), "char n[12] = {'a', 'b', 'c'}");

        // Int Array initialization
        auto varDeclO = VariableDeclaration::create(
            createArrayDataType(BasicType(UINT8), 4), "o",
            createArrayValueType(BasicType(UINT8), {"2", "3", "4"}));
        EXPECT_EQ(varDeclO.getCode(), "uint8_t o[4] = {2, 3, 4}");

        // String Array initialization
        auto stringValueType = createStringValueType(
            "DiesIstEinZweiterTest\0dwqdwq")->getCodeExpression()->code_;
        EXPECT_EQ(stringValueType, "\"DiesIstEinZweiterTest\"");

        auto charValueType = createBasicTypeValue(BasicType::CHAR,
                                                  "DiesIstEinDritterTest")
            ->getCodeExpression()->code_;
        EXPECT_EQ(charValueType, "DiesIstEinDritterTest");
    }

    {
        auto code = BinaryOperatorStatement(VarRefStatement(varDeclI), PLUS_OP,
                                            VarRefStatement(varDeclJ)).addRight(
            PLUS_OP, VarRefStatement(varDeclK)).addRight(MULTIPLY_OP,
                                                         VarRefStatement(varDeclI),
                                                         BRACKETS).addRight(
                            GREATER_THAN_OP, VarRefStatement(varDeclL)).getCode();

        EXPECT_EQ(code->code_, "(i+j+k*i)>l");

        // We have two ways to generate code for arithmetical operations, we check here if they result in the same code
        auto plusOperatorCode = BinaryOperatorStatement(VarRefStatement(varDeclI),
                                                        PLUS_OP,
                                                        VarRefStatement(varDeclJ))
            .getCode()->code_;
        auto plusOperatorCodeOp = (VarRefStatement(varDeclI)
            + VarRefStatement(varDeclJ)).getCode()->code_;
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
                    << ConstantExpressionStatement(createBasicTypeValue(INT32, "0"))))[VarRefStatement(
                varDeclJ)];
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
            assign(VarRef(varDeclI), VarRef(varDeclI)*VarRef(varDeclK)));
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
            createDataType(BasicType(INT32)), "num_tuples",
            createBasicTypeValue(BasicType(INT32), "0"));

        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   ADDRESS_OF_OP).getCode()->code_,
            "&num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   DEREFERENCE_POINTER_OP).getCode()->code_,
            "*num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   PREFIX_INCREMENT_OP).getCode()->code_,
            "++num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   PREFIX_DECREMENT_OP).getCode()->code_,
            "--num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   POSTFIX_INCREMENT_OP).getCode()->code_,
            "num_tuples++");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   POSTFIX_DECREMENT_OP).getCode()->code_,
            "num_tuples--");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   BITWISE_COMPLEMENT_OP).getCode()->code_,
            "~num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   LOGICAL_NOT_OP).getCode()->code_,
            "!num_tuples");
        EXPECT_EQ(
            UnaryOperatorStatement(VarRefStatement(variableDeclaration),
                                   SIZE_OF_TYPE_OP).getCode()->code_,
            "sizeof(num_tuples)");
    }

    {
        // check code generation for loops
        auto varDeclQ = VariableDeclaration::create(
            createDataType(BasicType(INT32)), "q",
            createBasicTypeValue(BasicType(INT32), "0"));
        auto varDeclNumTuple = VariableDeclaration::create(
            createDataType(BasicType(INT32)), "num_tuples",
            createBasicTypeValue(BasicType(INT32), "0"));

        auto varDeclSum = VariableDeclaration::create(
            createDataType(BasicType(INT32)), "sum",
            createBasicTypeValue(BasicType(INT32), "0"));

        ForLoopStatement loopStmt(
            varDeclQ.copy(),
            BinaryOperatorStatement(VarRefStatement(varDeclQ), LESS_THAN_OP,
                                    VarRefStatement(varDeclNumTuple)).copy(),
            UnaryOperatorStatement(VarRefStatement(varDeclQ), PREFIX_INCREMENT_OP).copy());

        loopStmt.addStatement(
            BinaryOperatorStatement(
                VarRefStatement(varDeclSum),
                ASSIGNMENT_OP,
                BinaryOperatorStatement(VarRefStatement(varDeclSum), PLUS_OP,
                                        VarRefStatement(varDeclQ))).copy());

        EXPECT_EQ(loopStmt.getCode()->code_,
                  "for(int32_t q = 0;q<num_tuples;++q){\nsum=sum+q;\n\n}\n");

        auto forLoop = ForLoopStatement(
            varDeclQ.copy(),
            BinaryOperatorStatement(VarRefStatement(varDeclQ), LESS_THAN_OP,
                                    VarRefStatement(varDeclNumTuple)).copy(),
            UnaryOperatorStatement(VarRefStatement(varDeclQ), PREFIX_INCREMENT_OP).copy()
        );

        EXPECT_EQ(forLoop.getCode()->code_,
                  "for(int32_t q = 0;q<num_tuples;++q){\n\n}\n");

        auto compareAssignment = BinaryOperatorStatement(
            VarRefStatement(varDeclK),
            ASSIGNMENT_OP,
            BinaryOperatorStatement(VarRefStatement(varDeclJ), GREATER_THAN_OP,
                                    ConstantExpressionStatement(INT32, "5")));

        EXPECT_EQ(compareAssignment.getCode()->code_, "k=j>5");
    }

    {
        /* check code generation of pointers */
        auto val = createPointerDataType(BasicType(INT32));
        assert(val != nullptr);
        auto variableDeclarationI = VariableDeclaration::create(
            createDataType(BasicType(INT32)), "i",
            createBasicTypeValue(BasicType(INT32), "0"));
        auto variableDeclarationP = VariableDeclaration::create(val, "array");
        EXPECT_EQ(variableDeclarationP.getCode(), "int32_t* array");

        /* new String Type */
        auto charPointerDataType = createPointerDataType(BasicType(CHAR));
        auto var_decl_temp = VariableDeclaration::create(
            charPointerDataType, "i", createStringValueType("Hello World"));
        EXPECT_EQ(var_decl_temp.getCode(), "char* i = \"Hello World\"");

        auto tupleBufferStructDecl = StructDeclaration::create("TupleBuffer",
                                                               "buffer").addField(
            VariableDeclaration::create(
                createDataType(BasicType(UINT64)), "num_tuples",
                createBasicTypeValue(BasicType(UINT64), "0"))).addField(
            variableDeclarationP);

        // check code generation for different assignment type
        auto varDeclTupleBuffer = VariableDeclaration::create(
            createUserDefinedType(tupleBufferStructDecl), "buffer");
        EXPECT_EQ(varDeclTupleBuffer.getCode(), "TupleBuffer");

        auto varDeclTupleBufferPointer = VariableDeclaration::create(
            createPointerDataType(createUserDefinedType(tupleBufferStructDecl)),
            "buffer");
        EXPECT_EQ(varDeclTupleBufferPointer.getCode(), "TupleBuffer* buffer");

        auto pointerDataType = createPointerDataType(
            createUserDefinedType(tupleBufferStructDecl));
        EXPECT_EQ(pointerDataType->getCode()->code_, "TupleBuffer*");

        auto typeDefinition = VariableDeclaration::create(
            createPointerDataType(createUserDefinedType(tupleBufferStructDecl)),
            "buffer").getTypeDefinitionCode();
        EXPECT_EQ(
            typeDefinition,
            "struct TupleBuffer{\nuint64_t num_tuples = 0;\nint32_t* array;\n}buffer");

    }
}

/**
 * @brief Simple test that generates code to process a input buffer and calculate a running sum.
 */
TEST_F(CodeGenerationTest, codeGenRunningSum) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    auto tupleBufferType = createAnonymUserDefinedType("NES::TupleBuffer");
    auto pipelineExecutionContextType = createAnonymUserDefinedType("NES::PipelineExecutionContext");
    auto getNumberOfTupleBuffer = FunctionCallStatement("getNumberOfTuples");
    auto allocateTupleBuffer = FunctionCallStatement("allocateTupleBuffer");

    auto getBufferOfTupleBuffer = FunctionCallStatement("getBuffer");

    /* struct definition for input tuples */
    auto structDeclTuple = StructDeclaration::create("Tuple", "").addField(
        VariableDeclaration::create(createDataType(BasicType(INT64)),
                                    "campaign_id"));

    /* struct definition for result tuples */

    auto structDeclResultTuple = StructDeclaration::create("ResultTuple", "")
        .addField(
            VariableDeclaration::create(createDataType(BasicType(INT64)), "sum"));

    /* === declarations === */
    auto varDeclTupleBuffers = VariableDeclaration::create(createReferenceDataType(tupleBufferType),
                                                           "input_buffer");
    auto varDeclPipelineExecutionContext =
        VariableDeclaration::create(createReferenceDataType(pipelineExecutionContextType),
                                    "pipelineExecutionContext");
    auto varDeclWindow = VariableDeclaration::create(
        createPointerDataType(createAnonymUserDefinedType("void")), "stateVar");
    VariableDeclaration varDeclWindowManager = VariableDeclaration::create(
        createPointerDataType(createAnonymUserDefinedType("NES::WindowManager")),
        "window_manager");

    /* Tuple *tuples; */
    auto varDeclTuple = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(structDeclTuple)), "tuples");

    auto varDeclResultTuple = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(structDeclResultTuple)),
        "resultTuples");

    /* variable declarations for fields inside structs */
    auto declFieldCampaignId = structDeclTuple.getVariableDeclaration(
        "campaign_id");

    auto varDeclFieldResultTupleSum = structDeclResultTuple.getVariableDeclaration("sum");

    /* === generating the query function === */

    /* variable declarations */

    /* uint64_t id = 0; */
    auto varDeclId = VariableDeclaration::create(
        createDataType(BasicType(UINT64)), "id",
        createBasicTypeValue(BasicType(INT32), "0"));
    /* int32_t ret = 0; */
    auto varDeclReturn = VariableDeclaration::create(
        createDataType(BasicType(INT32)), "ret",
        createBasicTypeValue(BasicType(INT32), "0"));
    /* int32_t sum = 0;*/
    auto varDeclSum = VariableDeclaration::create(
        createDataType(BasicType(INT64)), "sum",
        createBasicTypeValue(BasicType(INT64), "0"));

    /* init statements before for loop */

    /*  tuples = (Tuple *)tuple_buffer.getBuffer();*/
    BinaryOperatorStatement initTuplePtr(
        VarRef(varDeclTuple).assign(
            TypeCast(
                VarRefStatement(varDeclTupleBuffers).accessRef(getBufferOfTupleBuffer),
                createPointerDataType(createUserDefinedType(structDeclTuple)))));

    /* result_tuples = (ResultTuple *)output_tuple_buffer->data;*/
    auto resultTupleBufferDeclaration = VariableDeclaration::create(tupleBufferType, "resultTupleBuffer");
    BinaryOperatorStatement initResultTupleBufferPtr
        (VarDeclStatement(resultTupleBufferDeclaration).assign(VarRef(varDeclPipelineExecutionContext).accessRef(
            allocateTupleBuffer)));

    BinaryOperatorStatement initResultTuplePtr(
        VarRef(varDeclResultTuple).assign(
            TypeCast(
                VarRef(resultTupleBufferDeclaration).accessRef(getBufferOfTupleBuffer),
                createPointerDataType(
                    createUserDefinedType(structDeclResultTuple)))));

    /* for (uint64_t id = 0; id < tuple_buffer_1->num_tuples; ++id) */
    FOR loopStmt(
        varDeclId.copy(),
        (VarRef(varDeclId)
            < (VarRef(varDeclTupleBuffers).accessRef(getNumberOfTupleBuffer))).copy(),
        (++VarRef(varDeclId)).copy());

    /* sum = sum + tuples[id].campaign_id; */
    loopStmt.addStatement(
        VarRef(varDeclSum).assign(
            VarRef(varDeclSum)
                + VarRef(varDeclTuple)[VarRef(varDeclId)].accessRef(
                    VarRef(declFieldCampaignId))).copy());



    /* function signature:
     * typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*);
     */
    auto emitTupleBuffer = FunctionCallStatement("emitBuffer");
    emitTupleBuffer.addParameter(VarRef(resultTupleBufferDeclaration));
    auto mainFunction = FunctionBuilder::create("compiled_query").returns(
            createDataType(BasicType(UINT32)))
        .addParameter(varDeclTupleBuffers)
        .addParameter(varDeclWindow)
        .addParameter(varDeclWindowManager)
        .addParameter(varDeclPipelineExecutionContext)
        .addVariableDeclaration(varDeclReturn)
        .addVariableDeclaration(varDeclTuple)
        .addVariableDeclaration(varDeclResultTuple)
        .addVariableDeclaration(varDeclSum)
        .addStatement(initTuplePtr.copy())
        .addStatement(initResultTupleBufferPtr.copy())
        .addStatement(initResultTuplePtr.copy()).addStatement(StatementPtr(new ForLoopStatement(loopStmt)))
        .addStatement(
            /*   result_tuples[0].sum = sum; */
            VarRef(varDeclResultTuple)[Constant(INT32, "0")].accessRef(
                    VarRef(varDeclFieldResultTupleSum)).assign(VarRef(varDeclSum))
                .copy())
                .addStatement(VarRef(varDeclPipelineExecutionContext).accessRef(emitTupleBuffer).copy())
            /* return ret; */

        .addStatement(
            StatementPtr(new ReturnStatement(VarRefStatement(varDeclReturn))))
        .build();

    auto file = FileBuilder::create("query.cpp")
        .addDeclaration(structDeclTuple)
        .addDeclaration(structDeclResultTuple)
        .addDeclaration(mainFunction)
        .build();

    Compiler compiler;
    auto stage = CompiledExecutablePipeline::create(compiler.compile(file.code, true /** debugging **/));

    /* setup input and output for test */
    auto inputBuffer = nodeEngine->getBufferManager()->getBufferBlocking();
    inputBuffer.setTupleSizeInBytes(8);
    auto recordSchema = Schema::create()->addField("id", BasicType::INT64);
    auto layout = createRowLayout(recordSchema);

    for (uint32_t recordIndex = 0; recordIndex < 100; ++recordIndex) {
        layout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->write(inputBuffer, recordIndex);
    }
    inputBuffer.setNumberOfTuples(100);

    /* execute code */
    auto context = TestPipelineExecutionContext(nodeEngine->getBufferManager());
    if (!stage->execute(inputBuffer, nullptr, nullptr, context)) {
        std::cout << "Error!" << std::endl;
    }
    auto outputBuffer = context.buffers[0];
    NES_INFO(UtilityFunctions::prettyPrintTupleBuffer(outputBuffer, recordSchema));

    /* check result for correctness */
    auto sumGeneratedCode = layout->getValueField<int64_t>(/*recordIndex*/ 0, /*fieldIndex*/ 0)->read(outputBuffer);
    auto sum = 0;
    for (uint64_t recordIndex = 0; recordIndex < 100; ++recordIndex) {
        sum += layout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->read(inputBuffer);
    }
    EXPECT_EQ(sum, sumGeneratedCode);
}

/**
 * @brief This test generates a simple copy function, which copies code from one buffer to another
 */
TEST_F(CodeGenerationTest, codeGenerationCopy) {
    /* prepare objects for test */
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    auto source = createTestSourceCodeGen(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();

    NES_INFO("Generate Code");
    /* generate code for scanning input buffer */
    codeGenerator->generateCode(source->getSchema(), context, std::cout);
    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(createPrintSinkWithSchema(Schema::create()->addField("campaign_id", UINT64), std::cout)->getSchema(),
                                       context, std::cout);
    /* compile code to pipeline stage */
    Compiler compiler;
    auto stage = codeGenerator->compile(CompilerArgs(), context->code);

    /* prepare input and output tuple buffer */
    auto schema = Schema::create()->addField("i64", UINT64);
    auto buffer = source->receiveData().value();

    /* execute Stage */
    NES_INFO("Processing " << buffer.getNumberOfTuples() << " tuples: ");
    auto queryContext = TestPipelineExecutionContext(nodeEngine->getBufferManager());
    stage->execute(buffer, nullptr, nullptr, queryContext);
    auto resultBuffer = queryContext.buffers[0];
    /* check for correctness, input source produces uint64_t tuples and stores a 1 in each tuple */
    EXPECT_EQ(buffer.getNumberOfTuples(), resultBuffer.getNumberOfTuples());
    auto layout = createRowLayout(schema);
    for (uint64_t recordIndex = 0; recordIndex < buffer.getNumberOfTuples();
         ++recordIndex) {
        EXPECT_EQ(1, layout->getValueField<uint64_t>(recordIndex, /*fieldIndex*/0)->read(buffer));
    }
}
/**
 * @brief This test generates a predicate, which filters elements in the input buffer
 */
TEST_F(CodeGenerationTest, codeGenerationFilterPredicate) {
    /* prepare objects for test */
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    auto source = createTestSourceCodeGenFilter(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();

    auto inputSchema = source->getSchema();

    /* generate code for scanning input buffer */
    codeGenerator->generateCode(source->getSchema(), context, std::cout);

    auto pred = std::dynamic_pointer_cast<Predicate>(
        (PredicateItem(inputSchema->get(0))
            < PredicateItem(createBasicTypeValue(BasicType::INT64, "5"))).copy());

    codeGenerator->generateCode(pred, context, std::cout);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(
        createPrintSinkWithSchema(source->getSchema(), std::cout)->getSchema(), context,
        std::cout);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(CompilerArgs(), context->code);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();;
    NES_INFO("Processing " << inputBuffer.getNumberOfTuples() << " tuples: ");

    auto sizeOfTuple = (sizeof(uint32_t) + sizeof(uint32_t) + sizeof(char) * 12);

    /* execute Stage */
    auto queryContext = TestPipelineExecutionContext(nodeEngine->getBufferManager());
    stage->execute(inputBuffer, nullptr, NULL, queryContext);
    auto resultBuffer = queryContext.buffers[0];
    /* check for correctness, input source produces tuples consisting of two uint32_t values, 5 values will match the predicate */
    NES_INFO(
        "Number of generated output tuples: " << resultBuffer.getNumberOfTuples());
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5u);

    auto resultData = (SelectionDataGenSource::InputTuple*) resultBuffer.getBuffer();
    for (uint64_t i = 0; i < 5; ++i) {
        EXPECT_EQ(resultData[i].id, i);
        EXPECT_EQ(resultData[i].value, i * 2);
    }
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(CodeGenerationTest, codeGenerationWindowAssigner) {
    /* prepare objects for test */
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();

    auto input_schema = source->getSchema();

    codeGenerator->generateCode(source->getSchema(), context, std::cout);

    auto sum = Sum::on(Field(input_schema->get("value")));
    auto windowDefinition = createWindowDefinition(
        input_schema->get("key"), sum,
        TumblingWindow::of(TimeCharacteristic::ProcessingTime, Seconds(10)));

    codeGenerator->generateCode(windowDefinition, context, std::cout);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(CompilerArgs(), context->code);

    // init window handler
    auto windowHandler =
        new WindowHandler(windowDefinition, nodeEngine->getQueryManager(), nodeEngine->getBufferManager());
    windowHandler->setup(nullptr, 0);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    auto queryContext = TestPipelineExecutionContext(nodeEngine->getBufferManager());
    stage->execute(inputBuffer, windowHandler->getWindowState(),
                   windowHandler->getWindowManager(), queryContext);
    auto resultBuffer = queryContext.buffers[0];

    /* check for correctness, after a window assigner no tuple should be produced*/
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 0);

    //check partial aggregates in window state
    auto stateVar =
        (StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>*) windowHandler
            ->getWindowState();
    EXPECT_EQ(stateVar->get(0).value()->getPartialAggregates()[0], 5);
    EXPECT_EQ(stateVar->get(1).value()->getPartialAggregates()[0], 5);
}

/**
 * @brief This test generates a predicate with string comparision
 */
TEST_F(CodeGenerationTest, codeGenerationStringComparePredicateTest) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    /* prepare objects for test */
    auto source = createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();

    auto inputSchema = source->getSchema();
    codeGenerator->generateCode(inputSchema, context, std::cout);

    //predicate definition
    codeGenerator->generateCode(
        createPredicate(
            (inputSchema->get(2) > 30.4)
                && (inputSchema->get(4) == 'F' || (inputSchema->get(5) == "HHHHHHHHHHH"))),
        context, std::cout);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(createPrintSinkWithSchema(inputSchema, std::cout)->getSchema(),
                                       context, std::cout);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(CompilerArgs(), context->code);

    /* prepare input tuple buffer */
    auto optVal = source->receiveData();
    assert(!!optVal);
    auto inputBuffer = *optVal;

    /* execute Stage */

    auto queryContext = TestPipelineExecutionContext(nodeEngine->getBufferManager());
    stage->execute(inputBuffer, nullptr, nullptr, queryContext);

    auto resultBuffer = queryContext.buffers[0];

    /* check for correctness, input source produces tuples consisting of two uint32_t values, 3 values will match the predicate */
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 3);

    NES_INFO(UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, inputSchema));
}

/**
 * @brief This test generates a map predicate, which manipulates the input buffer content
 */
TEST_F(CodeGenerationTest, codeGenerationMapPredicateTest) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    /* prepare objects for test */
    auto source = createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();

    auto inputSchema = source->getSchema();

    codeGenerator->generateCode(inputSchema, context, std::cout);

    //predicate definition
    auto mappedValue = AttributeField("mappedValue", BasicType::FLOAT64).copy();
    codeGenerator->generateCode(
        mappedValue, createPredicate((inputSchema->get(2)*inputSchema->get(3)) + 2),
        context, std::cout);

    /* generate code for writing result tuples to output buffer */
    auto outputSchema = Schema::create()
        ->addField("id", BasicType::UINT32)
        ->addField("valueSmall", BasicType::INT16)
        ->addField("valueFloat", BasicType::FLOAT32)
        ->addField("valueDouble", BasicType::FLOAT64)
        ->addField(mappedValue)
        ->addField("valueChar", BasicType::CHAR)
        ->addField(
          "text", createArrayDataType(BasicType::CHAR, 12));

    auto schemaSize = outputSchema->getSchemaSizeInBytes();
    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(createPrintSinkWithSchema(outputSchema, std::cout)->getSchema(), context, std::cout);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(CompilerArgs(), context->code);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    auto queryContext = TestPipelineExecutionContext(nodeEngine->getBufferManager());
    stage->execute(inputBuffer, nullptr, nullptr, queryContext);

    auto resultBuffer = queryContext.buffers[0];

    auto inputLayout = createRowLayout(inputSchema);
    auto outputLayout = createRowLayout(outputSchema);
    for (size_t recordIndex = 0; recordIndex < resultBuffer.getNumberOfTuples() - 1; recordIndex++) {
        auto floatValue = inputLayout->getValueField<float>(recordIndex, /*fieldIndex*/ 2)->read(inputBuffer);
        auto doubleValue = inputLayout->getValueField<double>(recordIndex, /*fieldIndex*/ 3)->read(inputBuffer);
        auto reference = (floatValue * doubleValue) + 2;
        auto mapedValue = outputLayout->getValueField<double>(recordIndex, /*fieldIndex*/ 4)->read(resultBuffer);
        EXPECT_EQ(reference, mapedValue);
    }
}
}  // namespace NES

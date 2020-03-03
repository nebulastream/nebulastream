
#include <cassert>
#include <iostream>

#include <math.h>

#include "gtest/gtest.h"

#include <iostream>

#include <Catalogs/QueryCatalog.hpp>
#include <API/InputQuery.hpp>
#include <Services/CoordinatorService.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <NodeEngine/BufferManager.hpp>

#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/UnaryOperatorStatement.hpp>
#include <API/UserAPIExpression.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <Windows/WindowHandler.hpp>
#include <API/Types/DataTypes.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <SourceSink/DefaultSource.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <gtest/gtest.h>

namespace NES {

class CodeGenerationTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup QueryCatalogTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        setupLogging();
        std::cout << "Setup QueryCatalogTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down QueryCatalogTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down QueryCatalogTest test class." << std::endl;
    }
  protected:
    static void setupLogging() {
        // create PatternLayout
        log4cxx::LayoutPtr layoutPtr(
            new log4cxx::PatternLayout(
                "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

        // create FileAppender
        LOG4CXX_DECODE_CHAR(fileName, "QueryCatalogTest.log");
        log4cxx::FileAppenderPtr file(
            new log4cxx::FileAppender(layoutPtr, fileName));

        // create ConsoleAppender
        log4cxx::ConsoleAppenderPtr console(
            new log4cxx::ConsoleAppender(layoutPtr));

        // set log level
        NES::NESLogger->setLevel(log4cxx::Level::getDebug());
        //    logger->setLevel(log4cxx::Level::getInfo());

        // add appenders and other will inherit the settings
        NES::NESLogger->addAppender(file);
        NES::NESLogger->addAppender(console);
    }
};

const DataSourcePtr createTestSourceCodeGen() {
    return std::make_shared<DefaultSource>(Schema::create().addField(createField("campaign_id", UINT64)), 1, 1);
}

class SelectionDataGenSource : public GeneratorSource {
  public:
    SelectionDataGenSource(const Schema& schema, const uint64_t pNum_buffers_to_process) :
        GeneratorSource(schema, pNum_buffers_to_process) {
    }

    ~SelectionDataGenSource() = default;

    TupleBufferPtr receiveData() override {
        // 10 tuples of size one
        TupleBufferPtr buf = BufferManager::instance().getBuffer();
        uint64_t tupleCnt = buf->getBufferSizeInBytes()/sizeof(InputTuple);

        assert(buf->getBuffer() != NULL);

        InputTuple* tuples = (InputTuple*) buf->getBuffer();
        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].id = i;
            tuples[i].value = i*2;
            for (int j = 0; j < 11; ++j) {
                tuples[i].text[j] = ((j + i)%(255 - 'a')) + 'a';
            }
            tuples[i].text[12] = '\0';
        }

        buf->setBufferSizeInBytes(sizeof(InputTuple));
        buf->setNumberOfTuples(tupleCnt);
        return buf;
    }

    struct __attribute__((packed)) InputTuple {
        uint32_t id;
        uint32_t value;
        char text[12];
    };
};

const DataSourcePtr createTestSourceCodeGenFilter() {
    DataSourcePtr source(std::make_shared<SelectionDataGenSource>(
        Schema::create()
            .addField("id", BasicType::UINT32)
            .addField("value", BasicType::UINT32)
            .addField("text", createArrayDataType(BasicType::CHAR, 12)), 1));

    return source;
}

class PredicateTestingDataGeneratorSource : public GeneratorSource {
  public:
    PredicateTestingDataGeneratorSource(const Schema& schema, const uint64_t pNum_buffers_to_process) :
        GeneratorSource(schema, pNum_buffers_to_process) {
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

    TupleBufferPtr receiveData() override {
        // 10 tuples of size one
        TupleBufferPtr buf = BufferManager::instance().getBuffer();
        uint64_t tupleCnt = buf->getBufferSizeInBytes()/sizeof(InputTuple);

        assert(buf->getBuffer() != NULL);

        InputTuple* tuples = (InputTuple*) buf->getBuffer();

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

        buf->setBufferSizeInBytes(sizeof(InputTuple));
        buf->setNumberOfTuples(tupleCnt);
        return buf;
    }
};

const DataSourcePtr createTestSourceCodeGenPredicate() {
    DataSourcePtr source(std::make_shared<PredicateTestingDataGeneratorSource>(
        Schema::create()
            .addField("id", BasicType::UINT32)
            .addField("valueSmall", BasicType::INT16)
            .addField("valueFloat", BasicType::FLOAT32)
            .addField("valueDouble", BasicType::FLOAT64)
            .addField("valueChar", BasicType::CHAR)
            .addField("text", createArrayDataType(BasicType::CHAR, 12)), 1));

    return source;
}

class WindowTestingDataGeneratorSource : public GeneratorSource {
  public:
    WindowTestingDataGeneratorSource(const Schema& schema, const uint64_t pNum_buffers_to_process) :
        GeneratorSource(schema, pNum_buffers_to_process) {
    }

    ~WindowTestingDataGeneratorSource() = default;

    struct __attribute__((packed)) InputTuple {
        uint64_t key;
        uint64_t value;
    };

    TupleBufferPtr receiveData() override {
        // 10 tuples of size one
        TupleBufferPtr buf = BufferManager::instance().getBuffer();
        uint64_t tupleCnt = 10;

        assert(buf->getBuffer() != NULL);

        InputTuple* tuples = (InputTuple*) buf->getBuffer();

        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].key = i%2;
            tuples[i].value = 1;
        }

        buf->setBufferSizeInBytes(sizeof(InputTuple));
        buf->setNumberOfTuples(tupleCnt);
        return buf;
    }
};

const DataSourcePtr createWindowTestDataSource() {
    DataSourcePtr source(std::make_shared<WindowTestingDataGeneratorSource>(
        Schema::create()
            .addField("key", BasicType::UINT64)
            .addField("value", BasicType::UINT64), 10));
    return source;
}

/* TODO: make proper test suite out of these */
int CodeGenTestCases() {

    VariableDeclaration var_decl_i =
        VariableDeclaration::create(createDataType(BasicType(INT32)), "i", createBasicTypeValue(BasicType(INT32), "0"));
    VariableDeclaration var_decl_j =
        VariableDeclaration::create(createDataType(BasicType(INT32)), "j", createBasicTypeValue(BasicType(INT32), "5"));
    VariableDeclaration var_decl_k =
        VariableDeclaration::create(createDataType(BasicType(INT32)), "k", createBasicTypeValue(BasicType(INT32), "7"));
    VariableDeclaration var_decl_l =
        VariableDeclaration::create(createDataType(BasicType(INT32)), "l", createBasicTypeValue(BasicType(INT32), "2"));

    {
        BinaryOperatorStatement bin_op(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j));
        std::cout << bin_op.getCode()->code_ << std::endl;
        CodeExpressionPtr code = bin_op.addRight(PLUS_OP, VarRefStatement(var_decl_k)).getCode();

        std::cout << code->code_ << std::endl;
    }
    {
        std::cout << "=========================" << std::endl;

        std::vector<std::string> vals = {"a", "b", "c"};
        VariableDeclaration var_decl_m =
            VariableDeclaration::create(createArrayDataType(BasicType(CHAR), 12),
                                        "m",
                                        createArrayValueType(BasicType(CHAR), vals));
        std::cout << (VarRefStatement(var_decl_m).getCode()->code_) << std::endl;

        VariableDeclaration var_decl_n = VariableDeclaration::create(createArrayDataType(BasicType(CHAR), 12), "n",
                                                                     createArrayValueType(BasicType(CHAR), vals));
        std::cout << var_decl_n.getCode() << std::endl;

        VariableDeclaration var_decl_o = VariableDeclaration::create(createArrayDataType(BasicType(UINT8), 4), "o",
                                                                     createArrayValueType(BasicType(UINT8),
                                                                                          {"2", "3", "4"}));
        std::cout << var_decl_o.getCode() << std::endl;

        VariableDeclaration var_decl_p = VariableDeclaration::create(createArrayDataType(BasicType(CHAR), 20), "p",
                                                                     createStringValueType("diesisteinTest", 20));
        std::cout << var_decl_p.getCode() << std::endl;

        std::cout << createStringValueType("DiesIstEinZweiterTest\0dwqdwq")->getCodeExpression()->code_ << std::endl;

        std::cout << createBasicTypeValue(BasicType::CHAR, "DiesIstEinDritterTest")->getCodeExpression()->code_
                  << std::endl;

        std::cout << "=========================" << std::endl;
    }

    {
        CodeExpressionPtr code =
            BinaryOperatorStatement(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j))
                .addRight(PLUS_OP, VarRefStatement(var_decl_k))
                .addRight(MULTIPLY_OP, VarRefStatement(var_decl_i), BRACKETS)
                .addRight(GREATER_THEN_OP, VarRefStatement(var_decl_l))
                .getCode();

        std::cout << code->code_ << std::endl;

        std::cout << "=========================" << std::endl;

        std::cout << BinaryOperatorStatement(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j))
            .getCode()
            ->code_
                  << std::endl;
        std::cout << (VarRefStatement(var_decl_i) + VarRefStatement(var_decl_j)).getCode()->code_ << std::endl;

        std::cout << "=========================" << std::endl;

        std::cout << UnaryOperatorStatement(VarRefStatement(var_decl_i), POSTFIX_INCREMENT_OP).getCode()->code_
                  << std::endl;
        std::cout << (++VarRefStatement(var_decl_i)).getCode()->code_ << std::endl;
        std::cout << (VarRefStatement(var_decl_i) >= VarRefStatement(var_decl_j))[VarRefStatement(var_decl_j)]
            .getCode()
            ->code_
                  << std::endl;

        std::cout << ((~VarRefStatement(var_decl_i) >=
            VarRefStatement(var_decl_j)
                << ConstantExprStatement(createBasicTypeValue(INT32, "0"))))[VarRefStatement(var_decl_j)]
            .getCode()
            ->code_
                  << std::endl;

        std::cout << VarRefStatement(var_decl_i)
            .assign(VarRefStatement(var_decl_i) + VarRefStatement(var_decl_j))
            .getCode()
            ->code_
                  << std::endl;

        std::cout << (sizeOf(VarRefStatement(var_decl_i))).getCode()->code_ << std::endl;

        std::cout << assign(VarRef(var_decl_i), VarRef(var_decl_i)).getCode()->code_ << std::endl;

        std::cout << IF(VarRef(var_decl_i) < VarRef(var_decl_j),
                        assign(VarRef(var_decl_i), VarRef(var_decl_i)*VarRef(var_decl_k)))
            .getCode()
            ->code_
                  << std::endl;

        std::cout << "=========================" << std::endl;

        std::cout << IfStatement(BinaryOperatorStatement(VarRefStatement(var_decl_i), GREATER_THEN_OP,
                                                         VarRefStatement(var_decl_j)),
                                 ReturnStatement(VarRefStatement(var_decl_i)))
            .getCode()
            ->code_
                  << std::endl;

        std::cout << IfStatement(VarRefStatement(var_decl_j), VarRefStatement(var_decl_i)).getCode()->code_
                  << std::endl;
    }

    {
        std::cout << BinaryOperatorStatement(VarRefStatement(var_decl_k), ASSIGNMENT_OP,
                                             BinaryOperatorStatement(VarRefStatement(var_decl_j), GREATER_THEN_OP,
                                                                     VarRefStatement(var_decl_i)))
            .getCode()
            ->code_
                  << std::endl;
    }

    {
        VariableDeclaration var_decl_num_tup = VariableDeclaration::create(
            createDataType(BasicType(INT32)), "num_tuples", createBasicTypeValue(BasicType(INT32), "0"));

        std::cout << toString(ADDRESS_OF_OP) << ": "
                  << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), ADDRESS_OF_OP).getCode()->code_
                  << std::endl;
        std::cout << toString(DEREFERENCE_POINTER_OP) << ": "
                  << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), DEREFERENCE_POINTER_OP).getCode()->code_
                  << std::endl;
        std::cout << toString(PREFIX_INCREMENT_OP) << ": "
                  << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), PREFIX_INCREMENT_OP).getCode()->code_
                  << std::endl;
        std::cout << toString(PREFIX_DECREMENT_OP) << ": "
                  << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), PREFIX_DECREMENT_OP).getCode()->code_
                  << std::endl;
        std::cout << toString(POSTFIX_INCREMENT_OP) << ": "
                  << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), POSTFIX_INCREMENT_OP).getCode()->code_
                  << std::endl;
        std::cout << toString(POSTFIX_DECREMENT_OP) << ": "
                  << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), POSTFIX_DECREMENT_OP).getCode()->code_
                  << std::endl;
        std::cout << toString(BITWISE_COMPLEMENT_OP) << ": "
                  << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), BITWISE_COMPLEMENT_OP).getCode()->code_
                  << std::endl;
        std::cout << toString(LOGICAL_NOT_OP) << ": "
                  << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), LOGICAL_NOT_OP).getCode()->code_
                  << std::endl;
        std::cout << toString(SIZE_OF_TYPE_OP) << ": "
                  << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), SIZE_OF_TYPE_OP).getCode()->code_
                  << std::endl;
    }

    {

        VariableDeclaration var_decl_q = VariableDeclaration::create(createDataType(BasicType(INT32)), "q",
                                                                     createBasicTypeValue(BasicType(INT32), "0"));
        VariableDeclaration var_decl_num_tup = VariableDeclaration::create(
            createDataType(BasicType(INT32)), "num_tuples", createBasicTypeValue(BasicType(INT32), "0"));

        VariableDeclaration var_decl_sum = VariableDeclaration::create(createDataType(BasicType(INT32)), "sum",
                                                                       createBasicTypeValue(BasicType(INT32), "0"));

        ForLoopStatement loop_stmt(
            var_decl_q,
            BinaryOperatorStatement(VarRefStatement(var_decl_q), LESS_THEN_OP, VarRefStatement(var_decl_num_tup)),
            UnaryOperatorStatement(VarRefStatement(var_decl_q), PREFIX_INCREMENT_OP));

        loop_stmt.addStatement(BinaryOperatorStatement(VarRefStatement(var_decl_sum), ASSIGNMENT_OP,
                                                       BinaryOperatorStatement(VarRefStatement(var_decl_sum), PLUS_OP,
                                                                               VarRefStatement(var_decl_q)))
                                   .copy());

        std::cout << loop_stmt.getCode()->code_ << std::endl;

        std::cout << ForLoopStatement(var_decl_q,
                                      BinaryOperatorStatement(VarRefStatement(var_decl_q), LESS_THEN_OP,
                                                              VarRefStatement(var_decl_num_tup)),
                                      UnaryOperatorStatement(VarRefStatement(var_decl_q), PREFIX_INCREMENT_OP))
            .getCode()
            ->code_
                  << std::endl;

        std::cout << BinaryOperatorStatement(VarRefStatement(var_decl_k), ASSIGNMENT_OP,
                                             BinaryOperatorStatement(VarRefStatement(var_decl_j), GREATER_THEN_OP,
                                                                     ConstantExprStatement(INT32, "5")))
            .getCode()
            ->code_
                  << std::endl;
    }
    /* pointers */
    {

        DataTypePtr val = createPointerDataType(BasicType(INT32));
        assert(val != nullptr);
        VariableDeclaration var_decl_i = VariableDeclaration::create(createDataType(BasicType(INT32)), "i",
                                                                     createBasicTypeValue(BasicType(INT32), "0"));
        VariableDeclaration var_decl_p = VariableDeclaration::create(val, "array");

        /* new String Type */
        DataTypePtr charptr = createPointerDataType(BasicType(CHAR));
        VariableDeclaration
            var_decl_temp = VariableDeclaration::create(charptr, "i", createStringValueType("Hello World"));
        std::cout << var_decl_p.getCode() << std::endl;

        std::cout << var_decl_temp.getCode() << std::endl;

        StructDeclaration struct_decl =
            StructDeclaration::create("TupleBuffer", "buffer")
                .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "num_tuples",
                                                      createBasicTypeValue(BasicType(UINT64), "0")))
                .addField(var_decl_p);

        std::cout << VariableDeclaration::create(createUserDefinedType(struct_decl), "buffer").getCode() << std::endl;
        std::cout << VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl)), "buffer")
            .getCode()
                  << std::endl;
        std::cout << createPointerDataType(createUserDefinedType(struct_decl))->getCode()->code_ << std::endl;

        std::cout << VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl)), "buffer")
            .getTypeDefinitionCode()
                  << std::endl;
    }
    /* TODO: Write Test Cases for this code */

    /*
    std::cout << (VarRefStatement(var_decl_tuple))[ConstantExprStatement(INT32, "0")]
                     .getCode()
                     ->code_
              << std::endl;
    std::cout << BinaryOperatorStatement(VarRefStatement(var_decl_tuple), ARRAY_REFERENCE_OP,
    VarRefStatement(var_decl_i)) .getCode()
                     ->code_
              << std::endl;
    std::cout << BinaryOperatorStatement(BinaryOperatorStatement(VarRefStatement(var_decl_tuple), ARRAY_REFERENCE_OP,
                                                                 VarRefStatement(var_decl_i)),
                                         MEMBER_SELECT_REFERENCE_OP, VarRefStatement(decl_field_campaign_id))
                     .getCode()
                     ->code_
              << std::endl;
  */

    return 0;
}

/**
 * @brief Simple test that generates code to process a input buffer and calculate a running sum.
 */
TEST_F(CodeGenerationTest, codeGenRunningSum) {

    /* === struct type definitions === */
    /** define structure of TupleBuffer
      struct TupleBuffer {
        void *data;
        uint64_t buffer_size;
        uint64_t tuple_size_bytes;
        uint64_t num_tuples;
      };
    */
    StructDeclaration structDeclTupleBuffer =
        StructDeclaration::create("TupleBuffer", "")
            .addField(VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))), "data"))
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "buffer_size"))
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "tuple_size_bytes"))
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "num_tuples"));

    /**
       define the WindowState struct
     struct WindowState {
      void *windowState;
      };
    */
    StructDeclaration structDeclState =
        StructDeclaration::create("WindowState", "")
            .addField(VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))),
                                                  "windowState"));

    /* struct definition for input tuples */
    StructDeclaration structDeclTuple =
        StructDeclaration::create("Tuple", "")
            .addField(VariableDeclaration::create(createDataType(BasicType(INT64)), "campaign_id"));

    /* struct definition for result tuples */
    StructDeclaration structDeclResultTuple =
        StructDeclaration::create("ResultTuple", "")
            .addField(VariableDeclaration::create(createDataType(BasicType(INT64)), "sum"));

    /* === declarations === */
    VariableDeclaration varDeclTupleBuffers = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(structDeclTupleBuffer)),
        "input_buffer");
    VariableDeclaration varDeclTupleBufferOutput = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(structDeclTupleBuffer)), "output_tuple_buffer");
    VariableDeclaration varDeclWindow =
        VariableDeclaration::create(createPointerDataType(createAnnonymUserDefinedType("void")),
                                    "state_var");
    VariableDeclaration varDeclWindowManager =
        VariableDeclaration::create(createPointerDataType(createAnnonymUserDefinedType("NES::WindowManager")),
                                    "window_manager");

    /* Tuple *tuples; */
    VariableDeclaration varDeclTuple =
        VariableDeclaration::create(createPointerDataType(createUserDefinedType(structDeclTuple)), "tuples");

    VariableDeclaration varDeclResultTuple = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(structDeclResultTuple)), "result_tuples");

    /* variable declarations for fields inside structs */
    VariableDeclaration declFieldCampaignId = structDeclTuple.getVariableDeclaration("campaign_id");
    VariableDeclaration declFieldNumTuplesStructTupleBuffer =
        structDeclTupleBuffer.getVariableDeclaration("num_tuples");
    VariableDeclaration declFieldDataPtrStructTupleBuf = structDeclTupleBuffer.getVariableDeclaration("data");
    VariableDeclaration varDeclFieldResultTupleSum = structDeclResultTuple.getVariableDeclaration("sum");

    /* === generating the query function === */

    /* variable declarations */

    /* TupleBuffer *tuple_buffer_1; */
    VariableDeclaration varDeclTupleBuffer1 = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(structDeclTupleBuffer)), "tuple_buffer_1");
    /* uint64_t id = 0; */
    VariableDeclaration varDeclId = VariableDeclaration::create(createDataType(BasicType(UINT64)), "id",
                                                                createBasicTypeValue(BasicType(INT32), "0"));
    /* int32_t ret = 0; */
    VariableDeclaration varDeclReturn = VariableDeclaration::create(createDataType(BasicType(INT32)), "ret",
                                                                    createBasicTypeValue(BasicType(INT32), "0"));
    /* int32_t sum = 0;*/
    VariableDeclaration varDeclSum = VariableDeclaration::create(createDataType(BasicType(INT64)), "sum",
                                                                 createBasicTypeValue(BasicType(INT64), "0"));

    /* init statements before for loop */

    /* tuple_buffer_1 = window_buffer[0]; */
    BinaryOperatorStatement initTupleBufferPtr(
        VarRefStatement(varDeclTupleBuffer1)
            .assign(VarRefStatement(varDeclTupleBuffers)));
    /*  tuples = (Tuple *)tuple_buffer_1->data;*/
    BinaryOperatorStatement initTuplePtr(
        VarRef(varDeclTuple)
            .assign(TypeCast(
                VarRefStatement(varDeclTupleBuffer1).accessPtr(VarRef(declFieldDataPtrStructTupleBuf)),
                createPointerDataType(createUserDefinedType(structDeclTuple)))));

    /* result_tuples = (ResultTuple *)output_tuple_buffer->data;*/
    BinaryOperatorStatement initResultTuplePtr(
        VarRef(varDeclResultTuple)
            .assign(
                TypeCast(VarRef(varDeclTupleBufferOutput).accessPtr(VarRef(declFieldDataPtrStructTupleBuf)),
                         createPointerDataType(createUserDefinedType(structDeclResultTuple)))));

    /* for (uint64_t id = 0; id < tuple_buffer_1->num_tuples; ++id) */
    FOR loopStmt(varDeclId,
                 (VarRef(varDeclId) <
                     (VarRef(varDeclTupleBuffer1).accessPtr(VarRef(declFieldNumTuplesStructTupleBuffer)))),
                 ++VarRef(varDeclId));

    /* sum = sum + tuples[id].campaign_id; */
    loopStmt.addStatement(VarRef(varDeclSum)
                              .assign(VarRef(varDeclSum) + VarRef(varDeclTuple)[VarRef(varDeclId)].accessRef(
                                  VarRef(declFieldCampaignId)))
                              .copy());

    /* function signature:
     * typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*);
     */

    FunctionDeclaration mainFunction =
        FunctionBuilder::create("compiled_query")
            .returns(createDataType(BasicType(UINT32)))
            .addParameter(varDeclTupleBuffers)
            .addParameter(varDeclWindow)
            .addParameter(varDeclWindowManager)
            .addParameter(varDeclTupleBufferOutput)
            .addVariableDeclaration(varDeclReturn)
            .addVariableDeclaration(varDeclTuple)
            .addVariableDeclaration(varDeclResultTuple)
            .addVariableDeclaration(varDeclTupleBuffer1)
            .addVariableDeclaration(varDeclSum)
            .addStatement(initTupleBufferPtr.copy())
            .addStatement(initTuplePtr.copy())
            .addStatement(initResultTuplePtr.copy())
            .addStatement(StatementPtr(new ForLoopStatement(loopStmt)))
            .addStatement(/*   result_tuples[0].sum = sum; */
                VarRef(varDeclResultTuple)[Constant(INT32, "0")]
                    .accessRef(VarRef(varDeclFieldResultTupleSum))
                    .assign(VarRef(varDeclSum))
                    .copy())
                /* return ret; */
            .addStatement(StatementPtr(new ReturnStatement(VarRefStatement(varDeclReturn))))
            .build();

    CodeFile file = FileBuilder::create("query.cpp")
        .addDeclaration(structDeclTupleBuffer)
        .addDeclaration(structDeclState)
        .addDeclaration(structDeclTuple)
        .addDeclaration(structDeclResultTuple)
        .addDeclaration(mainFunction)
        .build();

    Compiler compiler;
    ExecutablePipelinePtr stage = createCompiledExecutablePipeline(compiler.compile(file.code));


    /* setup input and output for test */
    auto inputBuffer = BufferManager::instance().getBuffer();
    inputBuffer->setTupleSizeInBytes(8);
    auto recordSchema = Schema::create().addField("id", BasicType::INT64);
    auto layout = createRowLayout(recordSchema.copy());

    for (uint32_t i = 0; i < 100; ++i) {
        layout->writeField<int64_t>(inputBuffer, i, 0, i);
    }
    inputBuffer->setNumberOfTuples(100);

    auto outputBuffer = BufferManager::instance().getBuffer();
    outputBuffer->setTupleSizeInBytes(8);
    outputBuffer->setNumberOfTuples(1);
    /* execute code */
    if (!stage->execute(inputBuffer, nullptr, nullptr, outputBuffer)) {
        std::cout << "Error!" << std::endl;
    }

    NES_INFO(toString(outputBuffer.get(), recordSchema));

    /* check result for correctness */
    auto sumGeneratedCode = layout->readField<int64_t>(outputBuffer, 0, 0);
    auto sum = 0;
    for (uint64_t i = 0; i < 100; ++i) {
        sum += layout->readField<int64_t>(inputBuffer, i, 0);;
    }
    EXPECT_EQ(sum, sumGeneratedCode);
}

/**
 * @brief This test generates a simple copy function, which copies code from one buffer to another
 */
TEST_F(CodeGenerationTest, codeGenerationCopy) {
    /* prepare objects for test */
    DataSourcePtr source = createTestSourceCodeGen();
    CodeGeneratorPtr codeGenerator = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();

    NES_INFO("Generate Code");
    /* generate code for scanning input buffer */
    codeGenerator->generateCode(source->getSchema(), context, std::cout);
    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCode(createPrintSinkWithSchema(Schema::create().addField("campaign_id", UINT64), std::cout),
                                context,
                                std::cout);
    /* compile code to pipeline stage */
    Compiler compiler;
    ExecutablePipelinePtr stage = codeGenerator->compile(CompilerArgs(), context->code);

    /* prepare input tuple buffer */
    Schema schema = Schema::create().addField("i64", UINT64);
    TupleBufferPtr buffer = source->receiveData();
    NES_INFO("Processing " << buffer->getNumberOfTuples() << " tuples: ");
    TupleBufferPtr resultBuffer = BufferManager::instance().getBuffer();
    resultBuffer->setTupleSizeInBytes(sizeof(uint64_t));
    /* execute Stage */
    stage->execute(buffer, NULL, NULL, resultBuffer);

    /* check for correctness, input source produces uint64_t tuples and stores a 1 in each tuple */
    EXPECT_EQ(buffer->getNumberOfTuples(), resultBuffer->getNumberOfTuples());

    uint64_t* result_data = (uint64_t*) resultBuffer->getBuffer();
    for (uint64_t i = 0; i < buffer->getNumberOfTuples(); ++i) {
        EXPECT_EQ(result_data[i], 1);
    }
}

const PredicatePtr createPredicate();

int CodeGeneratorFilterTest() {
    /* prepare objects for test */
    DataSourcePtr source = createTestSourceCodeGenFilter();
    CodeGeneratorPtr code_gen = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();

    Schema input_schema = source->getSchema();

    std::cout << "Generate Filter Code" << std::endl;
    /* generate code for scanning input buffer */
    code_gen->generateCode(source->getSchema(), context, std::cout);

    std::cout << std::make_shared<Predicate>(
        (PredicateItem(input_schema[0]) < PredicateItem(createBasicTypeValue(NES::BasicType::INT64, "5")))
    )->toString() << std::endl;

    PredicatePtr pred = std::dynamic_pointer_cast<Predicate>(
        (PredicateItem(input_schema[0]) < PredicateItem(createBasicTypeValue(NES::BasicType::INT64, "5"))).copy()
    );
    /*
    PredicatePtr pred=std::dynamic_pointer_cast<Predicate>(
        (PredicateItem(input_schema[0]) == 5).copy()
    );

    PredicatePtr pred=std::dynamic_pointer_cast<Predicate>(
        (5 == input_schema[0]).copy()
    );


    std::cout << std::make_shared<Predicate>(
            (PredicateItem(createStringTypeValue("abc"))==PredicateItem(createStringTypeValue("def"))))->toString() << std::endl;

    PredicatePtr pred=std::dynamic_pointer_cast<Predicate>(
            (PredicateItem(createStringTypeValue("abc"))==PredicateItem(createStringTypeValue("abc"))).copy()
            );
    */
    code_gen->generateCode(pred, context, std::cout);

    /* generate code for writing result tuples to output buffer */
    code_gen->generateCode(createPrintSinkWithSchema(Schema::create()
                                                         .addField("id", BasicType::UINT32)
                                                         .addField("value", BasicType::UINT32)
                                                         .addField("text", createArrayDataType(BasicType::CHAR, 12)),
                                                     std::cout), context, std::cout);

    /* compile code to pipeline stage */
    auto stage = code_gen->compile(CompilerArgs(), context->code);
    if (!stage)
        return -1;

    /* prepare input tuple buffer */
    TupleBufferPtr buf = source->receiveData();
    //std::cout << NES::toString(buf.get(),source->getSchema()) << std::endl;
    std::cout << "Processing " << buf->getNumberOfTuples() << " tuples: " << std::endl;
    uint32_t sizeoftuples = (sizeof(uint32_t) + sizeof(uint32_t) + sizeof(char)*12);
    size_t buffer_size = buf->getNumberOfTuples()*sizeoftuples;
    std::cout << "This is my NUMBER....: " << buffer_size << std::endl;
    TupleBufferPtr result_buffer = std::make_shared<TupleBuffer>(malloc(buffer_size), buffer_size, sizeoftuples, 0);
    /* execute Stage */
    stage->execute(buf, NULL, NULL, result_buffer);

    /* check for correctness, input source produces tuples consisting of two uint32_t values, 5 values will match the predicate */
    std::cout << "---------- My Number of tuples...." << result_buffer->getNumberOfTuples() << std::endl;
    if (result_buffer->getNumberOfTuples() != 5) {
        std::cout << "Wrong number of tuples in output: " << result_buffer->getNumberOfTuples()
                  << " (should have been: " << buf->getNumberOfTuples() << ")" << std::endl;
        return -1;
    }
    SelectionDataGenSource::InputTuple* result_data = (SelectionDataGenSource::InputTuple*) result_buffer->getBuffer();
    for (uint64_t i = 0; i < 5; ++i) {
        if (result_data[i].id != i || result_data[i].value != i*2) {
            std::cout << "Error in Result! Mismatch position: " << i << std::endl;
            return -1;
        }
    }
    std::cout << NES::toString(result_buffer.get(), Schema::create()
        .addField("id", BasicType::UINT32)
        .addField("value", BasicType::UINT32)
        .addField("text", createArrayDataType(BasicType(CHAR), 12))) << std::endl;

    std::cout << "Result of SelectionCodeGenTest is Correct!" << std::endl;

    return 0;
}

/**
 * Window assigner codegen test
 * @return
 */
int WindowAssignerCodeGenTest() {
    /* prepare objects for test */
    DataSourcePtr source = createWindowTestDataSource();
    CodeGeneratorPtr code_gen = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();

    Schema input_schema = source->getSchema();

    std::cout << "Generate Predicate Code" << std::endl;
    code_gen->generateCode(source->getSchema(), context, std::cout);

    auto sum = Sum::on(Field(input_schema.get("value")));
    WindowDefinitionPtr
        window_definition_ptr(new WindowDefinition(input_schema.get("key"),
                                                   sum,
                                                   TumblingWindow::of(TimeCharacteristic::ProcessingTime,
                                                                      Seconds(10))));
    // auto window_operator = createWindowOperator(window_definition_ptr);
    //predicate definition
    code_gen->generateCode(window_definition_ptr, context, std::cout);


    /* compile code to pipeline stage */
    auto stage = code_gen->compile(CompilerArgs(), context->code);
    if (!stage)
        return -1;

    // init window handler
    auto window_handler = new WindowHandler(window_definition_ptr);
    window_handler->setup(nullptr, 0);

    /* prepare input tuple buffer */
    TupleBufferPtr buf = source->receiveData();

    //std::cout << NES::toString(buf.get(),source->getSchema()) << std::endl;
    std::cout << "Processing " << buf->getNumberOfTuples() << " tuples: " << std::endl;

    TupleBufferPtr result_buffer = std::make_shared<TupleBuffer>(malloc(0), 0, 0, 0);

    /* execute Stage */
    stage->execute(
        buf,
        window_handler->getWindowState(),
        window_handler->getWindowManager(),
        result_buffer);

    /* check for correctness, input source produces tuples consisting of two uint32_t values, 5 values will match the predicate */
    std::cout << "---------- My Number of tuples...." << result_buffer->getNumberOfTuples() << std::endl;
    if (result_buffer->getNumberOfTuples() != 0) {
        std::cout << "Wrong number of tuples in output: " << result_buffer->getNumberOfTuples()
                  << " (should have been: " << buf->getNumberOfTuples() << ")" << std::endl;
        return -1;
    }
    auto stateVar = (StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>*) window_handler->getWindowState();

    if (stateVar->get(0).value()->getPartialAggregates()[0] != 5) {
        std::cerr << "Result of SelectionCodeGenTest is False!" << std::endl;
    } else {
        std::cout << "Result of SelectionCodeGenTest is Correct!" << std::endl;
    }
    if (stateVar->get(1).value()->getPartialAggregates()[0] != 5) {
        std::cerr << "Result of SelectionCodeGenTest is False!" << std::endl;
    } else {
        std::cout << "Result of SelectionCodeGenTest is Correct!" << std::endl;
    }
    return 0;
}

/**
 * New Predicatetests for showing stuff
 * @return
 */
int CodePredicateTests() {
    /* prepare objects for test */
    DataSourcePtr source = createTestSourceCodeGenPredicate();
    CodeGeneratorPtr code_gen = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();

    Schema input_schema = source->getSchema();

    std::cout << "Generate Predicate Code" << std::endl;
    code_gen->generateCode(input_schema, context, std::cout);

    //predicate definition
    code_gen->generateCode(createPredicate(
        (input_schema[2] > 30.4) && (input_schema[4] == 'F' || (input_schema[5] == "HHHHHHHHHHH"))),
                           context,
                           std::cout);

    unsigned int numberOfResultTuples = 3;

    /* generate code for writing result tuples to output buffer */
    code_gen->generateCode(createPrintSinkWithSchema(Schema::create()
                                                         .addField("id", BasicType::UINT32)
                                                         .addField("valueSmall", BasicType::INT16)
                                                         .addField("valueFloat", BasicType::FLOAT32)
                                                         .addField("valueDouble", BasicType::FLOAT64)
                                                         .addField("valueChar", BasicType::CHAR)
                                                         .addField("text", createArrayDataType(BasicType::CHAR, 12)),
                                                     std::cout), context, std::cout);

    /* compile code to pipeline stage */
    auto stage = code_gen->compile(CompilerArgs(), context->code);
    if (!stage)
        return -1;

    /* prepare input tuple buffer */
    TupleBufferPtr buf = source->receiveData();
    //std::cout << NES::toString(buf.get(),source->getSchema()) << std::endl;
    std::cout << "Processing " << buf->getNumberOfTuples() << " tuples: " << std::endl;
    uint32_t sizeoftuples =
        (sizeof(uint32_t) + sizeof(int16_t) + sizeof(float) + sizeof(double) + sizeof(char) + sizeof(char)*12);
    size_t buffer_size = buf->getNumberOfTuples()*sizeoftuples;
    TupleBufferPtr result_buffer = std::make_shared<TupleBuffer>(malloc(buffer_size), buffer_size, sizeoftuples, 0);


    /* execute Stage */
    stage->execute(buf, NULL, NULL, result_buffer);

    /* check for correctness, input source produces tuples consisting of two uint32_t values, 5 values will match the predicate */
    std::cout << "---------- My Number of tuples...." << result_buffer->getNumberOfTuples() << std::endl;
    if (result_buffer->getNumberOfTuples() != numberOfResultTuples) {
        std::cout << "Wrong number of tuples in output: " << result_buffer->getNumberOfTuples()
                  << " (should have been: " << buf->getNumberOfTuples() << ")" << std::endl;
        return -1;
    }

    std::cout << NES::toString(result_buffer.get(), Schema::create()
        .addField("id", BasicType::UINT32)
        .addField("valueSmall", BasicType::INT16)
        .addField("valueFloat", BasicType::FLOAT32)
        .addField("valueDouble", BasicType::FLOAT64)
        .addField("valueChar", BasicType::CHAR)
        .addField("text", createArrayDataType(BasicType::CHAR, 12))) << std::endl;
    std::cout << "Result of SelectionCodeGenTest is Correct!" << std::endl;

    return 0;
}

/**
 * New Predicatetests for showing stuff
 * @return
 */
int CodeMapPredicatePtrTests() {
    /* prepare objects for test */
    DataSourcePtr source = createTestSourceCodeGenPredicate();
    CodeGeneratorPtr code_gen = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();

    Schema input_schema = source->getSchema();

    std::cout << "Generate Predicate Code" << std::endl;
    code_gen->generateCode(input_schema, context, std::cout);

    //predicate definition
    AttributeFieldPtr mapped_value = AttributeField("mapped_value", BasicType::FLOAT64).copy();
    code_gen->generateCode(mapped_value, createPredicate((input_schema[2]*input_schema[3]) + 2), context, std::cout);

    /* generate code for writing result tuples to output buffer */
    code_gen->generateCode(createPrintSinkWithSchema(Schema::create()
                                                         .addField("id", BasicType::UINT32)
                                                         .addField("valueSmall", BasicType::INT16)
                                                         .addField("valueFloat", BasicType::FLOAT32)
                                                         .addField("valueDouble", BasicType::FLOAT64)
                                                         .addField(mapped_value)
                                                         .addField("valueChar", BasicType::CHAR)
                                                         .addField("text", createArrayDataType(BasicType::CHAR, 12)),
                                                     std::cout), context, std::cout);

    unsigned int numberOfResultTuples = 132;

    /* compile code to pipeline stage */
    auto stage = code_gen->compile(CompilerArgs(), context->code);
    if (!stage)
        return -1;

    /* prepare input tuple buffer */
    TupleBufferPtr buf = source->receiveData();
    //std::cout << NES::toString(buf.get(),source->getSchema()) << std::endl;
    std::cout << "Processing " << buf->getNumberOfTuples() << " tuples: " << std::endl;
    uint32_t sizeoftuples =
        (sizeof(uint32_t) + sizeof(int16_t) + sizeof(float) + sizeof(double) + sizeof(double) + sizeof(char)
            + sizeof(char)*12);
    size_t buffer_size = buf->getNumberOfTuples()*sizeoftuples;

    TupleBufferPtr result_buffer = std::make_shared<TupleBuffer>(malloc(buffer_size), buffer_size, sizeoftuples, 0);

    /* execute Stage */
    stage->execute(buf, NULL, NULL, result_buffer);

    /* check for correctness, input source produces tuples consisting of two uint32_t values, 5 values will match the predicate */
    std::cout << "---------- My Number of tuples...." << result_buffer->getNumberOfTuples() << std::endl;
    if (result_buffer->getNumberOfTuples() != numberOfResultTuples) {
        std::cout << "Wrong number of tuples in output: " << result_buffer->getNumberOfTuples()
                  << " (should have been: " << buf->getNumberOfTuples() << ")" << std::endl;
        return -1;
    }

    std::cout << NES::toString(result_buffer.get(), Schema::create()
        .addField("id", BasicType::UINT32)
        .addField("valueSmall", BasicType::INT16)
        .addField("valueFloat", BasicType::FLOAT32)
        .addField("valueDouble", BasicType::FLOAT64)
        .addField("mapped_value", BasicType::FLOAT64)
        .addField("valueChar", BasicType::CHAR)
        .addField("text", createArrayDataType(BasicType::CHAR, 12))) << std::endl;
    std::cout << "Result of MapPredPtrCodeGenTest is Correct!" << std::endl;

    return 0;
}

int testTupleBufferPrinting() {

    struct __attribute__((packed)) MyTuple {
        uint64_t i64;
        float f;
        double d;
        uint32_t i32;
        char s[12];
    };

    MyTuple* my_array = (MyTuple*) malloc(5*sizeof(MyTuple));
    for (unsigned int i = 0; i < 5; ++i) {
        my_array[i] = MyTuple{i, float(0.5f*i), double(i*0.2), i*2, "1234"};
        std::cout << my_array[i].i64 << "|" << my_array[i].f << "|" << my_array[i].d << "|" << my_array[i].i32 << "|"
                  << std::string(my_array[i].s, 12) << std::endl;
    }

    TupleBuffer buf{my_array, 5*sizeof(MyTuple), sizeof(MyTuple), 5};
    Schema s = Schema::create()
        .addField("i64", UINT64)
        .addField("f", FLOAT32)
        .addField("d", FLOAT64)
        .addField("i32", UINT32)
        .addField("s", 12);

    std::string reference = "+----------------------------------------------------+\n"
                            "|i64:UINT64|f:FLOAT32|d:FLOAT64|i32:UINT32|s:CHAR|\n"
                            "+----------------------------------------------------+\n"
                            "|0|0.000000|0.000000|0|1234|\n"
                            "|1|0.500000|0.200000|2|1234|\n"
                            "|2|1.000000|0.400000|4|1234|\n"
                            "|3|1.500000|0.600000|6|1234|\n"
                            "|4|2.000000|0.800000|8|1234|\n"
                            "+----------------------------------------------------+";

    std::string result = NES::toString(buf, s);
    std::cout << "'" << reference << "'" << reference.size() << std::endl;
    std::cout << "'" << result << "'" << result.size() << std::endl;

    assert(reference.size() == result.size());
    if (reference == result) {
        std::cout << "Print Test Successful!" << std::endl;
    } else {
        std::cout << "Print Test Failed!" << std::endl;
    }

    free(my_array);
    return 0;
}

} // namespace NES

/*

int main() {



  if (!NES::WindowAssignerCodeGenTest()) {
    std::cout << "Test CodeGenTest Passed!" << std::endl << std::endl;
  } else {
    std::cerr << "Test CodeGenTest Failed!" << std::endl << std::endl;
    return -1;
  }

  if (!NES::CodeGeneratorTest()) {
    std::cerr << "Test CodeGeneratorTest Passed!" << std::endl << std::endl;
  } else {
    std::cerr << "Test CodeGeneratorTest Failed!" << std::endl << std::endl;
    return -1;
  }

  if (!NES::CodeGeneratorFilterTest()) {
    std::cerr << "Test CodeGeneratorFilterTest Passed!" << std::endl << std::endl;
  } else {
    std::cerr << "Test CodeGeneratorFilterTest Failed!" << std::endl << std::endl;
    return -1;
  }

  if (!NES::testTupleBufferPrinting()) {
    std::cout << "Test Print Tuple Buffer Passed!" << std::endl << std::endl;
  } else {
    std::cerr << "Test Print Tuple Buffer Failed!" << std::endl << std::endl;
    return -1;
  }

  if (!NES::CodePredicateTests()) {
    std::cout << "Test Predicate Passed!" << std::endl << std::endl;
  } else {
    std::cerr << "Test Predicate Failed!" << std::endl << std::endl;
    return -1;
  }

  if (!NES::CodeMapPredicatePtrTests()) {
    std::cout << "Test Map for Predicate Pointers Passed!" << std::endl << std::endl;
  } else {
    std::cerr << "Test Map for Predicate Pointers Failed!" << std::endl << std::endl;
    return -1;
  }

  std::cout << " all tests are passed" << std::endl;
  return 0;
}
*/

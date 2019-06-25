
#include <cassert>
#include <iostream>

#include <CodeGen/CodeGen.hpp>
#include <CodeGen/PipelineStage.hpp>
#include <Core/DataTypes.hpp>
#include <Util/ErrorHandling.hpp>
#include <Runtime/DataSink.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/BufferManager.hpp>

#include <CodeGen/C_CodeGen/BinaryOperatorStatement.hpp>
#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/FileBuilder.hpp>
#include <CodeGen/C_CodeGen/FunctionBuilder.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/C_CodeGen/UnaryOperatorStatement.hpp>
#include <API/UserAPIExpression.hpp>

namespace iotdb {

  const DataSourcePtr createTestSourceCodeGen()
  {
      // Shall this go to the UnitTest Directory in future?
      class Functor {
        public:
          Functor() : one(1) {}
          TupleBufferPtr operator()()
          {
              // 10 tuples of size one
              TupleBufferPtr buf = BufferManager::instance().getBuffer();
              size_t tupleCnt = buf->buffer_size / sizeof(uint64_t);

              assert(buf->buffer != NULL);

              uint64_t* tuples = (uint64_t*)buf->buffer;
              for (uint64_t i = 0; i < tupleCnt; i++) {
                  tuples[i] = one;
              }
              buf->tuple_size_bytes = sizeof(uint64_t);
              buf->num_tuples = tupleCnt;
              return buf;
          }

          uint64_t one;
      };

      DataSourcePtr source(new GeneratorSource<Functor>(Schema::create().addField(createField("campaign_id", UINT64)), 1));

      return source;
  }


  class SelectionDataGenFunctor {
  public:
      SelectionDataGenFunctor(){}

      struct __attribute__((packed)) InputTuple {
        uint32_t id;
        uint32_t value;
        char text[12];
      };


      TupleBufferPtr operator()()
      {
          // 10 tuples of size one
          TupleBufferPtr buf = BufferManager::instance().getBuffer();
          uint64_t tupleCnt = buf->buffer_size / sizeof(InputTuple);

          assert(buf->buffer != NULL);

          InputTuple* tuples = (InputTuple*)buf->buffer;
          for (uint32_t i = 0; i < tupleCnt; i++) {
              tuples[i].id = i;
              tuples[i].value = i*2;
              for(int j=0;j<11;++j) {
                  tuples[i].text[j] = ((j+i)%(255-'a'))+'a';
              }
              tuples[i].text[12] = '\0';
          }
          buf->tuple_size_bytes = sizeof(InputTuple);
          buf->num_tuples = tupleCnt;
          return buf;
      }
  };


    const DataSourcePtr createTestSourceCodeGenFilter()
    {

        DataSourcePtr source(new GeneratorSource<SelectionDataGenFunctor>(
                Schema::create()
                    .addField("id", BasicType::UINT32)
                    .addField("value", BasicType::UINT32)
                    .addField("text", createArrayDataType(BasicType::CHAR, 12)), 1));

        return source;
    }


/* TODO: make proper test suite out of these */
int CodeGenTestCases()
{

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

        std::vector<std::string> vals = {"a","b","c"};
        VariableDeclaration var_decl_m =
                VariableDeclaration::create(createArrayDataType(BasicType(CHAR), 12), "m", createArrayValueType(BasicType(CHAR), vals));
        std::cout << (VarRefStatement(var_decl_m).getCode()->code_) << std::endl;

        VariableDeclaration var_decl_n = VariableDeclaration::create(createArrayDataType(BasicType(CHAR), 12), "n",
                                                                     createArrayValueType(BasicType(CHAR), vals));
        std::cout << var_decl_n.getCode() << std::endl;

        VariableDeclaration var_decl_o = VariableDeclaration::create(createArrayDataType(BasicType(UINT8), 4), "o",
                                                                     createArrayValueType(BasicType(UINT8), {"2","3","4"}));
        std::cout << var_decl_o.getCode() << std::endl;

        VariableDeclaration var_decl_p = VariableDeclaration::create(createArrayDataType(BasicType(CHAR), 20), "p",
                                                                     createStringValueType("diesisteinTest", 20));
        std::cout << var_decl_p.getCode() << std::endl;

        std::cout << createStringValueType("DiesIstEinZweiterTest\0dwqdwq")->getCodeExpression()->code_ << std::endl;

        std::cout << createBasicTypeValue(BasicType::CHAR, "DiesIstEinDritterTest")->getCodeExpression()->code_ << std::endl;

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
                        assign(VarRef(var_decl_i), VarRef(var_decl_i) * VarRef(var_decl_k)))
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
        VariableDeclaration var_decl_temp = VariableDeclaration::create(charptr, "i", createStringValueType("Hello World"));
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

int CodeGenTest()
{

    /* === struct type definitions === */
    /** define structure of TupleBuffer
      struct TupleBuffer {
        void *data;
        uint64_t buffer_size;
        uint64_t tuple_size_bytes;
        uint64_t num_tuples;
      };
    */
    StructDeclaration struct_decl_tuple_buffer =
        StructDeclaration::create("TupleBuffer", "")
            .addField(VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))), "data"))
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "buffer_size"))
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "tuple_size_bytes"))
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "num_tuples"));

    /**
       define the WindowState struct
     struct WindowState {
      void *window_state;
      };
    */
    StructDeclaration struct_decl_state =
        StructDeclaration::create("WindowState", "")
            .addField(VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))),
                                                  "window_state"));

    /* struct definition for input tuples */
    StructDeclaration struct_decl_tuple =
        StructDeclaration::create("Tuple", "")
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "campaign_id"));

    /* struct definition for result tuples */
    StructDeclaration struct_decl_result_tuple =
        StructDeclaration::create("ResultTuple", "")
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "sum"));

    /* === declarations === */

    VariableDeclaration var_decl_tuple_buffers = VariableDeclaration::create(
        createPointerDataType(createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer))), "window_buffer");
    VariableDeclaration var_decl_tuple_buffer_output = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer)), "output_tuple_buffer");
    VariableDeclaration var_decl_state =
        VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl_state)), "global_state");

    /* Tuple *tuples; */

    VariableDeclaration var_decl_tuple =
        VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl_tuple)), "tuples");

    VariableDeclaration var_decl_result_tuple = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_result_tuple)), "result_tuples");

    /* variable declarations for fields inside structs */
    VariableDeclaration decl_field_campaign_id = struct_decl_tuple.getVariableDeclaration("campaign_id");
    VariableDeclaration decl_field_num_tuples_struct_tuple_buf =
        struct_decl_tuple_buffer.getVariableDeclaration("num_tuples");
    VariableDeclaration decl_field_data_ptr_struct_tuple_buf = struct_decl_tuple_buffer.getVariableDeclaration("data");
    VariableDeclaration var_decl_field_result_tuple_sum = struct_decl_result_tuple.getVariableDeclaration("sum");

    /* === generating the query function === */

    /* variable declarations */

    /* TupleBuffer *tuple_buffer_1; */
    VariableDeclaration var_decl_tuple_buffer_1 = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer)), "tuple_buffer_1");
    /* uint64_t id = 0; */
    VariableDeclaration var_decl_id = VariableDeclaration::create(createDataType(BasicType(UINT64)), "id",
                                                                  createBasicTypeValue(BasicType(INT32), "0"));
    /* int32_t ret = 0; */
    VariableDeclaration var_decl_return = VariableDeclaration::create(createDataType(BasicType(INT32)), "ret",
                                                                      createBasicTypeValue(BasicType(INT32), "0"));
    /* int32_t sum = 0;*/
    VariableDeclaration var_decl_sum = VariableDeclaration::create(createDataType(BasicType(INT32)), "sum",
                                                                   createBasicTypeValue(BasicType(INT32), "0"));

    /* init statements before for loop */

    /* tuple_buffer_1 = window_buffer[0]; */
    BinaryOperatorStatement init_tuple_buffer_ptr(
        VarRefStatement(var_decl_tuple_buffer_1)
            .assign(VarRefStatement(var_decl_tuple_buffers)[ConstantExprStatement(INT32, "0")]));
    /*  tuples = (Tuple *)tuple_buffer_1->data;*/
    BinaryOperatorStatement init_tuple_ptr(
        VarRef(var_decl_tuple)
            .assign(TypeCast(
                VarRefStatement(var_decl_tuple_buffer_1).accessPtr(VarRef(decl_field_data_ptr_struct_tuple_buf)),
                createPointerDataType(createUserDefinedType(struct_decl_tuple)))));

    /* result_tuples = (ResultTuple *)output_tuple_buffer->data;*/
    BinaryOperatorStatement init_result_tuple_ptr(
        VarRef(var_decl_result_tuple)
            .assign(
                TypeCast(VarRef(var_decl_tuple_buffer_output).accessPtr(VarRef(decl_field_data_ptr_struct_tuple_buf)),
                         createPointerDataType(createUserDefinedType(struct_decl_result_tuple)))));

    /* for (uint64_t id = 0; id < tuple_buffer_1->num_tuples; ++id) */
    FOR loop_stmt(var_decl_id,
                  (VarRef(var_decl_id) <
                   (VarRef(var_decl_tuple_buffer_1).accessPtr(VarRef(decl_field_num_tuples_struct_tuple_buf)))),
                  ++VarRef(var_decl_id));

    /* sum = sum + tuples[id].campaign_id; */
    loop_stmt.addStatement(VarRef(var_decl_sum)
                               .assign(VarRef(var_decl_sum) + VarRef(var_decl_tuple)[VarRef(var_decl_id)].accessRef(
                                                                  VarRef(decl_field_campaign_id)))
                               .copy());

    /* function signature:
     * typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*);
     */

    FunctionDeclaration main_function =
        FunctionBuilder::create("compiled_query")
            .returns(createDataType(BasicType(UINT32)))
            .addParameter(var_decl_tuple_buffers)
            .addParameter(var_decl_state)
            .addParameter(var_decl_tuple_buffer_output)
            .addVariableDeclaration(var_decl_return)
            .addVariableDeclaration(var_decl_tuple)
            .addVariableDeclaration(var_decl_result_tuple)
            .addVariableDeclaration(var_decl_tuple_buffer_1)
            .addVariableDeclaration(var_decl_sum)
            .addStatement(init_tuple_buffer_ptr.copy())
            .addStatement(init_tuple_ptr.copy())
            .addStatement(init_result_tuple_ptr.copy())
            .addStatement(StatementPtr(new ForLoopStatement(loop_stmt)))
            .addStatement(/*   result_tuples[0].sum = sum; */
                          VarRef(var_decl_result_tuple)[Constant(INT32, "0")]
                              .accessRef(VarRef(var_decl_field_result_tuple_sum))
                              .assign(VarRef(var_decl_sum))
                              .copy())
            /* return ret; */
            .addStatement(StatementPtr(new ReturnStatement(VarRefStatement(var_decl_return))))
            .build();

    CodeFile file = FileBuilder::create("query.cpp")
                        .addDeclaration(struct_decl_tuple_buffer)
                        .addDeclaration(struct_decl_state)
                        .addDeclaration(struct_decl_tuple)
                        .addDeclaration(struct_decl_result_tuple)
                        .addDeclaration(main_function)
                        .build();

    PipelineStagePtr stage = compile(file);

    /* setup minimal runtime to execute generated code */

    uint64_t* my_array = (uint64_t*)malloc(100 * sizeof(uint64_t));
    for (unsigned int i = 0; i < 100; ++i) {
        my_array[i] = i;
    }

    TupleBuffer buf{my_array, 100 * sizeof(uint64_t), sizeof(uint64_t), 100};

    uint64_t* result_array = (uint64_t*)malloc(1 * sizeof(uint64_t));

    std::vector<TupleBuffer*> bufs;
    bufs.push_back(&buf);

    TupleBuffer result_buf{result_array, sizeof(uint64_t), sizeof(uint64_t), 1};

    /* execute code */
    if (!stage->execute(bufs, nullptr, &result_buf)) {
        std::cout << "Error!" << std::endl;
    }

    std::cout << toString(&result_buf, Schema::create().addField("sum", UINT64)) << std::endl;

    /* check result for correctness */
    uint64_t sum_generated_code = result_array[0];
    std::cout << "Sum (Generated Code): " << sum_generated_code << std::endl;

    uint64_t my_sum = 0;
    for (uint64_t i = 0; i < 100; ++i) {
        my_sum += my_array[i];
    }
    std::cout << "my sum: " << my_sum << std::endl;

    free(my_array);
    free(result_array);

    if (my_sum == sum_generated_code) {
        return 0;
    }
    else {
        std::cerr << "Test Failed, sums do not match!" << std::endl;
        return 1;
    }

    return 0;
}

int CodeGeneratorTest()
{
    /* prepare objects for test */
    DataSourcePtr source = createTestSourceCodeGen();
    CodeGeneratorPtr code_gen = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();

    std::cout << "Generate Code" << std::endl;
    /* generate code for scanning input buffer */
    code_gen->generateCode(source, context, std::cout);
    /* generate code for writing result tuples to output buffer */
    code_gen->generateCode(createPrintSink(Schema::create().addField("campaign_id",UINT64),std::cout), context, std::cout);
    /* compile code to pipeline stage */
    PipelineStagePtr stage = code_gen->compile(CompilerArgs());
    if(!stage)
        return -1;
    /* prepare input tuple buffer */
    Schema s = Schema::create()
                   .addField("i64", UINT64);
    TupleBufferPtr buf = source->receiveData();
    std::vector<TupleBuffer*> input_buffers;
    input_buffers.push_back(buf.get());
    //std::cout << iotdb::toString(buf.get(),source->getSchema()) << std::endl;
    std::cout << "Processing " << buf->num_tuples << " tuples: " << std::endl;
    size_t buffer_size = buf->num_tuples*sizeof (uint64_t);
    TupleBuffer result_buffer(malloc(buffer_size), buffer_size,sizeof(uint64_t),0);

    /* execute Stage */
    stage->execute(input_buffers, NULL, &result_buffer);

    /* check for correctness, input source produces uint64_t tuples and stores a 1 in each tuple */
    //std::cout << "Result Buffer: #tuples: " << result_buffer.num_tuples << std::endl;
    if(buf->num_tuples!=result_buffer.num_tuples){
      std::cout << "Wrong number of tuples in output: " << result_buffer.num_tuples
                << " (should have been: " << buf->num_tuples << ")" << std::endl;
      return -1;
    }
    uint64_t* result_data = (uint64_t*) result_buffer.buffer;
    for(uint64_t i=0;i<buf->num_tuples;++i){
        if(result_data[i]!=1){
          std::cout << "Error in Result! Mismatch position: " << i << std::endl;
          return -1;
        }
    }

    //std::cout << iotdb::toString(result_buffer,s) << std::endl;

    return 0;
}

    const PredicatePtr createPredicate();

    int CodeGeneratorFilterTest()
    {
        /* prepare objects for test */
    DataSourcePtr source = createTestSourceCodeGenFilter();
    CodeGeneratorPtr code_gen = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();

    Schema input_schema = source->getSchema();

    std::cout << "Generate Filter Code" << std::endl;
    /* generate code for scanning input buffer */
    code_gen->generateCode(source, context, std::cout);


	std::cout << std::make_shared<Predicate>(
		      (PredicateItem(input_schema[0])<PredicateItem(createBasicTypeValue(iotdb::BasicType::INT64,"5")))
		    )->toString() << std::endl;

	PredicatePtr pred=std::dynamic_pointer_cast<Predicate>(
	      (PredicateItem(input_schema[0])<PredicateItem(createBasicTypeValue(iotdb::BasicType::INT64,"5"))).copy()
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
    code_gen->generateCode(createPrintSink(Schema::create()
                                                   .addField("id", BasicType::UINT32)
                                                   .addField("value", BasicType::UINT32)
                                                   .addField("text", createArrayDataType(BasicType::CHAR, 12)), std::cout), context, std::cout);

    /* compile code to pipeline stage */
    PipelineStagePtr stage = code_gen->compile(CompilerArgs());
    if(!stage)
        return -1;

    /* prepare input tuple buffer */
    TupleBufferPtr buf = source->receiveData();
    std::vector<TupleBuffer*> input_buffers;
    input_buffers.push_back(buf.get());
    std::cout << "My Test" << std::endl;
    //std::cout << iotdb::toString(buf.get(),source->getSchema()) << std::endl;
    std::cout << "Processing " << buf->num_tuples << " tuples: " << std::endl;
    uint32_t sizeoftuples = (sizeof(uint32_t) + sizeof(uint32_t) + sizeof(char) * 12);
    size_t buffer_size = buf->num_tuples * sizeoftuples;
    std::cout << "This is my NUMBER....: " << buffer_size << std::endl;
    TupleBuffer result_buffer(malloc(buffer_size), buffer_size, sizeoftuples, 0);

    /* execute Stage */
    stage->execute(input_buffers, NULL, &result_buffer);

    /* check for correctness, input source produces tuples consisting of two uint32_t values, 5 values will match the predicate */
    std::cout << "---------- My Number of tuples...." << result_buffer.num_tuples << std::endl;
    if(result_buffer.num_tuples!=5){
        std::cout << "Wrong number of tuples in output: " << result_buffer.num_tuples
                  << " (should have been: " << buf->num_tuples << ")" << std::endl;
        //return -1;
    }
    SelectionDataGenFunctor::InputTuple* result_data = (SelectionDataGenFunctor::InputTuple*) result_buffer.buffer;
    for(uint64_t i=0;i<5;++i){
        if(result_data[i].id!=i || result_data[i].value!=i*2){
            std::cout << "Error in Result! Mismatch position: " << i << std::endl;
            //return -1;
        }
    }
    std::cout << "Jetzt gehts los" << std::endl;
    std::cout << iotdb::toString(result_buffer,Schema::create()
                                 .addField("id", BasicType::UINT32)
                                 .addField("value", BasicType::UINT32)
                                 .addField("text", createArrayDataType(BasicType(CHAR), 12))) << std::endl;

    std::cout << "Result of SelectionCodeGenTest is Correct!" << std::endl;

    return 0;
    }

int testTupleBufferPrinting()
{

    struct __attribute__((packed)) MyTuple {
        uint64_t i64;
        float f;
        double d;
        uint32_t i32;
        char s[12];
    };

    MyTuple* my_array = (MyTuple*)malloc(5 * sizeof(MyTuple));
    for (unsigned int i = 0; i < 5; ++i) {
        my_array[i] = MyTuple{i, float(0.5f * i), double(i * 0.2), i * 2, "1234"};
        std::cout << my_array[i].i64 << "|" << my_array[i].f << "|" << my_array[i].d << "|" << my_array[i].i32 << "|"
                  << std::string(my_array[i].s, 12) << std::endl;
    }

    TupleBuffer buf{my_array, 5 * sizeof(MyTuple), sizeof(MyTuple), 5};
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

    std::string result = iotdb::toString(buf, s);
    std::cout << "'" << reference << "'" << reference.size() << std::endl;
    std::cout << "'" << result << "'" << result.size() << std::endl;

    assert(reference.size() == result.size());
    if (reference == result) {
        std::cout << "Print Test Successful!" << std::endl;
    }
    else {
        std::cout << "Print Test Failed!" << std::endl;
    }

    free(my_array);
    return 0;
}

} // namespace iotdb

int main()
{

    /** \todo make proper test case out of this function! */
    iotdb::CodeGenTestCases();

    if (!iotdb::CodeGenTest()) {
        std::cout << "Test CodeGenTest Passed!" << std::endl << std::endl;
    }
    else {
        std::cerr << "Test CodeGenTest Failed!" << std::endl << std::endl;
        return -1;
    }

    if (!iotdb::CodeGeneratorTest()) {
        std::cerr << "Test CodeGeneratorTest Passed!" << std::endl << std::endl;
    }
    else {
        std::cerr << "Test CodeGeneratorTest Failed!" << std::endl << std::endl;
        return -1;
    }

    if (!iotdb::CodeGeneratorFilterTest()) {
        std::cerr << "Test CodeGeneratorFilterTest Passed!" << std::endl << std::endl;
    }
    else {
        std::cerr << "Test CodeGeneratorFilterTest Failed!" << std::endl << std::endl;
        return -1;
    }

    if (!iotdb::testTupleBufferPrinting()) {
        std::cout << "Test Print Tuple Buffer Passed!" << std::endl << std::endl;
    }
    else {
        std::cerr << "Test Print Tuple Buffer Failed!" << std::endl << std::endl;
        return -1;
    }

    return 0;
}

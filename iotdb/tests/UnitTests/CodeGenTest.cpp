
#include <iostream>
#include <cassert>

#include <CodeGen/PipelineStage.hpp>
#include <CodeGen/CodeGen.hpp>
#include <Core/DataTypes.hpp>
#include <Util/ErrorHandling.hpp>

#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/C_CodeGen/UnaryOperatorStatement.hpp>
#include <CodeGen/C_CodeGen/BinaryOperatorStatement.hpp>
#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/FunctionBuilder.hpp>
#include <CodeGen/C_CodeGen/FileBuilder.hpp>

namespace iotdb {

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
      CodeExpressionPtr code = BinaryOperatorStatement(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j))
                                   .addRight(PLUS_OP, VarRefStatement(var_decl_k))
                                   .addRight(MULTIPLY_OP, VarRefStatement(var_decl_i), BRACKETS)
                                   .addRight(GREATER_THEN_OP, VarRefStatement(var_decl_l))
                                   .getCode();

      std::cout << code->code_ << std::endl;

      std::cout << "=========================" << std::endl;

      std::cout << BinaryOperatorStatement(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j)).getCode()->code_ << std::endl;
      std::cout << (VarRefStatement(var_decl_i) + VarRefStatement(var_decl_j)).getCode()->code_ << std::endl;

      std::cout << "=========================" << std::endl;

      std::cout << UnaryOperatorStatement(VarRefStatement(var_decl_i),POSTFIX_INCREMENT_OP).getCode()->code_ << std::endl;
      std::cout << (++VarRefStatement(var_decl_i)).getCode()->code_ << std::endl;
      std::cout << (VarRefStatement(var_decl_i) >= VarRefStatement(var_decl_j))[VarRefStatement(var_decl_j)].getCode()->code_ << std::endl;

      std::cout << ((~VarRefStatement(var_decl_i) >= VarRefStatement(var_decl_j) << ConstantExprStatement(createBasicTypeValue(INT32,"0"))))[VarRefStatement(var_decl_j)].getCode()->code_ << std::endl;

      std::cout << VarRefStatement(var_decl_i).assign(VarRefStatement(var_decl_i)+VarRefStatement(var_decl_j)).getCode()->code_ << std::endl;

      std::cout << (sizeOf(VarRefStatement(var_decl_i))).getCode()->code_ << std::endl;

      std::cout << assign(VarRef(var_decl_i), VarRef(var_decl_i)).getCode()->code_  << std::endl;

      std::cout << IF(VarRef(var_decl_i)<VarRef(var_decl_j),
                      assign(VarRef(var_decl_i), VarRef(var_decl_i)*VarRef(var_decl_k))).getCode()->code_  << std::endl;

      std::cout << "=========================" << std::endl;


      std::cout << IfStatement(
                       BinaryOperatorStatement(VarRefStatement(var_decl_i), GREATER_THEN_OP, VarRefStatement(var_decl_j)),
                       ReturnStatement(VarRefStatement(var_decl_i)))
                       .getCode()
                       ->code_
                << std::endl;

      std::cout << IfStatement(VarRefStatement(var_decl_j), VarRefStatement(var_decl_i)).getCode()->code_ << std::endl;
    }

    {
      std::cout << BinaryOperatorStatement(
                       VarRefStatement(var_decl_k), ASSIGNMENT_OP,
                       BinaryOperatorStatement(VarRefStatement(var_decl_j), GREATER_THEN_OP, VarRefStatement(var_decl_i)))
                       .getCode()
                       ->code_
                << std::endl;
    }

    {
      VariableDeclaration var_decl_num_tup = VariableDeclaration::create(createDataType(BasicType(INT32)), "num_tuples",
                                                                         createBasicTypeValue(BasicType(INT32), "0"));

      std::cout << toString(ADDRESS_OF_OP) << ": "
                << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), ADDRESS_OF_OP).getCode()->code_ << std::endl;
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

      VariableDeclaration var_decl_q =
          VariableDeclaration::create(createDataType(BasicType(INT32)), "q", createBasicTypeValue(BasicType(INT32), "0"));
      VariableDeclaration var_decl_num_tup = VariableDeclaration::create(createDataType(BasicType(INT32)), "num_tuples",
                                                                         createBasicTypeValue(BasicType(INT32), "0"));

      VariableDeclaration var_decl_sum = VariableDeclaration::create(createDataType(BasicType(INT32)), "sum",
                                                                     createBasicTypeValue(BasicType(INT32), "0"));

      ForLoopStatement loop_stmt(var_decl_q, BinaryOperatorStatement(VarRefStatement(var_decl_q), LESS_THEN_OP,
                                                                     VarRefStatement(var_decl_num_tup)),
                                 UnaryOperatorStatement(VarRefStatement(var_decl_q), PREFIX_INCREMENT_OP));

      loop_stmt.addStatement(BinaryOperatorStatement(VarRefStatement(var_decl_sum), ASSIGNMENT_OP,
                                                     BinaryOperatorStatement(VarRefStatement(var_decl_sum), PLUS_OP,
                                                                             VarRefStatement(var_decl_q)))
                                 .copy());

      std::cout << loop_stmt.getCode()->code_ << std::endl;

      std::cout << ForLoopStatement(var_decl_q, BinaryOperatorStatement(VarRefStatement(var_decl_q), LESS_THEN_OP,
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
      VariableDeclaration var_decl_i =
          VariableDeclaration::create(createDataType(BasicType(INT32)), "i", createBasicTypeValue(BasicType(INT32), "0"));
      VariableDeclaration var_decl_p = VariableDeclaration::create(val, "array");

      std::cout << var_decl_i.getCode() << std::endl;
      std::cout << var_decl_p.getCode() << std::endl;

      StructDeclaration struct_decl =
          StructDeclaration::create("TupleBuffer", "buffer")
              .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "num_tuples",
                                                    createBasicTypeValue(BasicType(UINT64), "0")))
              .addField(var_decl_p);

      std::cout << VariableDeclaration::create(createUserDefinedType(struct_decl), "buffer").getCode() << std::endl;
      std::cout
          << VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl)), "buffer").getCode()
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
    std::cout << BinaryOperatorStatement(VarRefStatement(var_decl_tuple), ARRAY_REFERENCE_OP, VarRefStatement(var_decl_i))
                     .getCode()
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

  int CodeGenTest() {

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
            .addField(
                VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))), "window_state"));

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
    VariableDeclaration decl_field_num_tuples_struct_tuple_buf = struct_decl_tuple_buffer.getVariableDeclaration("num_tuples");
    VariableDeclaration decl_field_data_ptr_struct_tuple_buf = struct_decl_tuple_buffer.getVariableDeclaration("data");
    VariableDeclaration var_decl_field_result_tuple_sum = struct_decl_result_tuple.getVariableDeclaration("sum");


    /* === generating the query function === */

    /* variable declarations */

    /* TupleBuffer *tuple_buffer_1; */
    VariableDeclaration var_decl_tuple_buffer_1 = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer)), "tuple_buffer_1");
    /* uint64_t id = 0; */
    VariableDeclaration var_decl_id =
        VariableDeclaration::create(createDataType(BasicType(UINT64)), "id", createBasicTypeValue(BasicType(INT32), "0"));
    /* int32_t ret = 0; */
    VariableDeclaration var_decl_return =
        VariableDeclaration::create(createDataType(BasicType(INT32)), "ret", createBasicTypeValue(BasicType(INT32), "0"));
    /* int32_t sum = 0;*/
    VariableDeclaration var_decl_sum =
        VariableDeclaration::create(createDataType(BasicType(INT32)), "sum", createBasicTypeValue(BasicType(INT32), "0"));

    /* init statements before for loop */

    /* tuple_buffer_1 = window_buffer[0]; */
    BinaryOperatorStatement init_tuple_buffer_ptr(VarRefStatement(var_decl_tuple_buffer_1).assign(
                                                    VarRefStatement(var_decl_tuple_buffers)[ConstantExprStatement(INT32, "0")]));
    /*  tuples = (Tuple *)tuple_buffer_1->data;*/
    BinaryOperatorStatement init_tuple_ptr(
        VarRef(var_decl_tuple).assign(
        TypeCast(VarRefStatement(var_decl_tuple_buffer_1)
          .accessPtr(VarRef(decl_field_data_ptr_struct_tuple_buf)),
          createPointerDataType(createUserDefinedType(struct_decl_tuple)))));

     /* result_tuples = (ResultTuple *)output_tuple_buffer->data;*/
    BinaryOperatorStatement init_result_tuple_ptr(
        VarRef(var_decl_result_tuple).assign(
        TypeCast(VarRef(var_decl_tuple_buffer_output).accessPtr(VarRef(decl_field_data_ptr_struct_tuple_buf)),
                              createPointerDataType(createUserDefinedType(struct_decl_result_tuple)))));

    /* for (uint64_t id = 0; id < tuple_buffer_1->num_tuples; ++id) */
    FOR loop_stmt(
        var_decl_id, (VarRef(var_decl_id) < (VarRef(var_decl_tuple_buffer_1).accessPtr(
                                                 VarRef(decl_field_num_tuples_struct_tuple_buf)))),
        ++VarRef(var_decl_id));

    /* sum = sum + tuples[id].campaign_id; */
    loop_stmt.addStatement(
            VarRef(var_decl_sum).assign(
                VarRef(var_decl_sum)
              + VarRef(var_decl_tuple)[VarRef(var_decl_id)].accessRef(VarRef(decl_field_campaign_id)))
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
            .addStatement( /*   result_tuples[0].sum = sum; */
              VarRef(var_decl_result_tuple)[Constant(INT32, "0")].accessRef(VarRef(var_decl_field_result_tuple_sum)).assign(VarRef(var_decl_sum))
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

    uint64_t *my_array = (uint64_t *)malloc(100 * sizeof(uint64_t));
    for (unsigned int i = 0; i < 100; ++i) {
      my_array[i] = i;
    }

    TupleBuffer buf{my_array, 100 * sizeof(uint64_t), sizeof(uint64_t), 100};

    uint64_t *result_array = (uint64_t *)malloc(1 * sizeof(uint64_t));

    std::vector<TupleBuffer *> bufs;
    bufs.push_back(&buf);

    TupleBuffer result_buf{result_array, sizeof(uint64_t), sizeof(uint64_t), 1};

    /* execute code */
    if (!stage->execute(bufs, nullptr, &result_buf)) {
      std::cout << "Error!" << std::endl;
    }

    std::cout << toString(&result_buf,Schema::create().addField("sum",UINT64)) << std::endl;

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

    if(my_sum==sum_generated_code){
        return 0;
      }else{
        std::cerr << "Test Failed, sums do not match!" << std::endl;
        return 1;
      }

    return 0;
  }

  int CodeGeneratorTest(){

    CodeGeneratorPtr code_gen = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();
    std::cout << "Generate Code" << std::endl;
    code_gen->generateCode(createTestSource(), context, std::cout);
    PipelineStagePtr stage = code_gen->compile(CompilerArgs());

    if(stage)
      return 0;
    else
      return -1;
  }

}



int main(){

  iotdb::CodeGenTestCases();

  if(iotdb::CodeGeneratorTest()){
    std::cerr << "Test Failed!" << std::endl;
    return -1;
  }

  if(!iotdb::CodeGenTest()){
    std::cout << "Test Passed!" << std::endl;
    return 0;
    }else{
    std::cerr << "Test Failed!" << std::endl;
    return -1;
    }

  return 0;
}


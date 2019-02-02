/*
 * PipelineStage.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <CodeGen/PipelineStage.hpp>
#include <iostream>
#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <Util/ErrorHandling.hpp>

namespace iotdb {

bool PipelineStage::execute(TupleBuffer buf, WindowState* state) {
  std::cout << "Execute a Pipeline Stage!" << std::endl;
  return true;
}

PipelineStage::~PipelineStage(){

}



class CPipelineStage : public PipelineStage{
public:
 CPipelineStage(CompiledCCodePtr compiled_code);
 CPipelineStage(const CPipelineStage&);

 const PipelineStagePtr copy() const override;

protected:

 TupleBuffer execute_impl(std::vector<TupleBuffer*>& input_buffers) override final;
 virtual uint32_t callCFunction(TupleBuffer** tuple_buffers, WindowState* state, TupleBuffer* result_buffer);

 CompiledCCodePtr compiled_code_;
};

CPipelineStage::CPipelineStage(CompiledCCodePtr compiled_code)
  : compiled_code_(compiled_code){

}

CPipelineStage::CPipelineStage(const CPipelineStage& other){
  /* consider deep copying this! */
  this->compiled_code_ = other.compiled_code_;
}

const PipelineStagePtr CPipelineStage::copy() const{
    return PipelineStagePtr(new CPipelineStage(*this));
}

TupleBuffer CPipelineStage::execute_impl(std::vector<TupleBuffer*>& input_buffers){

  WindowState* state;
  TupleBuffer result={nullptr,0,0,0};

  uint32_t ret = callCFunction(input_buffers.data(), state, &result);
  if(!ret)
    IOTDB_FATAL_ERROR("Execution of Compled PipelineStage Failed!");
  return result;
}

typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*);

uint32_t CPipelineStage::callCFunction(TupleBuffer** tuple_buffers, WindowState* state, TupleBuffer* result_buffer){
  return (*compiled_code_->getFunctionPointer<SharedCLibPipelineQueryPtr>(
      "compiled_query"))(tuple_buffers,state, result_buffer);

}

PipelineStagePtr createPipelineStage(const CompiledCCodePtr compiled_code){
   return PipelineStagePtr(new CPipelineStage(compiled_code));
}


}

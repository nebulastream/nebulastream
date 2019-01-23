/*
 * PipelineStage.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <CodeGen/PipelineStage.hpp>
#include <iostream>
#include <CodeGen/C_CodeGen/CodeCompiler.hpp>

namespace iotdb {

bool PipelineStage::execute(TupleBuffer buf) {
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

 TupleBuffer execute_impl() override final;
 virtual TupleBuffer callCFunction(TupleBuffer** c_tables);

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

TupleBuffer CPipelineStage::execute_impl(){


}

typedef TupleBuffer (*SharedCLibPipelineQueryPtr)(TupleBuffer**);

TupleBuffer CPipelineStage::callCFunction(TupleBuffer** tuple_buffers){
  return (*compiled_code_->getFunctionPointer<SharedCLibPipelineQueryPtr>(
      "compiled_query"))(tuple_buffers);

}

PipelineStagePtr createPipelineStage(const CompiledCCodePtr compiled_code){
   return PipelineStagePtr(new CPipelineStage(compiled_code));
}


}

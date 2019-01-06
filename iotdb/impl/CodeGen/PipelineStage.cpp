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

 const PipelineStagePtr copy() const override;

protected:
 CPipelineStage(const CPipelineStage& other);

 TupleBuffer execute_impl() override final;
 virtual TupleBuffer callCFunction(TupleBuffer** c_tables);

 CompiledCCodePtr compiled_code_;
};

TupleBuffer CPipelineStage::execute_impl(){


}

typedef TupleBuffer (*SharedCLibPipelineQueryPtr)(TupleBuffer**);

TupleBuffer CPipelineStage::callCFunction(TupleBuffer** tuple_buffers){
  return (*compiled_code_->getFunctionPointer<SharedCLibPipelineQueryPtr>(
      "compiled_query"))(tuple_buffers);

}


}

/*
 * PipelineStage.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <CodeGen/PipelineStage.hpp>
#include <iostream>

namespace iotdb {

bool PipelineStage::execute(TupleBuffer buf) {
  std::cout << "Execute a Pipeline Stage!" << std::endl;
  return true;
}
}

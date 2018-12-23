/*
 * PipelineStage.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <core/PipelineStage.hpp>
#include <iostream>

bool PipelineStage::execute(TupleBuffer buf) {
  std::cout << "Execute a Pipeline Stage!" << std::endl;
  return true;
}

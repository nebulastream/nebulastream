#include <cstddef>
#include <iomanip>
#include <iostream>
#include <API/InputQuery.hpp>
#include <Operators/AggregateOperator.hpp>
#include <Operators/GenerateOperator.hpp>
#include <Operators/GroupByOperator.hpp>
#include <Operators/JoinOperator.hpp>
#include <Operators/MapOperator.hpp>
#include <Operators/KeyOperator.hpp>
#include <Operators/OrderByOperator.hpp>
#include <Operators/PrintOperator.hpp>
#include <Operators/ReadOperator.hpp>
#include <Operators/ReadWindowOperator.hpp>
#include <Operators/TableReadOperator.hpp>
#include <Operators/WindowOperator.hpp>
#include <Operators/WriteOperator.hpp>
#include <Operators/InputOperator.hpp>
#include <Operators/Operator.hpp>
#include <Operators/FilterOperator.hpp>
namespace iotdb{
//#include "code_generation/code_generator.h"
//#include "operator/aggregate_operator.h"
//#include "operator/filter_operator.h"
//#include "operator/generate_operator.h"
//#include "operator/group_by_operator.h"
//#include "operator/input_operator.h"
//#include "operator/join_operator.h"
//#include "operator/key_operator.h"
//#include "operator/map_operator.h"
//#include "operator/order_by_operator.h"
//#include "operator/print_operator.h"
//#include "operator/read_operator.h"
//#include "operator/read_window_operator.h"
//#include "operator/table_read_operator.h"
//#include "operator/window_operator.h"
//#include "operator/write_operator.h"
//#include "api/source.h"

InputQuery::InputQuery(Config& config, Schema& schema, Source& source):
		schema(schema), config(config), source(source)
	{
		current = NULL;
	}

InputQuery::~InputQuery() {}

InputQueryPtr InputQuery::create(Config& config, Schema& schema, Source& source)
{
	InputQueryPtr q = std::make_shared<InputQuery>(config, schema, source);
	InputType type = source.getType();
	string path = source.getPath();
	q->current = new InputOperator(type, path, new ReadOperator(schema));
	q->root = q->current;
	return q;
}


void InputQuery::execute() {

}

/*
 * Relational Operators
 */
InputQuery& InputQuery::filter(Predicate &&predicate) {
  Operator* newOp = new FilterOperator(predicate, current);
  if (current)
    newOp->rightChild = current;
  root = newOp;
  current = newOp;
  return *this;
}

InputQuery &InputQuery::groupBy(std::string fieldId) {
  Operator *newOp = new GroupByOperator(schema.get(fieldId), current);
  if (current)
    newOp->rightChild = current;
  root = newOp;
  current = newOp;
  return *this;
}

InputQuery &InputQuery::orderBy(std::string& fieldId, std::string& sortedness) {
  Operator *newOp = new OrderByOperator(schema.get(fieldId), current);
  if (current)
    newOp->rightChild = current;
  root = newOp;
  current = newOp;
  return *this;
}

InputQuery &InputQuery::aggregate(Aggregation &&aggregation) {
  // TODO: diff between window and batch
  Operator *newOp = new ReadWindowOperator(schema, new AggregateOperator(aggregation, current));
  if (current)
    newOp->rightChild = current;
  root = newOp;
  current = newOp;
  return *this;
}

InputQuery &InputQuery::join(Operator* op, JoinPredicate &&joinPred) {
  Operator *newOp = new JoinOperator(joinPred, current, op);
  root = newOp;
  current = newOp;
  return *this;
}

/*
 * Streaming Operators
 */
InputQuery &InputQuery::window(Window &&window) {
  Operator *newOp = new WindowOperator(window.assigner, window.trigger, current);
  if (current)
    newOp->rightChild = current;
  root = newOp;
  current = newOp;
  return *this;
}

InputQuery &InputQuery::keyBy(std::string& fieldId) {
  Operator *newOp = new KeyOperator(schema.get(fieldId), new GroupByOperator(schema.get(fieldId), current));
  if (current)
    newOp->rightChild = current;
  root = newOp;
  current = newOp;
  return *this;
}

InputQuery &InputQuery::map(Mapper &&mapper) {
  Operator *newOp = new MapOperator(mapper, current);
  if (current)
    newOp->rightChild = current;
  root = newOp;
  current = newOp;
  return *this;
}

/*
 * Input Operators
 */
InputQuery &InputQuery::input(InputType type, std::string path) {
  assert(0);
  Operator *newOp = new InputOperator(type, path, current);
  root = newOp;
  current = newOp;
  return *this;
}


InputQuery &InputQuery::write(std::string file_name) {
  Operator *newOp = new WriteOperator(current, file_name);
  if (current)
    newOp->rightChild = current;
  root = newOp;
  current = newOp;
  return *this;
}

InputQuery &InputQuery::print() {
  Operator *newOp = new PrintOperator(current);
  if (current)
    newOp->rightChild = current;
  root = newOp;
  current = newOp;
  return *this;
}

void InputQuery::printInputQueryPlan(Operator *p, int indent) {
  // Taken from https://stackoverflow.com/questions/13484943/print-a-binary-tree-in-a-pretty-way

  if (p != NULL) {
    if (p->rightChild) {
      printInputQueryPlan(p->rightChild, indent + 4);
    }
    if (indent) {
      std::cout << std::setw(indent) << ' ';
    }
    if (p->rightChild) {
      std::cout << "  /\n" << std::setw(indent) << ' ';
    }
    std::cout << p->to_string() << "\n ";
    if (p->leftChild) {
      std::cout << std::setw(indent) << ' ' << "/ \\\n";
      printInputQueryPlan(p->leftChild, indent + 4);
    }
  }
}

InputQuery &InputQuery::printInputQueryPlan() {
  std::cout << "InputQuery Plan " << std::string(69, '-') << std::endl;

  if (root == NULL) {
    printf("No root node; cant print InputQueryplan\n");
  } else {
    printInputQueryPlan(current, 0);
    printf("\n");
  }
  return *this;
}

//InputQuery &InputQuery::printPipelinePermutations() {
//  std::cout << "InputQuery Plan - Permutations of the longest Pipeline " << std::string(30, '-') << std::endl;
//
//  /* Produce Code Generator */
//  CodeGenerator code_generator = CodeGenerator(config, schema);
//  InputQueryContext InputQuery_context = InputQueryContext(schema);
//  code_generator.addInputQueryContext(InputQuery_context);
//  current->produce(code_generator);
//
//  /* Choose longest Pipeline and get enumerator. */
//  CMethod::Builder longest_pipeline = code_generator.pipeline(code_generator.longestPipeline());
//  CMethod::PipelineEnumerator enumerator = CMethod::PipelineEnumerator(longest_pipeline);
//
//  /* Print all permutations. */
//  enumerator.printPermutations();
//  std::cout << std::endl;
//
//  return *this;
//}
}

#include <cstddef>
#include <iomanip>
#include <iostream>
#include <API/InputQuery.hpp>
#include <Operators/Operator.hpp>


#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <Util/ErrorHandling.hpp>

namespace iotdb{

InputQuery createQueryFromCodeString(const std::string& query_code_snippet){

  /* translate user code to a shared library, load and execute function, then return query object */

  std::stringstream code;
  code << "#include <API/InputQuery.hpp>" << std::endl;
  code << "#include <API/Config.hpp>" << std::endl;
  code << "#include <API/Schema.hpp>" << std::endl;
  code << "namespace iotdb{" << std::endl;
  code << "InputQuery createQuery(){" << std::endl;
  code << query_code_snippet << std::endl;
  code << "}" << std::endl;
  code << "}" << std::endl;
  CCodeCompiler compiler;
  CompiledCCodePtr compiled_code = compiler.compile(code.str());
  if(!code){
    IOTDB_FATAL_ERROR("Compilation of query code failed! Code: " << code.str());
  }

  typedef InputQuery (*CreateQueryFunctionPtr)();
  CreateQueryFunctionPtr func = compiled_code->getFunctionPointer<CreateQueryFunctionPtr>("_ZN5iotdb11createQueryEv");
  if(!func){
    IOTDB_FATAL_ERROR("Error retrieving function! Symbol not found!");
  }
  /* call loaded function to create query object */
  InputQuery query((*func)());

  return query;
}


InputQuery::InputQuery(const Config& config, const DataSourcePtr& source):
		config(config), source(source), root()
	{

	}

/* TODO: perform deep copy of operator graph */
InputQuery::InputQuery(const InputQuery& query)
   : config(query.config), source(query.source), root(query.root->copy()){

}

InputQuery::~InputQuery() {}

InputQuery InputQuery::create(const Config& config, const DataSourcePtr& source)
{
  InputQuery q(config, source);
  OperatorPtr op=createScan(source);
  q.root=op;
  return q;
}


void InputQuery::execute() {

}

/*
 * Relational Operators
 */
InputQuery& InputQuery::filter(PredicatePtr predicate) {
//  Operator* newOp = new FilterOperator(predicate, current);
//  if (current)
//    newOp->rightChild = current;
//  root = newOp;
//  current = newOp;
  return *this;
}

/*
InputQuery &InputQuery::groupBy(const AttributeFieldPtr& field) {
//  Operator *newOp = new GroupByOperator(field, current);
//  if (current)
//    newOp->rightChild = current;
//  root = newOp;
//  current = newOp;
  return *this;
}

InputQuery &InputQuery::groupBy(const VecAttributeFieldPtr& field){
//  Operator *newOp = new GroupByOperator(schema.get(fieldId), current);
//  if (current)
//    newOp->rightChild = current;
//  root = newOp;
//  current = newOp;
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
*/

/*
 * Streaming Operators
 */
/*
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
*/

/*
 * Input Operators
 */

/*
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
*/

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

const std::vector<OperatorPtr> getChildNodes(const OperatorPtr& op);

const std::vector<OperatorPtr> getChildNodes(const OperatorPtr& op){
  std::vector<OperatorPtr> result;
  if(!op){
      return result;
  }else {
    return op->childs;
  }
}


void InputQuery::printInputQueryPlan(const OperatorPtr& p, int indent) {
  if (p) {
      if (indent) {
        std::cout << std::setw(indent) << ' ';
      }
      if (p) {
        std::cout << "  \n" << std::setw(indent) << ' ';
      }
      std::cout << p->toString() << "\n ";
    std::vector<OperatorPtr> childs = getChildNodes(p);

    for(const OperatorPtr& op : childs){
        if (op) {
          printInputQueryPlan(op, indent + 4);
        }
    }
  }
}


InputQuery &InputQuery::printInputQueryPlan() {
  std::cout << "InputQuery Plan " << std::string(50, '-') << std::endl;

  if (!root) {
    printf("No root node; cannot print InputQueryplan\n");
  } else {
    printInputQueryPlan(root, 0);
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

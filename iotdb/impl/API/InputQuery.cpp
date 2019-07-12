#include <cstddef>
#include <iomanip>
#include <iostream>

#include <API/InputQuery.hpp>
#include <Operators/Operator.hpp>
#include <Runtime/DataSink.hpp>

#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <API/UserAPIExpression.hpp>

namespace iotdb {

    const OperatorPtr recursiveCopy(OperatorPtr ptr) {
        OperatorPtr operatorPtr = ptr->copy();
        std::vector<OperatorPtr> children = ptr->childs;

        for (u_int i = 0; i < children.size(); i++) {
            const OperatorPtr copiedChild = recursiveCopy(children[i]);
            if (!copiedChild) {
                return nullptr;
            }
            operatorPtr->childs.push_back(copiedChild);
        }

        return operatorPtr;
    }

/* some utility functions to encapsulate node handling to be
 * independent of implementation of query graph */
    const std::vector<OperatorPtr> getChildNodes(const OperatorPtr &op);

    void addChild(const OperatorPtr &op_parent, const OperatorPtr &op_child);

    const InputQuery createQueryFromCodeString(const std::string &query_code_snippet) {

        try {
            /* translate user code to a shared library, load and execute function, then return query object */

            std::stringstream code;
            code << "#include <API/InputQuery.hpp>" << std::endl;
            code << "#include <API/Config.hpp>" << std::endl;
            code << "#include <API/Schema.hpp>" << std::endl;
            code << "#include <Runtime/DataSource.hpp>" << std::endl;
            code << "namespace iotdb{" << std::endl;
            code << "InputQuery createQuery(){" << std::endl;
            code << query_code_snippet << std::endl;
            code << "}" << std::endl;
            code << "}" << std::endl;
            CCodeCompiler compiler;
            CompiledCCodePtr compiled_code = compiler.compile(code.str());
            if (!code) {
                IOTDB_FATAL_ERROR("Compilation of query code failed! Code: " << code.str());
            }

            typedef InputQuery (*CreateQueryFunctionPtr)();
            CreateQueryFunctionPtr func = compiled_code->getFunctionPointer<CreateQueryFunctionPtr>(
                    "_ZN5iotdb11createQueryEv");
            if (!func) {
                IOTDB_FATAL_ERROR("Error retrieving function! Symbol not found!");
            }
            /* call loaded function to create query object */
            InputQuery query((*func)());
            return query;

        } catch (...) {
            std::cout << "Exception occurred while computing the user query";
            throw "Failed to create the query from input code string";
        }
    }

// const OperatorPtr recursiveOperatorCopy(const OperatorPtr& op){

// return OperatorPtr();
//}

// const OperatorPtr copyOperatorTree(InputQuery sub_query){
// return sub_query
//}

    InputQuery::InputQuery(const DataSourcePtr &source) : source(source),
                                                                                root() {}

/* TODO: perform deep copy of operator graph */
    InputQuery::InputQuery(const InputQuery &query) : source(query.source),
                                                      root(recursiveCopy(query.root)) {
    }

    InputQuery &InputQuery::operator=(const InputQuery &query) {
        if (&query != this) {
            this->source = query.source;
            this->root = recursiveCopy(query.root);
        }
        return *this;
    }


    InputQuery::~InputQuery() {}

    InputQuery InputQuery::create(const DataSourcePtr &source) {
        InputQuery q(source);
        OperatorPtr op = createSourceOperator(source);
        q.root = op;
        return q;
    }

/*
 * Relational Operators
 */
    InputQuery &InputQuery::filter(Predicate predicate) {
        PredicatePtr pred = std::dynamic_pointer_cast<Predicate>(
                predicate.copy()
        );
        return filter(pred);
    }

    InputQuery &InputQuery::filter(const PredicatePtr &predicate) {
        OperatorPtr op = createFilterOperator(predicate);
        //op->parent = root;
        addChild(op, root);
        root = op;
        return *this;
    }

    InputQuery &InputQuery::groupBy(const Attributes &grouping_fields, const AggregationPtr &aggr_spec) {
        OperatorPtr op;
        //  = createAggregationOperator(AggregationSpec
        //  {grouping_fields, aggr_spec});
        addChild(op, root);
        root = op;
        return *this;
    }

    InputQuery &InputQuery::orderBy(const Sort &fields) {
        OperatorPtr op = createSortOperator(fields);
        addChild(op, root);
        root = op;
        return *this;
    }

    InputQuery &InputQuery::join(const InputQuery &sub_query, const JoinPredicatePtr &joinPred) {
        OperatorPtr op = createJoinOperator(joinPred);
        addChild(op, root);
        // addChild(op, copyRecursive(sub_query,sub_query.root));
        addChild(op, sub_query.root);
        root = op;
        return *this;
    }

// streaming operators
    InputQuery &InputQuery::window(const WindowPtr &window) {
        OperatorPtr op = createWindowOperator(window);
        addChild(op, root);
        root = op;
        return *this;
    }

    InputQuery &InputQuery::keyBy(const Attributes &fields) {
        OperatorPtr op = createKeyByOperator(fields);
        addChild(op, root);
        root = op;
        return *this;
    }

    InputQuery &InputQuery::map(const MapperPtr &mapper) {
        OperatorPtr op = createMapOperator(mapper);
        addChild(op, root);
        root = op;
        return *this;
    }

// output operators
    InputQuery &InputQuery::writeToFile(const std::string &file_name) {
        OperatorPtr op = createSinkOperator(createBinaryFileSink(file_name));
        addChild(op, root);
        root = op;
        return *this;
    }

    InputQuery &InputQuery::print(std::ostream &out) {
        OperatorPtr op = createSinkOperator(createPrintSink(out));
        addChild(op, root);
        root = op;
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

// class Operator;
// typedef std::shared_ptr<Operator> OperatorPtr;


    void addChild(const OperatorPtr &op_parent, const OperatorPtr &op_child) {
        if (op_parent && op_child) {
            op_parent->childs.push_back(op_child);
            op_child->parent = op_parent;
        }
    }


// InputQuery &InputQuery::printPipelinePermutations() {
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
} // namespace iotdb

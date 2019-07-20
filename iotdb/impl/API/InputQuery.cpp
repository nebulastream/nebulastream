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
            code << "#include <API/InputQuery.hpp>" << std::endl;
            code << "#include <API/Environment.hpp>" << std::endl;
            code << "#include <API/UserAPIExpression.hpp>" << std::endl;
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

    InputQuery::InputQuery(Stream& source_stream) : source_stream(source_stream),
                                                              root() {}

/* TODO: perform deep copy of operator graph */
    InputQuery::InputQuery(const InputQuery &query) : source_stream(query.source_stream),
                                                      root(recursiveCopy(query.root)) {
    }

    InputQuery &InputQuery::operator=(const InputQuery &query) {
        if (&query != this) {
            this->source_stream = query.source_stream;
            this->root = recursiveCopy(query.root);
        }
        return *this;
    }


    InputQuery::~InputQuery() {}


    InputQuery InputQuery::from(Stream& stream) {
        InputQuery q(stream);
        OperatorPtr op = createSourceOperator(createSchemaTestDataSource(stream.getSchema()));
        q.root = op;
        return q;
    }

    /*
    * Relational Operators
    */

    InputQuery &InputQuery::select(const Field &field) {
        Field* f1;
        const Field& f2 = field;
        IOTDB_NOT_IMPLEMENTED
        return *this;
    }

    InputQuery &InputQuery::select(const Field &field1, const Field &field2) {
        IOTDB_NOT_IMPLEMENTED
        return *this;
    }

    InputQuery &InputQuery::filter(const UserAPIExpression& predicate) {
        PredicatePtr pred = createPredicate(predicate);
        OperatorPtr op = createFilterOperator(pred);
        addChild(op, root);
        root = op;
        return *this;
    }

    InputQuery &InputQuery::map(const Field &field, const Predicate predicate) {
        IOTDB_NOT_IMPLEMENTED
    }

    InputQuery &InputQuery::combine(const iotdb::InputQuery &sub_query) {
        IOTDB_NOT_IMPLEMENTED
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

    InputQuery &InputQuery::window(const iotdb::WindowTypePtr windowType, const iotdb::WindowAggregation &aggregation) {
        IOTDB_NOT_IMPLEMENTED
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


    void addChild(const OperatorPtr &op_parent, const OperatorPtr &op_child) {
        if (op_parent && op_child) {
            op_parent->childs.push_back(op_child);
            op_child->parent = op_parent;
        }
    }

} // namespace iotdb

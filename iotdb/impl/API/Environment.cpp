#include <cstddef>
#include <iomanip>
#include <iostream>

#include <API/Environment.hpp>
#include <API/InputQuery.hpp>
#include <Operators/Operator.hpp>

#include <QueryCompiler/C_CodeGenerator/CodeCompiler.hpp>
#include <QueryCompiler/CodeGenerator.hpp>

namespace iotdb {
    Environment::Environment(const Config& config) : config(config) {}
    Environment Environment::create(const Config& config) { return Environment(config); }
    void Environment::executeQuery(const InputQuery& inputQuery) {

        CodeGeneratorPtr code_gen = createCodeGenerator();
        PipelineContextPtr context = createPipelineContext();
        inputQuery.getRoot()->produce(code_gen, context, std::cout);


        PipelineStagePtr stage = code_gen->compile(CompilerArgs());
    }

    const std::vector<OperatorPtr> getChildNodes(const OperatorPtr &op) {
        std::vector<OperatorPtr> result;
        if (!op) {
            return result;
        } else {
            return op->childs;
        }
    }

    void Environment::printInputQueryPlan(const OperatorPtr &p, int indent) {
        if (p) {
            if (indent) {
                std::cout << std::setw(indent) << ' ';
            }
            if (p) {
                std::cout << "  \n" << std::setw(indent) << ' ';
            }
            std::cout << p->toString() << "\n ";
            std::vector<OperatorPtr> childs = getChildNodes(p);

            for (const OperatorPtr &op : childs) {
                if (op) {
                    printInputQueryPlan(op, indent + 4);
                }
            }
        }
    }

    void Environment::printInputQueryPlan(const InputQuery& query) {
        std::cout << "InputQuery Plan " << std::string(50, '-') << std::endl;

        if (!query.getRoot()) {
            printf("No root node; cannot print InputQueryplan\n");
        } else {
            printInputQueryPlan(query.getRoot(), 0);
            printf("\n");
        }
    }

} // namespace iotdb

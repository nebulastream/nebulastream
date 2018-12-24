
#include <memory>
#include <vector>

namespace iotdb{

    class Operator;
    typedef std::unique_ptr<Operator> OperatorPtr;

    class CodeGenerator;
    typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

    class QueryExecutionPlan;
    typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

    enum OperatorType{TABLE_SCAN, STREAM_SCAN, SELECTION, PROJECTION, JOIN, AGGREGATION, CUSTOM};

    class Operator{

        virtual void produce(CodeGeneratorPtr codegen, QueryExecutionPlanPtr qep) = 0;
        virtual void consume(CodeGeneratorPtr codegen, QueryExecutionPlanPtr qep) = 0;

        virtual ~Operator();
    private:
        OperatorType type;
        std::vector<OperatorPtr> childs;
    };

}

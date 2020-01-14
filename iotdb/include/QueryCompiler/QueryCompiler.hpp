#ifndef INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
#define INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
#include <memory>
namespace NES {

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class QueryCompiler;
typedef std::shared_ptr<QueryCompiler> QueryCompilerPtr;

/**
 * @brief The query compiler compiles physical query plans to an executable query plan
 */
class QueryCompiler {
 public:
  QueryCompiler(QueryCompiler* queryCompiler);
  ~QueryCompiler() = default;
  /**
   * Creates a new query compiler
   * @param codeGenerator
   * @return
   */
  static QueryCompilerPtr create();
  /**
   * @brief compiles a queryplan to a executable query plan
   * @param queryPlan
   * @return
   */
  QueryExecutionPlanPtr compile(OperatorPtr queryPlan);

 private:
  QueryCompiler();
};

QueryCompilerPtr createDefaultQueryCompiler();

}

#endif //INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_

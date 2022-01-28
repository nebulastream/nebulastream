/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_INCLUDE_OPTIMIZER_QUERYMERGER_SIGNATURE_QUERYSIGNATURE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYMERGER_SIGNATURE_QUERYSIGNATURE_HPP_

#include <map>
#include <memory>
#include <vector>
#include <z3++.h>

namespace z3 {
class expr;
using ExprPtr = std::shared_ptr<expr>;
}// namespace z3

namespace NES::Optimizer {

class QuerySignature;
using QuerySignaturePtr = std::shared_ptr<QuerySignature>;

/**
 * @brief This class is responsible for holding the signature of a query plan. The signature of a query plan is used for comparing
 * it with the signature of another query plan to establish equality relationship among them.
 *
 * A signature is represented by First-Order Logic (FOL) formula derived for the operators contained in the query plan.
 * We prepare a conjunctive normal form (CNF) of conditions occurring in the query plan and a CNF of columns or projections
 * expected as result.
 *
 * For Example:
 *      For a logical stream "cars" with schema "Id, Color, Speed".
 *
 *  1.) Given a query Q1: Query::from("cars").map(attr("speed") = 100).filter(attr("color") == 'RED').sink(Print());
 *      The query plan signature (QPSig) is given by : (conditions:(stream="cars" and cars.color=='RED'); columns:(cars.Id, cars.Color, cars.Speed=100); windows:())
 *
 *  2.) Given a query Q2: Query::from("cars").map(attr("speed") = attr("speed")*100).filter(attr("speed") > 100 ).filter(attr("color") == 'RED').sink(Print());
 *      The query plan signature (QPSig) is given by : (conditions:(stream="cars" and cars.speed*100>100 and cars.color=='RED'); columns:(cars.Id, cars.Color, cars.speed=speed*100); windows:())
 *
 *  3.) Given a query Q3: Query::from("cars").filter(attr("speed") > 100 ).map(attr("speed") = attr("speed")*100).filter(attr("color") == 'RED').sink(Print());
 *      The query plan signature (QPSig) is given by : (conditions:(stream="cars" and cars.speed>100 and cars.color=='RED'); columns:(cars.Id, cars.Color, cars.speed=speed*100); windows:())
 *
 *  4.) Given a query Q4: Query::from("cars").filter(attr("color") == 'RED').map(attr("speed") = attr("speed")*100).filter(attr("speed") +100 > 200 ).sink(Print());
 *      The query plan signature (QPSig) is given by : (conditions:(stream="cars" and cars.color=='RED' and (cars.speed*100)+100>200); columns:(cars.Id, cars.Color, cars.speed=cars.speed*100); windows:())
 *
 *  5.) Given a query Q5: Query::from("cars").windowByKey("Color",SlidingWindow::of(EventTime("ts")),Seconds(10),Seconds(10)),Count().as("total")).sink(Print());
 *      The query plan signature (QPSig) is given by : (conditions:(stream="cars"); columns:(end, start, cars.Color, Count()); windows:("type": window-key = 'type' and
 *      window-time-key = 'ts' and window-time-size = 10000 and window-time-slide = 10000))
 */
class QuerySignature {
  public:
    /**
     * @brief Create instance of Query plan signature
     * @param conditions: the predicates involved in the query
     * @param columns: the predicates involving columns to be extracted
     * @param schemaFieldToExprMaps: map of tuple schemas expected at the operator
     * @param windowsExpressions: the map containing window expressions
     * @return Shared instance of the query plan signature.
     */
    static QuerySignaturePtr create(z3::ExprPtr&& conditions,
                                    std::vector<std::string>&& columns,
                                    std::vector<std::map<std::string, z3::ExprPtr>>&& schemaFieldToExprMaps,
                                    std::map<std::string, z3::ExprPtr>&& windowsExpressions);

    /**
     * @brief Get the conditions
     * @return Condition predicates in CNF form
     */
    z3::ExprPtr getConditions();

    /**
     * @brief Get the column predicates
     * @return map of column name to list of predicates
     */
    const std::vector<std::string>& getColumns();

    /**
     * @brief Get vector of schemas for the operator
     * @return pointer to Operator schemas
     */
    const std::vector<std::map<std::string, z3::ExprPtr>>& getSchemaFieldToExprMaps();

    /**
     * @brief Get the window definitions
     * @return map of window definitions
     */
    const std::map<std::string, z3::ExprPtr>& getWindowsExpressions();

  private:
    QuerySignature(z3::ExprPtr&& conditions,
                   std::vector<std::string>&& columns,
                   std::vector<std::map<std::string, z3::ExprPtr>>&& schemaFieldToExprMaps,
                   std::map<std::string, z3::ExprPtr>&& windowsExpressions);

    z3::ExprPtr conditions;
    std::vector<std::string> columns;
    /**
     * This vector contains a collection of unique tuple schemas this operator can expect
     * For Example:
     * - A Union operator will experience tuples with 2 distinct schemas.
     * - A downstream Map operator to a union will also experience tuples with 2 distinct schemas.
     * - A down stream Union operator to two distinct Union operators will experience 4 distinct schemas (2 from left and 2 from right child).
     * - A down stream Join operator to two distinct Union operator can experience maximum 2 X 2 distinct schemas.
     */
    std::vector<std::map<std::string, z3::ExprPtr>> schemaFieldToExprMaps;
    std::map<std::string, z3::ExprPtr> windowsExpressions;
};
}// namespace NES::Optimizer

#endif  // NES_INCLUDE_OPTIMIZER_QUERYMERGER_SIGNATURE_QUERYSIGNATURE_HPP_

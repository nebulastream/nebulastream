/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_QUERYSIGNATURE_HPP
#define NES_QUERYSIGNATURE_HPP

#include <map>
#include <memory>
#include <vector>

namespace z3 {
class expr;
typedef std::shared_ptr<expr> ExprPtr;
}// namespace z3

namespace NES::Optimizer {
class QuerySignature;
typedef std::shared_ptr<QuerySignature> QuerySignaturePtr;

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
 *      The query plan signature (QPSig) is given by : (conditions:(stream="cars" and color=='RED'); columns:(Id, Color, Speed=100); windows:())
 *
 *  2.) Given a query Q2: Query::from("cars").map(attr("speed") = attr("speed")*100).filter(attr("speed") > 100 ).filter(attr("color") == 'RED').sink(Print());
 *      The query plan signature (QPSig) is given by : (conditions:(stream="cars" and speed*100>100 and color=='RED'); columns:(Id, Color, speed=speed*100); windows:())
 *
 *  3.) Given a query Q3: Query::from("cars").filter(attr("speed") > 100 ).map(attr("speed") = attr("speed")*100).filter(attr("color") == 'RED').sink(Print());
 *      The query plan signature (QPSig) is given by : (conditions:(stream="cars" and speed>100 and color=='RED'); columns:(Id, Color, speed=speed*100); windows:())
 *
 *  4.) Given a query Q4: Query::from("cars").filter(attr("color") == 'RED').map(attr("speed") = attr("speed")*100).filter(attr("speed") +100 > 200 ).sink(Print());
 *      The query plan signature (QPSig) is given by : (conditions:(stream="cars" and color=='RED' and (speed*100)+100>200); columns:(Id, Color, speed=speed*100); windows:())
 *
 *  5.) Given a query Q5: Query::from("cars").windowByKey("Color",SlidingWindow::of(EventTime("ts")),Seconds(10),Seconds(10)),Count().as("total")).sink(Print());
 *      The query plan signature (QPSig) is given by : (conditions:(stream="cars"); columns:(end, start, Color, Count()); windows:("type": window-key = 'type' and
 *      window-time-key = 'ts' and window-time-size = 10000 and window-time-slide = 10000))
 */
class QuerySignature {
  public:
    /**
     * @brief Create instance of Query plan signature
     * @param conditions: the predicates involved in the query
     * @param columns: the predicates involving columns to be extracted
     * @param windowsExpressions: the map containing window expressions
     * @return Shared instance of the query plan signature.
     */
    static QuerySignaturePtr create(z3::ExprPtr conditions, std::map<std::string, std::vector<z3::ExprPtr>> columns,
                                    std::map<std::string, z3::ExprPtr> windowsExpressions);

    /**
     * @brief Get the conditions
     * @return Condition predicates in CNF form
     */
    z3::ExprPtr getConditions();

    /**
     * @brief Get the column predicates
     * @return map of column name to list of predicates
     */
    std::map<std::string, std::vector<z3::ExprPtr>> getColumns();

    /**
     * @brief Get the window definitions
     * @return map of window definitions
     */
    std::map<std::string, z3::ExprPtr>& getWindowsExpressions();

    /**
     * @brief Validate if this signature is equal to input signature
     * @param other : the signature to be compared against
     * @return true if equal else false
     */
    bool isEqual(QuerySignaturePtr other);

  private:
    QuerySignature(z3::ExprPtr conditions, std::map<std::string, std::vector<z3::ExprPtr>> columns,
                   std::map<std::string, z3::ExprPtr> windowsExpressions);
    z3::ExprPtr conditions;
    std::map<std::string, std::vector<z3::ExprPtr>> columns;
    std::map<std::string, z3::ExprPtr> windowsExpressions;
};
}// namespace NES::Optimizer

#endif//NES_QUERYSIGNATURE_HPP

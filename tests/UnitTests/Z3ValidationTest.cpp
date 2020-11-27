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

#include <gtest/gtest.h>

#include <Util/Logger.hpp>
#include <memory>
#include <z3++.h>

using namespace z3;
namespace NES {
class Z3ValidationTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("Z3ValidationTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup Z3ValidationTest test class.");
    }

    void TearDown() { NES_INFO("Tear down Z3ValidationTest test class."); }

    const uint64_t buffers_managed = 10;
    const uint64_t buffer_size = 4 * 1024;
};

/**
   @brief: Demonstration of how Z3 can be used to prove validity of
   De Morgan's Duality Law: {e not(x and y) <-> (not x) or ( not y) }
*/
TEST_F(Z3ValidationTest, deMorganDualityValidation) {
    NES_INFO("De-Morgan Example");

    // create a context
    context c;
    //Create an instance of the solver
    solver s(c);

    //Define boolean constants x and y
    expr x = c.bool_const("x");
    expr y = c.bool_const("y");

    //Define the expression to evaluate
    expr expression = (!(x && y)) == (!x || !y);

    NES_INFO("Expression: " << expression);

    // adding the negation of the expression as a constraint.
    // We try to prove that inverse of this expression is valid (which is obviously false).
    s.add(!expression);

    NES_INFO("Content of the solver: " << s);
    NES_INFO("Statement folding inside z3 solver: " << s.to_smt2());
    ASSERT_EQ(s.check(), unsat);
}

/**
   @brief Validate for <tt>x > 1 and y > 1 that y + x > 1 </tt>.
*/
TEST_F(Z3ValidationTest, evaluateValidBinomialEquation) {

    // create a context
    std::shared_ptr<context> c = std::make_shared<context>();
    //Create an instance of the solver
    solver s(*c);

    //Define int constants
    expr x = c->int_const("x");
    expr y = c->int_const("y");

    //Add equations to
    s.add(x > 1);
    s.add(y > 1);
    s.add(x + y > 1);

    //Assert
    ASSERT_EQ(s.check(), sat);
}

/**
   @brief Validate for <tt>x > 1 and y > 1 that y + x > 1 </tt>.
*/
TEST_F(Z3ValidationTest, evaluateValidBinssomialEquation) {

    // create a context
    context c;
    //Create an instance of the solver
    solver s(c);

    //Define int constants
    sort sort = c.string_sort();
    expr x = c.constant(c.str_symbol("x"), sort);
    expr y = c.int_const("Y");

    expr valX = c.string_val("x");
    expr valY = c.int_val(10);

    //Add equations
    auto xEqualValX = to_expr(c, Z3_mk_eq(c, x, valX));
    auto yEqualValY = to_expr(c, Z3_mk_ge(c, y, valY));
    Z3_ast arr[] = {xEqualValX, yEqualValY};

    auto expt = to_expr(c, Z3_mk_and(c, 2, arr));
    s.add(expt);

    //Assert
    ASSERT_EQ(s.check(), sat);
}

/**
   @brief Validate for <tt>x > 1 and y > 1 that y + x < 1 </tt>.
*/
TEST_F(Z3ValidationTest, evaluateInvalidBinomialEquation) {

    // create a context
    context c;
    //Create an instance of the solver
    solver s(c);

    //Define int constants
    expr x = c.int_const("x");
    expr y = c.int_const("y");

    //Add equations
    s.add(x > 1);
    s.add(y > 1);
    s.add(x + y < 1);
    NES_INFO(s);

    //Assert
    ASSERT_EQ(s.check(), unsat);

    //Same equation written using api
    s.reset();
    auto one = c.int_val(1);
    auto xPlusOne = to_expr(c, x + 1);
    auto xLessThanOne = to_expr(c, Z3_mk_gt(c, x, one));
    auto yLessThanOne = to_expr(c, Z3_mk_gt(c, y, one));
    Z3_ast args[] = {x, y};
    auto xPlusY = to_expr(c, Z3_mk_add(c, 2, args));
    auto xPlusYLessThanOne = to_expr(c, Z3_mk_lt(c, xPlusY, one));

    s.add(xLessThanOne);
    NES_INFO(s);
    s.add(yLessThanOne);
    s.add(xPlusYLessThanOne);
    //Assert
    ASSERT_EQ(s.check(), unsat);
}

/**
   @brief Validate for <tt>(x==y and y==x) == (y==x and x==y) </tt>.
*/
TEST_F(Z3ValidationTest, equalityChecks) {
    context c;
    expr x = c.int_const("x");
    expr y = c.int_const("y");
    solver s(c);

    //We prove that equation (x==y and y==x) != (y==x and x==y) is unsatisfiable
    s.add(!((x == y && y == x) == (y == x && x == y)));
    ASSERT_EQ(s.check(), unsat);
}

/**
   @brief Validate that <tt>(x>=y) != (y>=x) </tt>.
*/
TEST_F(Z3ValidationTest, unequalityChecks) {
    context c;
    expr x = c.int_const("x");
    expr y = c.int_const("y");
    solver s(c);

    //We prove that equation (x>=y) != (y>=x) is satisfiable
    s.add(!((x >= y) == (y >= x)));
    ASSERT_EQ(s.check(), sat);

    //Two conditions are equal that is proved by making sure
    //that not equality of these conditions is unsatisfiable
    //
    //However, to prove that two conditions are not equal
    //we need to prove that equality of these conditions is unsatisfiable
    //
    //However, what to do when we have partially equal expressions?
    //This means for certain values they are equal and for certain they are not equal.
    //Example: x>=y and y>=x .... for x==y they are equal but for x!=y they are not.
    //This means the equality and inequality both are satisfiable.

    s.reset();
    expr stream = c.constant("stream", c.string_sort());
    expr streamVal = c.string_val("car");
    expr value50 = c.int_val("50");
    expr value40 = c.int_val("40");

    s.add(!((stream == streamVal) == (stream == streamVal)) && !(value40 == value50));

    NES_INFO("Chk that " << s.check());
    NES_INFO(s.get_model());

    //(and (< (* (* value 40) 40) 40) (< (* value 40) 40) (= streamName "car"))


    expr value = c.int_const("value");

    s.reset();
    s.add(((value * 40) < 40 && (value * 80) < 40) != ((value * 40) < 40 && (value * 80) < 40));
    NES_INFO(s.check());
    NES_INFO(s.get_model());
}

}// namespace NES
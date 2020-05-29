#define BREAKIFFAILED 1

#include <string>
#include <sstream>
#include <iostream>

#include <API/UserAPIExpression.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <API/Types/DataTypes.hpp>

namespace NES {

int testUserPredicateAPIstdToString(UserAPIExpression &left,
                                    UserAPIExpression &right) {
  std::string predicatetoString = (left == right).toString();
  std::cout << "EQUALS looks like: " << predicatetoString << std::endl;

  return 0;
}

int testUserPredicateAPIcombToString(UserAPIExpression &left,
                                     UserAPIExpression &right) {
  std::string predicatetoString = ((left == right) < left).toString();
  std::cout << "EQUALS looks like: " << predicatetoString << std::endl;

  return 0;
}

}  //end namespace NES

int main() {

  std::cout << std::endl << "---------------------------------------"
            << std::endl << "---------- PredicateTreeTest ----------"
            << std::endl << "---------------------------------------"
            << std::endl << std::endl;
  NES::setupLogging("PredicateTest.log", NES::LOG_DEBUG);

  NES::PredicateItem attNum = NES::PredicateItem(
      createField("field1", NES::BasicType::FLOAT32));
  NES::PredicateItem attChar = NES::PredicateItem(
      createField("field1", NES::BasicType::CHAR));
  NES::PredicateItem valDate = NES::PredicateItem(
      createBasicTypeValue(NES::BasicType::DATE, "1990.01.01"));
  NES::PredicateItem valInt = NES::PredicateItem(
      createBasicTypeValue(NES::BasicType::INT64, "654378"));

  if (testUserPredicateAPIstdToString(attNum, attChar)) {
    std::cout << "ATTRIBUTE-ATTRIBUTE-easy: some tests failed" << std::endl
              << std::endl;
    if (BREAKIFFAILED)
      return 0;
  } else {
    std::cout << "ATTRIBUTE-ATTRIBUTE-easy: all test passed" << std::endl;
  }

  if (testUserPredicateAPIcombToString(attNum, attChar)) {
    std::cout << "ATTRIBUTE-ATTRIBUTE-combined: some tests failed" << std::endl
              << std::endl;
    if (BREAKIFFAILED)
      return 0;
  } else {
    std::cout << "ATTRIBUTE-ATTRIBUTE-combined: all test passed" << std::endl;
  }
  std::cout << std::endl << "<------------ Change Parametertypes ------------->"
            << std::endl << std::endl;

  if (testUserPredicateAPIstdToString(valDate, valInt)) {
    std::cout << "VALUE-VALUE: some tests failed" << std::endl;
  } else {
    std::cout << "VALUE-VALUE: all test passed" << std::endl;
  }

  std::cout << (attNum > valInt).toString() << std::endl;

  return 0;
}

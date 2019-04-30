#define BRACKETMODE 1
#define BREAKIFFAILED 1
#define SHOWGENERATEDCODE 1

#include <string>
#include <sstream>
#include <iostream>

#include <API/UserAPIExpression.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>

#include <Core/DataTypes.hpp>

namespace iotdb{

int testUserPredicateAPIstd(UserAPIExpression &left, UserAPIExpression &right)
{
	
	std::stringstream stream;
	if (BRACKETMODE) stream << "(";
	stream << left.generateCode() << " == " << right.generateCode();
	if (BRACKETMODE) stream << ")";
	
	std::string generatedCode = (left == right).generateCode();
		
	if(generatedCode != (stream.str())){
		std::cout << "EQUALS looks like: " << generatedCode << std::endl;
		std::cout << "but should look like this: " << stream.str() << std::endl;
		std::cout << "EQUALS test failed" << std::endl << std::endl;
		return 1;
	}
	if(SHOWGENERATEDCODE) std::cout << "EQUALS looks like: " << generatedCode << std::endl;
    return 0;
}

} //end namespace iotdb


int main(){

	std::cout << std::endl << "---------------------------------------" 
			  << std::endl << "---------- PredicateTreeTest ----------" 
			  << std::endl << "---------------------------------------" 
			  << std::endl << std::endl;
			  
	iotdb::PredicateItem attNum = iotdb::PredicateItem(createField("field1", iotdb::BasicType::FLOAT32));
	iotdb::PredicateItem attChar = iotdb::PredicateItem(createField("field1", iotdb::BasicType::CHAR));
	iotdb::PredicateItem valDate = iotdb::PredicateItem(createBasicTypeValue(iotdb::BasicType::DATE,"1990.01.01"));
	iotdb::PredicateItem valInt = iotdb::PredicateItem(createBasicTypeValue(iotdb::BasicType::INT64,"654378"));
	
	if(testUserPredicateAPIstd(attNum, attChar)){
		std::cout << "ATTRIBUTE-ATTRIBUTE: some tests failed" << std::endl << std::endl;
		if(BREAKIFFAILED) return 0;
	} else {
		std::cout << "ATTRIBUTE-ATTRIBUTE: all test passed" << std::endl;
	}
	
	std::cout << std::endl 
			  << "<------------ Change Parametertypes ------------->" 
			  << std::endl << std::endl;
	
	if(testUserPredicateAPIstd(valDate, valInt)){
		std::cout << "VALUE-VALUE: some tests failed" << std::endl;
	} else {
		std::cout << "VALUE-VALUE: all test passed" << std::endl;
	}
	
	return 0;
}

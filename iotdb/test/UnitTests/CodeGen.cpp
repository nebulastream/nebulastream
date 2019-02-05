
#include <iostream>
//#include <CodeGen/C_CodeGen/FunctionBuilder.hpp>



namespace iotdb {

int CodeGenTestCases();
int CodeGenTest();

}

int main(){

  iotdb::CodeGenTestCases();

  if(!iotdb::CodeGenTest()){
    std::cout << "Test Passed!" << std::endl;
    return 0;
    }else{
    std::cerr << "Test Failed!" << std::endl;
    return -1;
    }
  return 0;
}


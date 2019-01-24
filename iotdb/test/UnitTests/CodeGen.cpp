
#include <iostream>
//#include <CodeGen/C_CodeGen/FunctionBuilder.hpp>



namespace iotdb {

int CodeGenTest();

}

int main(){

  if(!iotdb::CodeGenTest()){
    std::cout << "Test Passed!" << std::endl;
    return 0;
    }else{
    std::cout << "Test Failed!" << std::endl;
    return -1;
    }
  return 0;
}


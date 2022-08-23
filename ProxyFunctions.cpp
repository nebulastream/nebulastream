namespace NES::Runtime::ProxyFunctions {
#include "/home/pgrulich/projects/nes/nebulastream/nes-execution/src/ClangPlugin/Test.cpp"
int __XClazz__addMember(void * thisPtr,int x){
auto* thisPtr_ =(XClazz*)thisPtr;
return thisPtr_->addMember(x);
}
;
#include "/home/pgrulich/projects/nes/nebulastream/nes-execution/src/ClangPlugin/Test.cpp"
int __addXNes(int x){

return ::addXNes(x);
}
;
#include "/home/pgrulich/projects/nes/nebulastream/nes-execution/src/ClangPlugin/Test.cpp"
int NES__Test__addNamespaceX(int x){

return NES::Test::addNamespaceX(x);
}
;
}
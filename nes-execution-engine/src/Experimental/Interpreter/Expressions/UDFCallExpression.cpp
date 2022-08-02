#include <Experimental/Interpreter/Expressions/UDFCallExpression.hpp>

#include "Experimental/Interpreter/FunctionCall.hpp"
//#include <jni.h>
namespace NES::ExecutionEngine::Experimental::Interpreter {
/*
JavaVM* jvm;
JNIEnv* env;
jmethodID mid;
jclass cls2;
jobject object;
*/

int64_t callUDFProxyFunction(int64_t value) {
  //  jlong param = value;
   // return env->CallLongMethod(object, mid, param);
return value;
}

UDFCallExpression::UDFCallExpression(ExpressionPtr argument) : argument(argument) {

    // Pointer to native interface
    //================== prepare loading of Java VM ============================
   /* JavaVMInitArgs vm_args;                     // Initialization arguments
    JavaVMOption* options = new JavaVMOption[2];// JVM invocation options
    options[0].optionString = (char *) "-Djava.class.path=/home/pgrulich/projects/nes/nautilusbf/launcher/target/launcher.jar";
    options[1].optionString = (char *)"-verbose:class";// where to find java .class
    vm_args.version = JNI_VERSION_1_2;         // minimum Java version
    vm_args.nOptions = 2;                      // number of options
    vm_args.options = options;
    vm_args.ignoreUnrecognized = true;// invalid options make the JVM init fail
    //=============== load and initialize Java VM and JNI interface =============
    jint rc = JNI_CreateJavaVM(&jvm, (void**) &env, &vm_args);// YES !!
    //delete options;    // we then no longer need the initialisation options.
    if (rc != JNI_OK) {
        // TO DO: error processing...
        std::cout << "rc != JNI_OK --> " << rc << std::endl;
        exit(EXIT_FAILURE);
    }
    std::cout << "JVM load succeeded: Version ";
    jint ver = env->GetVersion();
    std::cout << ((ver >> 16) & 0x0f) << "." << (ver & 0x0f) << std::endl;


    // TO DO: add the code that will use JVM <============  (see next steps)

    auto c1 = env->FindClass("Test");
    env->ExceptionDescribe();// try to find the class
    if (c1 == nullptr)
        std::cerr << "ERROR: class not found !" << std::endl;
    jmethodID constructor = env->GetMethodID(c1, "<init>", "()V");
    object = env->NewObject(c1, constructor);
    mid = env->GetMethodID(c1, "execute", "(J)J");
    if (mid == nullptr) {
        std::cerr << "ERROR: method myFilterUdf() not found !" << std::endl;
    } else {
    }
    jint param = 1;
    auto res = env->CallIntMethod(object, mid, param);
    std::cerr << "Result " << res <<  std::endl;
    */
}

Value<> UDFCallExpression::execute(Record& record) {
    auto inputValue = argument->execute(record);
    return FunctionCall<>("callUDFProxyFunction", callUDFProxyFunction, inputValue.as<Int64>());
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter
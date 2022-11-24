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

#include <Execution/Operators/Relational/MapJavaUdf.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <jni.h>

namespace NES::Runtime::Execution::Operators {

JavaVM* jvm;
JNIEnv* env;

/**
 * Load classes without custom class loader
 * @param byteCodeList
 * @return
 */
std::vector<jclass> loadClassesFromByteList(std::unordered_map<std::string, std::vector<char>> byteCodeList){
    std::vector<jclass> classes;
    // Load the classes from the byte list and the serialized instance
    for (auto entry : byteCodeList) {
        // TODO add the custom class loader
        auto bufLen = entry.second.size();
        const auto byteCode = reinterpret_cast<jbyte*> (&entry.second[0]);
        auto clazz = env->DefineClass(entry.first.data(), NULL, byteCode, (jsize) bufLen);
        classes.push_back(clazz);
    }
    return classes;
}

// Only used for test purposes
MapJavaUdf::MapJavaUdf(std::string javaPath) {
    // Start VM
    if (jvm == nullptr) {
        // init java vm arguments
        JavaVMInitArgs vm_args;           // Initialization arguments
        JavaVMOption* options = new JavaVMOption[2];// JVM invocation options
        //options[0].optionString = (char*) ("-Djava.class.path=" + javaPath).c_str();
        options[0].optionString = (char*) "-Djava.class.path=/home/amichalke/nebulastream/udf";
        options[1].optionString = (char*) "-verbose:jni";
        options[2].optionString = (char*) "-verbose:class";// where to find java .class
        vm_args.nOptions = 3;                              // number of options
        vm_args.options = options;
        vm_args.version = JNI_VERSION_1_2;// minimum Java version
        vm_args.ignoreUnrecognized = true;// invalid options make the JVM init fail

        // create java VM anc check for errors
        jint rc = JNI_CreateJavaVM(&jvm, (void**) &env, &vm_args);
        if (rc != JNI_OK) {
            // TODO: error processing
            std::cout << rc << std::endl;
            exit(EXIT_FAILURE);
        }
        if (env->ExceptionOccurred()) { // TODO more error checking
            env->ExceptionDescribe(); // print the stack trace
            exit(1);
        }
    }
}

MapJavaUdf::MapJavaUdf(std::string className,
                       std::unordered_map<std::string, std::vector<char>> byteCodeList,
                       SchemaPtr inputSchema,
                       SchemaPtr outputSchema,
                       std::vector<char> serializedInstance,
                       std::string methodName) : className(className),
                                                 byteCodeList(byteCodeList),
                                                 inputSchema(inputSchema),
                                                 outputSchema(outputSchema),
                                                 serializedInstance(serializedInstance),
                                                 methodName(methodName){
    // Start VM
    if (jvm == nullptr) {
        // init java vm arguments
        JavaVMInitArgs vm_args;           // Initialization arguments
        vm_args.version = JNI_VERSION_1_2;// minimum Java version
        vm_args.ignoreUnrecognized = true;// invalid options make the JVM init fail

        // create java VM anc check for errors
        jint rc = JNI_CreateJavaVM(&jvm, (void**) &env, &vm_args);
        if (rc != JNI_OK) {
            // TODO: error processing
            std::cout << rc << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    auto classes = loadClassesFromByteList(byteCodeList);
}

const std::string pojoName = "Pojo"; // TODO: get from java client
//const std::string pojoName = "java/lang/Readable"; // TODO: get from java client
const std::string udf_name = "map"; // TODO: use method name??
const std::string javaClass = "Udf";
const SchemaPtr inputSchema = Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64);

void *executeUdf(void *record){
    jclass pojoClass = env->FindClass(pojoName.data());
    if (env->ExceptionOccurred()) { // TODO more error checking
        std::cout << "find class\n";
        env->ExceptionDescribe(); // print the stack trace
        exit(EXIT_FAILURE);
    }

    // load pojo
    // jclass pojo = DefineClass(env, class_name.c_str(), jobject loader, const jbyte *buf, jsize bufLen);
    // create pojo object

    //auto pojo_obj = new jvalue;
    //pojo_obj->l = env->AllocObject(pojo);
    jobject pojo = env->AllocObject(pojoClass);
    if (env->ExceptionOccurred()) { // TODO more error checking
        std::cout << "alloc obj\n";
        env->ExceptionDescribe(); // print the stack trace
        exit(EXIT_FAILURE);
    }

    for (auto field : inputSchema->fields) {
        std::cout << field->getName() << "\n";
        //for (uint64_t i = 0; i < record.numberOfFields(); i++) {
        // TODO get first the field ID
        // TODO where do refwe get the signature

        // TODO currently only support floats
        if (field->getDataType()->isInteger()) {
            jfieldID id =  env->GetFieldID(pojoClass, field->getName().c_str(), "I");
            if (env->ExceptionOccurred()) { // TODO more error checking
                std::cout << "get field id\n";
                env->ExceptionDescribe(); // print the stack trace
                exit(EXIT_FAILURE);
            }
            auto rec = (Record*)record;
            auto val = rec->read("id");
            // TODO this gives the wrong value...
            int val_ = (int&) val.getValue();
            jint cla = val_;
            //int value = (int)((Record*)record)->read(field->getName());
            env->SetIntField(pojo, id, cla);
            if (env->ExceptionOccurred()) { // TODO more error checking
                std::cout << "set int field\n";
                env->ExceptionDescribe(); // print the stack trace
                exit(EXIT_FAILURE);
            }
        }
    }

    // execute function
    jclass c1 = env->FindClass(javaClass.data());
    if (c1 == nullptr) {
        std::cerr << "ERROR: class not found !" << std::endl;
        //env->ExceptionDescribe(); // try to find the class
    }
    if (env->ExceptionOccurred()) { // TODO more error checking
        env->ExceptionDescribe(); // print the stack trace
        exit(EXIT_FAILURE);
    }

    // TODO we need to derive the signature somehow
    // use for integer objects
    //jmethodID mid = env->GetMethodID(c1, udf_name.c_str(), "(Ljava/lang/Integer;)Ljava/lang/Integer;");
    jmethodID mid = env->GetMethodID(c1, udf_name.c_str(), "(I)I");
    if (env->ExceptionOccurred()) { // TODO more error checking
        env->ExceptionDescribe(); // print the stack trace
        exit(EXIT_FAILURE);
    }
    std::cout << "found map\n";
    jmethodID constructor = env->GetMethodID(c1, "<init>", "()V");
    if (env->ExceptionOccurred()) { // TODO more error checking
        env->ExceptionDescribe(); // print the stack trace
        exit(EXIT_FAILURE);
    }
    std::cout << "found constructor\n";
    jobject object = env->NewObject(c1, constructor);
    if (env->ExceptionOccurred()) { // TODO more error checking
        env->ExceptionDescribe(); // print the stack trace
        exit(EXIT_FAILURE);
    }
    std::cout << "new object\n";

    // TODO change depending on the output value the function
    //auto udf_result = env->CallObjectMethod(object, mid, pojo);
    int udf_result = env->CallIntMethod(object, mid, pojo);
    if (env->ExceptionOccurred()) { // TODO more error checking
        env->ExceptionDescribe(); // print the stack trace
        exit(EXIT_FAILURE);
    }
    std::cout << "call Map: " << udf_result << "\n";

    auto result = new Record({{"result", Value<>(udf_result)}});
    return result;
}

void MapJavaUdf::execute(ExecutionContext& ctx, Record& record) const {
    auto resultRecord = FunctionCall<>("executeUdf", executeUdf, Value<MemRef>((int8_t*) &record));
    child->execute(ctx, (Record&) resultRecord.getValue());
}
}// namespace NES::Nautilus
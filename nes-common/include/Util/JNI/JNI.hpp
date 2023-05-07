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

#ifndef NES_NES_COMMON_INCLUDE_UTIL_JNI_JNI_HPP_
#define NES_NES_COMMON_INCLUDE_UTIL_JNI_JNI_HPP_

#include <Exceptions/RuntimeException.hpp>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>// For std::runtime_error
#include <string>
#include <sys/types.h>
#include <vector>

// The following code is inspired by jnipp provided by @mitchdowd (https://github.com/mitchdowd/jnipp)

// Forward Declarations
struct JNIEnv_;
struct JavaVM_;
struct _jmethodID;
struct _jfieldID;
class _jobject;
class _jclass;
class _jarray;
class _jstring;
struct _jmethodID;
namespace jni {

typedef JNIEnv_ JNIEnv;
typedef JavaVM_ JavaVM;

typedef _jobject* jobject;
typedef _jclass* jclass;
typedef _jarray* jarray;
typedef _jstring* jstring;
typedef struct _jmethodID *jmethodID;


/**
Get the appropriate JNI environment for this thread.
*/
JNIEnv* getEnv();

/**
 * @brief Detachs the current thread from the jvm and removes all local references.
 */
void detatchEnv();

/**
 * When the application's entry point is in C++ rather than in Java, it will
 * need to spin up its own instance of the Java Virtual Machine (JVM) before
 * it can initialize the Java Native Interface. Vm is used to create and
 * destroy a running JVM instance.
 * The JVM is stored in a global static variable. So it can be initialized exactly once.
 * Any further initialization causes an exception.
 * Before creation, the add option and add classpath methods can be used, to configure the jvm instance.
 *
 * As only ever one JVM instance can be created, it is shared by different users. For instance, for the execution
 * of UDFs or as a compilation backend.
 */
class JVM final {
  public:
    /**
     * @brief Returns an instance to a JVM, which is maybe not initialized yet.
     * @return JVM.
     */
    static JVM& get();
    JVM();
    /**
    * Initialized the Java Virtual Machine with specific options and a classpath.
    */
    void init();
    /**
     * @brief Adds a option to the jvm. Throws an exception if the JVM is already started.
     * @param options
     * @return JVM
     */
    JVM& addOption(const std::string& options);
    /**
     * @brief Appends a classpath to the jvm before initialization.
     * @param classPath
     * @return JVM
     */
    JVM& addClasspath(const std::string& classPath);

    /**
     * @brief Indicates if the JVM was already initialized.
     * @return bool
     */
    bool isInitialized();

  private:
    std::vector<std::string> options;
    std::string classPath;
};

/**
 * A supplied name or type signature could not be resolved.
 */
class NameResolutionException : public NES::Exceptions::RuntimeException {
  public:
    /**
     * Constructor with an error message.
     * @param name The name of the unresolved symbol.
     */
    explicit NameResolutionException(const char* name) : RuntimeException(name) {}
};

/**
* The Java Native Interface was not properly initialized.
*/
class InitializationException : public NES::Exceptions::RuntimeException {
  public:
    /**
     * Constructor with an error message.
     * @aram msg Message to pass to the Exception.
     */
    explicit InitializationException(const char* msg) : RuntimeException(msg) {}
};

}// namespace jni

#endif//NES_NES_COMMON_INCLUDE_UTIL_JNI_JNI_HPP_

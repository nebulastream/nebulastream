/*MIT License
Copyright (c) 2016 Mitchell Dowd

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

       THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
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

#ifndef NES_RUNTIME_INCLUDE_UTIL_JNI_JNI_HPP_
#define NES_RUNTIME_INCLUDE_UTIL_JNI_JNI_HPP_

#include <Exceptions/RuntimeException.hpp>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/types.h>
#include <vector>

/* The following code contains portions of jnipp, provided by @mitchdowd (https://github.com/mitchdowd/jnipp)
*/

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

namespace NES::jni {

typedef JNIEnv_ JNIEnv;
typedef JavaVM_ JavaVM;

typedef _jobject* jobject;
typedef _jclass* jclass;
typedef _jarray* jarray;
typedef _jstring* jstring;
typedef struct _jmethodID* jmethodID;

/**
Get the appropriate JNI environment for this thread.
*/
JNIEnv* getEnv();

/**
 * @brief Detachs the current thread from the jvm and removes all local references.
 */
void detachEnv();

/**
 * When the application's entry point is in C++ rather than in Java, it will
 * need to spin up its own instance of the Java Virtual Machine (JVM) before
 * it can initialize the Java Native Interface. Vm is used to create and
 * destroy a running JVM instance.
 * The JVM is stored in a global static variable. So it can be initialized exactly once.
 * Any further initialization causes an exception.
 * Before creation, the addOption and addClasspath methods can be used, to configure the JVM instance.
 *
 * As only ever one JVM instance can be created, it is shared by different users. For instance, for the execution
 * of UDFs or as a compilation backend.
 */
class JVM final {
  public:
    JVM() : options() {}

    /**
     * @brief Returns an instance to a JVM, which may or may not be initialized yet.
     * @return JVM.
     */
    static JVM& get();

    /**
    * @brief Initialized the Java Virtual Machine with specific options and a classpath.
    */
    void init();
    /**
     * @brief Adds a option to the JVM. Throws an exception if the JVM is already started.
     * @param options
     * @return JVM
     */
    JVM& addOption(const std::string& options);
    /**
     * @brief Appends a classpath to the JVM before initialization.
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
     * @param msg Message to pass to the Exception.
     */
    explicit InitializationException(const char* msg) : RuntimeException(msg) {}
};

}// namespace NES::jni

#endif // NES_RUNTIME_INCLUDE_UTIL_JNI_JNI_HPP_

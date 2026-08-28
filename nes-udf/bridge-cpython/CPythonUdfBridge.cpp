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

/* CPython bridge implementing the NebulaStream scalar-UDF C ABI (UdfAbi.h).
 *
 * This is ONE reusable .so that serves ALL Python UDFs: the ENTRYPOINT string is a
 * "module.function" that the bridge imports and calls per row. A user authors a UDF as a
 * plain Python function and registers it with
 *   CREATE FUNCTION f(...) RETURNS ... FROM '<this bridge>.so' ENTRYPOINT 'module.function';
 *
 * The interpreter embeds CPython. It has a GIL, so we initialise once, release the GIL, and
 * bracket every entry point in PyGILState_Ensure/Release — calls are safe (but serialised) from
 * any engine thread. A Python-level exception is caught, formatted, and returned as UDF_ERROR,
 * so the worker survives. A *hard* failure (os._exit, a segfault in a C extension) is not
 * containable in-process — that is what the (future) sidecar backend is for.
 *
 * Ported from ../udf/poc_dlopen/bridge/udf_bridge.cpp and adapted to the clean ABI (explicit null
 * flags, full type table, length-delimited/binary-safe VARSIZED as Python `bytes`).
 */

#include <Python.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <UdfAbi.h>

namespace
{

struct UdfCtx
{
    PyObject* callable = nullptr; /* owned reference to the Python function */
    std::vector<int> argTypes;
    int returnType = 0;
};

std::mutex g_mutex;
std::unordered_map<int, UdfCtx*> g_registry;
int g_nextHandle = 1;
std::once_flag g_pyInit;

char* dupString(const std::string& text)
{
    char* copy = static_cast<char*>(std::malloc(text.size() + 1));
    if (copy != nullptr)
    {
        std::memcpy(copy, text.c_str(), text.size() + 1);
    }
    return copy;
}

void initPythonOnce()
{
    Py_InitializeEx(0);
    /* Make the UDF modules importable. NES_UDF_PATH points at the dir containing them. */
    const char* env = std::getenv("NES_UDF_PATH");
    const std::string path = (env != nullptr && *env != '\0') ? env : ".";
    if (PyObject* sysPath = PySys_GetObject("path"))
    {
        PyObject* entry = PyUnicode_FromString(path.c_str());
        PyList_Insert(sysPath, 0, entry);
        Py_DECREF(entry);
    }
    PyEval_SaveThread(); /* release the GIL so PyGILState_Ensure works from any thread */
}

/* Must be called with the GIL held and a Python error set; returns a malloc'd message. */
char* fetchPythonError()
{
    PyObject* type = nullptr;
    PyObject* value = nullptr;
    PyObject* traceback = nullptr;
    PyErr_Fetch(&type, &value, &traceback);
    PyErr_NormalizeException(&type, &value, &traceback);

    std::string out;
    if (type != nullptr)
    {
        if (PyObject* name = PyObject_GetAttrString(type, "__name__"))
        {
            const char* text = PyUnicode_AsUTF8(name);
            out += (text != nullptr) ? text : "<exc>";
            Py_DECREF(name);
        }
    }
    if (value != nullptr)
    {
        if (PyObject* asStr = PyObject_Str(value))
        {
            const char* text = PyUnicode_AsUTF8(asStr);
            out += ": ";
            out += (text != nullptr) ? text : "";
            Py_DECREF(asStr);
        }
    }
    PyErr_Clear();
    Py_XDECREF(type);
    Py_XDECREF(value);
    Py_XDECREF(traceback);
    return dupString(out);
}

PyObject* toPyArg(const void* value, long long length, int isNull, int typeCode)
{
    if (isNull != 0)
    {
        Py_RETURN_NONE;
    }
    switch (typeCode)
    {
        case UDF_BOOL:
            return PyBool_FromLong(*reinterpret_cast<const std::int8_t*>(value) != 0);
        case UDF_INT8:
            return PyLong_FromLongLong(*reinterpret_cast<const std::int8_t*>(value));
        case UDF_INT16:
            return PyLong_FromLongLong(*reinterpret_cast<const std::int16_t*>(value));
        case UDF_INT32:
            return PyLong_FromLongLong(*reinterpret_cast<const std::int32_t*>(value));
        case UDF_INT64:
            return PyLong_FromLongLong(*reinterpret_cast<const std::int64_t*>(value));
        case UDF_UINT8:
            return PyLong_FromUnsignedLongLong(*reinterpret_cast<const std::uint8_t*>(value));
        case UDF_UINT16:
            return PyLong_FromUnsignedLongLong(*reinterpret_cast<const std::uint16_t*>(value));
        case UDF_UINT32:
            return PyLong_FromUnsignedLongLong(*reinterpret_cast<const std::uint32_t*>(value));
        case UDF_UINT64:
            return PyLong_FromUnsignedLongLong(*reinterpret_cast<const std::uint64_t*>(value));
        case UDF_FLOAT32:
            return PyFloat_FromDouble(*reinterpret_cast<const float*>(value));
        case UDF_FLOAT64:
            return PyFloat_FromDouble(*reinterpret_cast<const double*>(value));
        case UDF_VARSIZED:
            return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(value), static_cast<Py_ssize_t>(length));
        default:
            return nullptr;
    }
}

/* Marshal the Python result into the ABI out-params per the declared return type. GIL held. */
int writeResult(
    PyObject* result, int returnType, void* resultScalar, char** resultString, long long* resultStringLen, int* resultNull, char** err)
{
    if (result == Py_None)
    {
        *resultNull = 1;
        return UDF_OK;
    }
    switch (returnType)
    {
        case UDF_BOOL:
            *reinterpret_cast<std::int8_t*>(resultScalar) = static_cast<std::int8_t>(PyObject_IsTrue(result));
            return UDF_OK;
        case UDF_INT8:
        case UDF_INT16:
        case UDF_INT32:
        case UDF_INT64: {
            const long long v = PyLong_AsLongLong(result);
            if (PyErr_Occurred() != nullptr)
            {
                *err = fetchPythonError();
                return UDF_ERROR;
            }
            if (returnType == UDF_INT8)
            {
                *reinterpret_cast<std::int8_t*>(resultScalar) = static_cast<std::int8_t>(v);
            }
            else if (returnType == UDF_INT16)
            {
                *reinterpret_cast<std::int16_t*>(resultScalar) = static_cast<std::int16_t>(v);
            }
            else if (returnType == UDF_INT32)
            {
                *reinterpret_cast<std::int32_t*>(resultScalar) = static_cast<std::int32_t>(v);
            }
            else
            {
                *reinterpret_cast<std::int64_t*>(resultScalar) = static_cast<std::int64_t>(v);
            }
            return UDF_OK;
        }
        case UDF_UINT8:
        case UDF_UINT16:
        case UDF_UINT32:
        case UDF_UINT64: {
            const unsigned long long v = PyLong_AsUnsignedLongLong(result);
            if (PyErr_Occurred() != nullptr)
            {
                *err = fetchPythonError();
                return UDF_ERROR;
            }
            if (returnType == UDF_UINT8)
            {
                *reinterpret_cast<std::uint8_t*>(resultScalar) = static_cast<std::uint8_t>(v);
            }
            else if (returnType == UDF_UINT16)
            {
                *reinterpret_cast<std::uint16_t*>(resultScalar) = static_cast<std::uint16_t>(v);
            }
            else if (returnType == UDF_UINT32)
            {
                *reinterpret_cast<std::uint32_t*>(resultScalar) = static_cast<std::uint32_t>(v);
            }
            else
            {
                *reinterpret_cast<std::uint64_t*>(resultScalar) = static_cast<std::uint64_t>(v);
            }
            return UDF_OK;
        }
        case UDF_FLOAT32:
        case UDF_FLOAT64: {
            const double v = PyFloat_AsDouble(result);
            if (PyErr_Occurred() != nullptr)
            {
                *err = fetchPythonError();
                return UDF_ERROR;
            }
            if (returnType == UDF_FLOAT32)
            {
                *reinterpret_cast<float*>(resultScalar) = static_cast<float>(v);
            }
            else
            {
                *reinterpret_cast<double*>(resultScalar) = v;
            }
            return UDF_OK;
        }
        case UDF_VARSIZED: {
            char* bytes = nullptr;
            Py_ssize_t length = 0;
            if (PyBytes_Check(result))
            {
                PyBytes_AsStringAndSize(result, &bytes, &length);
            }
            else if (PyUnicode_Check(result))
            {
                bytes = const_cast<char*>(PyUnicode_AsUTF8AndSize(result, &length));
            }
            else
            {
                *err = dupString("VARSIZED-returning UDF must return bytes or str");
                return UDF_ERROR;
            }
            char* out = static_cast<char*>(std::malloc(length > 0 ? static_cast<size_t>(length) : 1));
            if (length > 0)
            {
                std::memcpy(out, bytes, static_cast<size_t>(length));
            }
            *resultString = out;
            *resultStringLen = static_cast<long long>(length);
            return UDF_OK;
        }
        default:
            *err = dupString("unsupported UDF return type code");
            return UDF_ERROR;
    }
}

}

extern "C" int initialize_udf(const char* entrypoint, int argc, const int* arg_type_codes, int return_type_code, char** errormessage)
{
    if (errormessage != nullptr)
    {
        *errormessage = nullptr;
    }
    std::call_once(g_pyInit, initPythonOnce);
    const PyGILState_STATE gil = PyGILState_Ensure();

    const std::string full = (entrypoint != nullptr) ? entrypoint : "";
    const std::string::size_type dot = full.rfind('.');
    if (dot == std::string::npos)
    {
        if (errormessage != nullptr)
        {
            *errormessage = dupString("entrypoint must be of the form 'module.function'");
        }
        PyGILState_Release(gil);
        return -1;
    }

    PyObject* module = PyImport_ImportModule(full.substr(0, dot).c_str());
    if (module == nullptr)
    {
        if (errormessage != nullptr)
        {
            *errormessage = fetchPythonError();
        }
        PyGILState_Release(gil);
        return -1;
    }
    PyObject* function = PyObject_GetAttrString(module, full.substr(dot + 1).c_str());
    Py_DECREF(module);
    if (function == nullptr || PyCallable_Check(function) == 0)
    {
        Py_XDECREF(function);
        if (errormessage != nullptr)
        {
            *errormessage = (PyErr_Occurred() != nullptr) ? fetchPythonError() : dupString("entrypoint attribute is not callable");
        }
        PyGILState_Release(gil);
        return -1;
    }

    auto* ctx = new UdfCtx();
    ctx->callable = function; /* transfer ownership */
    ctx->argTypes.assign(arg_type_codes, arg_type_codes + argc);
    ctx->returnType = return_type_code;

    int handle = 0;
    {
        const std::lock_guard<std::mutex> lock(g_mutex);
        handle = g_nextHandle++;
        g_registry[handle] = ctx;
    }
    PyGILState_Release(gil);
    return handle;
}

extern "C" int execute_udf_row(
    int handle,
    const void* const* arg_values,
    const long long* arg_lens,
    const int* arg_nulls,
    void* result_scalar,
    char** result_string,
    long long* result_string_len,
    int* result_null,
    char** errormessage)
{
    if (errormessage != nullptr)
    {
        *errormessage = nullptr;
    }
    *result_null = 0;

    UdfCtx* ctx = nullptr;
    {
        const std::lock_guard<std::mutex> lock(g_mutex);
        auto it = g_registry.find(handle);
        if (it == g_registry.end())
        {
            if (errormessage != nullptr)
            {
                *errormessage = dupString("invalid UDF handle");
            }
            return UDF_ERROR;
        }
        ctx = it->second;
    }

    const PyGILState_STATE gil = PyGILState_Ensure();
    const auto argc = static_cast<int>(ctx->argTypes.size());
    PyObject* args = PyTuple_New(argc);
    for (int i = 0; i < argc; ++i)
    {
        PyObject* arg = toPyArg(arg_values[i], arg_lens[i], arg_nulls[i], ctx->argTypes[i]);
        if (arg == nullptr)
        {
            Py_DECREF(args);
            if (errormessage != nullptr)
            {
                *errormessage = dupString("unsupported argument type code");
            }
            PyGILState_Release(gil);
            return UDF_ERROR;
        }
        PyTuple_SET_ITEM(args, i, arg); /* steals the reference */
    }

    PyObject* result = PyObject_CallObject(ctx->callable, args);
    Py_DECREF(args);
    if (result == nullptr)
    {
        /* The UDF raised: format it and report failure — the engine is never killed. */
        if (errormessage != nullptr)
        {
            *errormessage = fetchPythonError();
        }
        PyGILState_Release(gil);
        return UDF_ERROR;
    }

    char* resultError = nullptr;
    const int status = writeResult(result, ctx->returnType, result_scalar, result_string, result_string_len, result_null, &resultError);
    Py_DECREF(result);
    PyGILState_Release(gil);

    if (status == UDF_ERROR)
    {
        if (errormessage != nullptr)
        {
            *errormessage = (resultError != nullptr) ? resultError : dupString("udf result marshalling error");
        }
        else
        {
            std::free(resultError);
        }
    }
    return status;
}

extern "C" void cleanup_udf(int handle)
{
    UdfCtx* ctx = nullptr;
    {
        const std::lock_guard<std::mutex> lock(g_mutex);
        auto it = g_registry.find(handle);
        if (it != g_registry.end())
        {
            ctx = it->second;
            g_registry.erase(it);
        }
    }
    if (ctx == nullptr)
    {
        return;
    }
    const PyGILState_STATE gil = PyGILState_Ensure();
    Py_XDECREF(ctx->callable);
    PyGILState_Release(gil);
    delete ctx;
}

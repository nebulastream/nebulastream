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
 * One .so serves all Python UDFs; ENTRYPOINT is a "module.function" string resolved per UDF.
 * Embeds CPython, which has a GIL: init once, release it, and bracket every entry point in
 * PyGILState_Ensure/Release. Batches reach Python through pyarrow, which must be importable (from
 * NES_UDF_VENV). A Python exception becomes UDF_ERROR; a hard failure (segfault, os._exit) is not
 * contained here -- that's the job of the out-of-process backend.
 */

/// NOLINTNEXTLINE(misc-include-cleaner)
#include <Python.h>

#include <dlfcn.h>
#include <pthread.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include <UdfAbi.h>
#include <PythonArrowColumns.hpp>
#include <PythonBytesParameters.hpp>

/// Implements the C ABI of UdfAbi.h on top of the CPython C API: C types and malloc'ed strings owned by the caller.
/// NOLINTBEGIN(google-runtime-int,cppcoreguidelines-owning-memory,cppcoreguidelines-no-malloc,misc-include-cleaner,cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic,cppcoreguidelines-pro-type-const-cast,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-pro-bounds-avoid-unchecked-container-access,readability-identifier-naming,concurrency-mt-unsafe)

/* Thread-specific data keys of the base namespace's libc.
 * Loaded with dlmopen, the bridge brings its own libc, whose key table is separate from the worker's, but both index the same slots
 * of a thread. Keys of libpython in the namespace would then overwrite the values of the worker's keys (folly, TBB), whose destructors
 * crash on them when the thread exits. As the first object of its namespace, the bridge interposes the key functions for libpython
 * and libstdc++ there and forwards them to the base libc, so all keys come from one table. In the base namespace the forwarding
 * changes nothing. */
namespace
{
template <typename Function>
Function baseLibc(const char* name)
{
    static void* const libc = dlmopen(LM_ID_BASE, "libc.so.6", RTLD_NOW | RTLD_NOLOAD);
    void* const function = libc != nullptr ? dlsym(libc, name) : nullptr;
    if (function == nullptr)
    {
        std::abort();
    }
    return reinterpret_cast<Function>(function);
}
}

extern "C" int pthread_key_create(pthread_key_t* key, void (*destructor)(void*))
{
    static auto* const function = baseLibc<int (*)(pthread_key_t*, void (*)(void*))>("pthread_key_create");
    return function(key, destructor);
}

extern "C" int pthread_key_delete(pthread_key_t key)
{
    static auto* const function = baseLibc<int (*)(pthread_key_t)>("pthread_key_delete");
    return function(key);
}

extern "C" void* pthread_getspecific(pthread_key_t key)
{
    static auto* const function = baseLibc<void* (*)(pthread_key_t)>("pthread_getspecific");
    return function(key);
}

extern "C" int pthread_setspecific(pthread_key_t key, const void* value)
{
    static auto* const function = baseLibc<int (*)(pthread_key_t, const void*)>("pthread_setspecific");
    return function(key, value);
}

namespace
{

/* A registered UDF. The Python objects are owned references in the main interpreter; the batch ones are created with the first
 * batch. A subinterpreter loads the function again from `entrypoint` or `source`, see callableFor. */
struct UdfCtx
{
    std::uint64_t id = 0; /* never reused, unlike the address, to key what a thread keeps of the UDF */
    PyObject* callable = nullptr; /* the Python function */
    std::string entrypoint; /* "module.function", or empty for a UDF from source */
    std::string source;
    std::string functionName;
    std::vector<int> argTypes;
    std::vector<char> bytesParameters; /* for each argument, if a VARSIZED value stays raw bytes instead of a str */
    int returnType = 0;
    PyObject* argumentSchema = nullptr; /* pa.Schema of the struct of argument columns */
    PyObject* argumentTypeList = nullptr; /* list of the argument type codes */
    PyObject* bytesParameterList = nullptr; /* bytesParameters as a list */
};

/* Runs a UDF on the Arrow batches of execute_udf_batch, defined next to PythonArrowColumnsSource. Type codes index _TYPES. */
constexpr const char* BatchSource = R"(
import pyarrow as pa

_TYPES = [pa.bool_(), pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(),
          pa.float32(), pa.float64(), pa.binary(), pa.binary(1)]


def _schema(types):
    return pa.schema([pa.field("a" + str(index), _TYPES[code]) for index, code in enumerate(types)])


def _column(column, code, as_bytes):
    if code == 11:
        return _binary_column(column, as_bytes)
    if code == 12:
        return _char_column(column, as_bytes)
    return column.to_pylist()


def _result(values, code):
    if code == 11:
        values = [None if v is None else _binary(v) for v in values]
    elif code == 12:
        values = [None if v is None else _char(v) for v in values]
    elif code == 0:
        values = [None if v is None else bool(v) for v in values]
    return pa.array(values, type=_TYPES[code])


def _run(function, schema, types, as_bytes, result_type, arguments, result):
    batch = pa.RecordBatch._import_from_c(arguments, schema)
    columns = [_column(column, code, raw) for column, code, raw in zip(batch.columns, types, as_bytes)]
    rows = batch.num_rows
    del batch
    values = [function(*row) for row in zip(*columns)] if columns else [function() for _ in range(rows)]
    _result(values, result_type)._export_to_c(result)


def _run_fused(fused, schema, types, as_bytes, result_types, arguments, result):
    batch = pa.RecordBatch._import_from_c(arguments, schema)
    columns = [_column(column, code, raw) for column, code, raw in zip(batch.columns, types, as_bytes)]
    rows = batch.num_rows
    del batch
    outputs = [fused(*row) for row in zip(*columns)] if columns else [fused() for _ in range(rows)]
    per_udf = zip(*outputs) if outputs else [() for _ in result_types]
    arrays = [_result(list(values), code) for values, code in zip(per_udf, result_types)]
    pa.StructArray.from_arrays(arrays, names=["r" + str(i) for i in range(len(arrays))])._export_to_c(result)
)";

/* A stage of UDFs that run together, see initialize_udf_stage. Holds its UDFs, which outlive it. `fused` is a new reference to the
 * function that calls all of them with the arguments of all of them and returns their results as a tuple, in the main interpreter.
 * The batch objects are created with the first batch. */
struct StageCtx
{
    std::uint64_t id = 0; /* like UdfCtx's */
    std::vector<UdfCtx*> udfs;
    int argumentCount = 0;
    PyObject* fused = nullptr;
    PyObject* argumentSchema = nullptr; /* pa.Schema of the struct of the argument columns of all UDFs */
    PyObject* argumentTypeList = nullptr;
    PyObject* bytesParameterList = nullptr;
    PyObject* resultTypeList = nullptr;
};

/* The ids of UdfCtx and StageCtx. A UDF and a stage reach the bridge as a pointer, so that calls need no lookup under a lock. */
std::atomic<std::uint64_t> g_nextId{1};
std::once_flag g_pyInit;
/* Written once under g_pyInit, read afterwards: whether the embedded interpreter came up. */
bool g_pyReady = false;
/* The namespace of BatchSource, created with the first UDF under the GIL; nullptr until then or if pyarrow is missing. */
PyObject* g_batchNamespace = nullptr;

char* dupString(const std::string& text)
{
    char* copy = static_cast<char*>(std::malloc(text.size() + 1));
    if (copy != nullptr)
    {
        std::memcpy(copy, text.c_str(), text.size() + 1);
    }
    return copy;
}

/* Puts the venv and the UDF modules on sys.path of the current interpreter. GIL held. */
void addSearchPaths()
{
    /* Opt-in venv for third-party packages (e.g. onnxruntime) not on the system Python's site-packages.
     * NES_UDF_VENV names a `python -m venv` directory; unset (the default) leaves the embedded interpreter
     * exactly as if this code did not exist -- system-local Python and its own site-packages only.
     *
     * A `python -m venv` names its one version-specific dir after whichever interpreter created it (e.g.
     * lib/python3.12), which need not match this bridge's own linked Python -- scan for it rather than
     * assume the two agree, so a pure-Python-only venv (no compiled extensions) works regardless of which
     * CPython minor built the bridge. A *compiled* extension inside the venv still needs a matching ABI;
     * scanning does not (and cannot) relax that -- it only stops an unrelated version-number mismatch from
     * hiding an otherwise-portable pure-Python package. */
    if (const char* venv = std::getenv("NES_UDF_VENV"); venv != nullptr && *venv != '\0')
    {
        std::error_code errorCode;
        for (const auto& entry : std::filesystem::directory_iterator(std::filesystem::path(venv) / "lib", errorCode))
        {
            if (errorCode || !entry.is_directory() || !entry.path().filename().string().starts_with("python"))
            {
                continue;
            }
            const auto sitePackages = (entry.path() / "site-packages").string();
            if (PyObject* sysPath = PySys_GetObject("path"))
            {
                PyObject* pyEntry = PyUnicode_FromString(sitePackages.c_str());
                PyList_Insert(sysPath, 0, pyEntry);
                Py_DECREF(pyEntry);
            }
            break;
        }
    }

    /* Make the UDF modules importable. NES_UDF_PATH points at the dir containing them. Inserted last so it
     * ends up frontmost -- a user's own UDF module always shadows a same-named venv package. */
    const char* env = std::getenv("NES_UDF_PATH");
    const std::string path = (env != nullptr && *env != '\0') ? env : ".";
    if (PyObject* sysPath = PySys_GetObject("path"))
    {
        PyObject* entry = PyUnicode_FromString(path.c_str());
        PyList_Insert(sysPath, 0, entry);
        Py_DECREF(entry);
    }
}

void initPythonOnce()
{
    /* The catalog API can name this bridge by a path other than the shipped one, which loads a second copy of this
     * library into the worker, with its own g_pyInit, but sharing the one embedded interpreter. Only the copy that
     * actually starts it up holds the GIL afterwards; on an already running interpreter Py_InitializeEx returns
     * without one, so take the GIL explicitly below rather than assume we hold it. */
    if (Py_IsInitialized() == 0)
    {
        /* The worker dlopens this bridge RTLD_LOCAL, and libpython with it, so the symbols of libpython are not global, and compiled
         * extension modules such as pyarrow's, which expect them from the process, fail to load. Promote the loaded libpython. */
        /* Only in the base namespace: in a namespace of its own (dlmopen), libpython already serves the extension modules. */
        if (Dl_info info{}; dladdr(reinterpret_cast<void*>(&Py_InitializeEx), &info) != 0 && info.dli_fname != nullptr)
        {
            Lmid_t namespaceId = LM_ID_BASE;
            if (void* self = dlopen(info.dli_fname, RTLD_NOW | RTLD_NOLOAD); self != nullptr)
            {
                dlinfo(self, RTLD_DI_LMID, &namespaceId);
                if (namespaceId == LM_ID_BASE)
                {
                    dlopen(info.dli_fname, RTLD_NOW | RTLD_NOLOAD | RTLD_GLOBAL);
                }
            }
        }
        Py_InitializeEx(0);
        /* Py_InitializeEx reports a failed start-up (e.g. a stdlib it cannot locate) by leaving the interpreter
         * uninitialized rather than by returning, and every later C API call on that state segfaults. Record the
         * outcome so the UDF entry points can fail the query instead of taking the process down with them. */
        if (Py_IsInitialized() == 0)
        {
            return;
        }
        PyEval_SaveThread(); /* release the GIL so PyGILState_Ensure works from any thread */
    }
    const PyGILState_STATE gil = PyGILState_Ensure();
    addSearchPaths();
    g_pyReady = true;
    PyGILState_Release(gil);
}

/* PyGILState_Ensure on a thread without a Python thread state creates one, and the matching Release destroys it again, which
 * unmaps its frame stack: per row, that took longer than calling the UDF. So the first call of a thread leaves its thread state
 * alive (the gilstate counter never drops to 0), and every later Ensure/Release only takes and drops the GIL. */
PyGILState_STATE ensureGil()
{
    thread_local const bool keepThreadState = []
    {
        PyGILState_Ensure();
        PyEval_SaveThread();
        return true;
    }();
    static_cast<void>(keepThreadState);
    return PyGILState_Ensure();
}

/* Returned by enter_udf_row for a thread that entered its own subinterpreter, next to the PyGILState_STATE values. */
constexpr int ENTERED_ISOLATED = 2;

/* The subinterpreter of this thread, with a GIL of its own, created on its first isolated call and kept for its lifetime, like the
 * thread state of the main interpreter. */
thread_local PyThreadState* t_isolated = nullptr;

/* Attaches this thread to its subinterpreter, creating it first. Returns false if Python cannot create it. */
bool enterIsolated()
{
    if (t_isolated == nullptr)
    {
        const PyGILState_STATE gil = ensureGil();
        PyThreadState* const main = PyThreadState_Get();
        PyInterpreterConfig config = {
            .use_main_obmalloc = 0,
            .allow_fork = 0,
            .allow_exec = 0,
            .allow_threads = 1,
            .allow_daemon_threads = 0,
            .check_multi_interp_extensions = 1,
            .gil = PyInterpreterConfig_OWN_GIL,
        };
        PyThreadState* isolated = nullptr;
        /* Detaches `main` and attaches the new interpreter. */
        const PyStatus status = Py_NewInterpreterFromConfig(&isolated, &config);
        if (PyStatus_Exception(status) != 0 || isolated == nullptr)
        {
            PyThreadState_Swap(main);
            PyGILState_Release(gil);
            return false;
        }
        addSearchPaths();
        PyEval_SaveThread();
        PyEval_RestoreThread(main);
        PyGILState_Release(gil);
        t_isolated = isolated;
    }
    PyEval_RestoreThread(t_isolated);
    return true;
}

/* Takes the GIL for a call, unless enter_udf_row already attached this thread to an interpreter. */
class Attached
{
public:
    Attached() : attached(PyThreadState_GetUnchecked() != nullptr)
    {
        if (!attached)
        {
            gil = ensureGil();
        }
    }

    ~Attached()
    {
        if (!attached)
        {
            PyGILState_Release(gil);
        }
    }

    Attached(const Attached&) = delete;
    Attached& operator=(const Attached&) = delete;
    Attached(Attached&&) = delete;
    Attached& operator=(Attached&&) = delete;

private:
    bool attached;
    PyGILState_STATE gil{};
};

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

/* Runs PythonBytesParametersSource on `function`. If that fails every parameter is a str. GIL held. */
std::vector<char> findBytesParameters(PyObject* function, const int argc)
{
    std::vector<char> flags(static_cast<size_t>(argc), 0);
    PyObject* globals = PyDict_New();
    PyObject* definition = PyRun_String(NES::PythonBytesParametersSource, Py_file_input, globals, globals);
    PyObject* result = (definition != nullptr)
        ? PyObject_CallFunction(PyDict_GetItemString(globals, "_bytes_parameters"), "Oi", function, argc)
        : nullptr;
    if (result != nullptr)
    {
        for (int i = 0; i < argc; ++i)
        {
            flags[static_cast<size_t>(i)] = static_cast<char>(PyObject_IsTrue(PyList_GET_ITEM(result, i)) == 1);
        }
    }
    PyErr_Clear();
    Py_XDECREF(result);
    Py_XDECREF(definition);
    Py_DECREF(globals);
    return flags;
}

/* Defines BatchSource once. Returns false with a Python error set if that fails, e.g. without pyarrow. GIL held. */
bool defineBatchNamespace()
{
    if (g_batchNamespace != nullptr)
    {
        return true;
    }
    PyObject* globals = PyDict_New();
    if (globals == nullptr || PyDict_SetItemString(globals, "__builtins__", PyEval_GetBuiltins()) != 0)
    {
        Py_XDECREF(globals);
        return false;
    }
    for (const char* source : {BatchSource, NES::PythonArrowColumnsSource})
    {
        PyObject* defined = PyRun_String(source, Py_file_input, globals, globals);
        if (defined == nullptr)
        {
            Py_DECREF(globals);
            return false;
        }
        Py_DECREF(defined);
    }
    g_batchNamespace = globals;
    return true;
}

PyObject* typeCodeList(const int* codes, const int count)
{
    PyObject* list = PyList_New(count);
    for (int i = 0; list != nullptr && i < count; ++i)
    {
        PyList_SET_ITEM(list, i, PyLong_FromLong(codes[i]));
    }
    return list;
}

/* Takes ownership of `function` (may be nullptr / non-callable). On success, returns the new UDF; on failure returns nullptr with
 * *errormessage set (or leaves any existing Python error to be formatted by the caller). GIL held. */
UdfInstance* registerCallable(PyObject* function, UdfCtx description, char** errormessage)
{
    if (function == nullptr || PyCallable_Check(function) == 0)
    {
        Py_XDECREF(function);
        if (errormessage != nullptr)
        {
            *errormessage = (PyErr_Occurred() != nullptr) ? fetchPythonError() : dupString("entrypoint attribute is not callable");
        }
        return nullptr;
    }

    auto* ctx = new UdfCtx(std::move(description));
    ctx->id = g_nextId++;
    ctx->callable = function; /* transfer ownership */
    ctx->bytesParameters = findBytesParameters(function, static_cast<int>(ctx->argTypes.size()));
    return reinterpret_cast<UdfInstance*>(ctx);
}

/* A new reference to the function `entrypoint` or `source` defines, in the current interpreter; nullptr with a Python error set
 * if that fails. GIL held. */
PyObject* loadCallable(const UdfCtx& ctx)
{
    if (!ctx.entrypoint.empty())
    {
        const auto dot = ctx.entrypoint.rfind('.');
        PyObject* module = PyImport_ImportModule(ctx.entrypoint.substr(0, dot).c_str());
        if (module == nullptr)
        {
            return nullptr;
        }
        PyObject* function = PyObject_GetAttrString(module, ctx.entrypoint.substr(dot + 1).c_str());
        Py_DECREF(module);
        return function;
    }
    PyObject* globals = PyDict_New();
    if (globals == nullptr || PyDict_SetItemString(globals, "__builtins__", PyEval_GetBuiltins()) != 0)
    {
        Py_XDECREF(globals);
        return nullptr;
    }
    PyObject* defined = PyRun_String(ctx.source.c_str(), Py_file_input, globals, globals);
    PyObject* function = nullptr;
    if (defined != nullptr)
    {
        function = PyDict_GetItemString(globals, ctx.functionName.c_str()); /* borrowed */
        Py_XINCREF(function);
        Py_DECREF(defined);
    }
    Py_DECREF(globals);
    return function;
}

/* The function of `ctx` in the interpreter the thread is attached to: the registered one in the main interpreter, loaded once per
 * thread in its subinterpreter. The functions of a subinterpreter live as long as it, past cleanup_udf. GIL held. */
PyObject* callableFor(const UdfCtx& ctx)
{
    if (PyInterpreterState_Get() == PyInterpreterState_Main())
    {
        return ctx.callable;
    }
    thread_local std::unordered_map<std::uint64_t, PyObject*> isolatedCallables;
    if (const auto it = isolatedCallables.find(ctx.id); it != isolatedCallables.end())
    {
        return it->second;
    }
    PyObject* function = loadCallable(ctx);
    if (function != nullptr)
    {
        isolatedCallables.emplace(ctx.id, function);
    }
    return function;
}

/* Creates the batch objects of `ctx` with its first batch. Returns false with a Python error set if that fails, e.g. without
 * pyarrow. GIL of the main interpreter held. */
bool prepareBatches(UdfCtx& ctx)
{
    if (ctx.argumentSchema != nullptr)
    {
        return true;
    }
    if (!defineBatchNamespace())
    {
        return false;
    }
    PyObject* types = typeCodeList(ctx.argTypes.data(), static_cast<int>(ctx.argTypes.size()));
    PyObject* bytes = PyList_New(static_cast<Py_ssize_t>(ctx.bytesParameters.size()));
    for (size_t i = 0; bytes != nullptr && i < ctx.bytesParameters.size(); ++i)
    {
        PyList_SET_ITEM(bytes, static_cast<Py_ssize_t>(i), PyBool_FromLong(ctx.bytesParameters[i]));
    }
    PyObject* schema
        = types != nullptr ? PyObject_CallFunctionObjArgs(PyDict_GetItemString(g_batchNamespace, "_schema"), types, nullptr) : nullptr;
    if (types == nullptr || bytes == nullptr || schema == nullptr)
    {
        Py_XDECREF(types);
        Py_XDECREF(bytes);
        Py_XDECREF(schema);
        return false;
    }
    ctx.argumentTypeList = types;
    ctx.bytesParameterList = bytes;
    ctx.argumentSchema = schema;
    return true;
}



PyObject* toPyArg(const void* value, long long length, int isNull, int typeCode, bool asBytes)
{
    if (isNull != 0)
    {
        Py_RETURN_NONE;
    }
    switch (typeCode)
    {
        case UDF_BOOL:
            return PyBool_FromLong(static_cast<long>(*reinterpret_cast<const std::int8_t*>(value) != 0));
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
            if (asBytes)
            {
                return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(value), static_cast<Py_ssize_t>(length));
            }
            /// Decoding raises for anything that is not UTF-8.
            return PyUnicode_DecodeUTF8(reinterpret_cast<const char*>(value), static_cast<Py_ssize_t>(length), nullptr);
        case UDF_CHAR:
            /// One byte, as one-byte bytes or, failing for anything but ASCII, a one-character str.
            if (asBytes)
            {
                return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(value), 1);
            }
            return PyUnicode_DecodeUTF8(reinterpret_cast<const char*>(value), 1, nullptr);
        default:
            return nullptr;
    }
}

/* Marshal the Python result into the ABI out-params per the declared return type. GIL held. */
/// NOLINTNEXTLINE(readability-function-cognitive-complexity)
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
            const long long scalar = PyLong_AsLongLong(result);
            if (PyErr_Occurred() != nullptr)
            {
                *err = fetchPythonError();
                return UDF_ERROR;
            }
            if (returnType == UDF_INT8)
            {
                *reinterpret_cast<std::int8_t*>(resultScalar) = static_cast<std::int8_t>(scalar);
            }
            else if (returnType == UDF_INT16)
            {
                *reinterpret_cast<std::int16_t*>(resultScalar) = static_cast<std::int16_t>(scalar);
            }
            else if (returnType == UDF_INT32)
            {
                *reinterpret_cast<std::int32_t*>(resultScalar) = static_cast<std::int32_t>(scalar);
            }
            else
            {
                *reinterpret_cast<std::int64_t*>(resultScalar) = static_cast<std::int64_t>(scalar);
            }
            return UDF_OK;
        }
        case UDF_UINT8:
        case UDF_UINT16:
        case UDF_UINT32:
        case UDF_UINT64: {
            const unsigned long long scalar = PyLong_AsUnsignedLongLong(result);
            if (PyErr_Occurred() != nullptr)
            {
                *err = fetchPythonError();
                return UDF_ERROR;
            }
            if (returnType == UDF_UINT8)
            {
                *reinterpret_cast<std::uint8_t*>(resultScalar) = static_cast<std::uint8_t>(scalar);
            }
            else if (returnType == UDF_UINT16)
            {
                *reinterpret_cast<std::uint16_t*>(resultScalar) = static_cast<std::uint16_t>(scalar);
            }
            else if (returnType == UDF_UINT32)
            {
                *reinterpret_cast<std::uint32_t*>(resultScalar) = static_cast<std::uint32_t>(scalar);
            }
            else
            {
                *reinterpret_cast<std::uint64_t*>(resultScalar) = static_cast<std::uint64_t>(scalar);
            }
            return UDF_OK;
        }
        case UDF_FLOAT32:
        case UDF_FLOAT64: {
            const double scalar = PyFloat_AsDouble(result);
            if (PyErr_Occurred() != nullptr)
            {
                *err = fetchPythonError();
                return UDF_ERROR;
            }
            if (returnType == UDF_FLOAT32)
            {
                *reinterpret_cast<float*>(resultScalar) = static_cast<float>(scalar);
            }
            else
            {
                *reinterpret_cast<double*>(resultScalar) = scalar;
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
                if (bytes == nullptr)
                {
                    *err = fetchPythonError();
                    return UDF_ERROR;
                }
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
        case UDF_CHAR: {
            const char* bytes = nullptr;
            Py_ssize_t length = 0;
            if (PyBytes_Check(result))
            {
                bytes = PyBytes_AsString(result);
                length = PyBytes_Size(result);
            }
            else if (PyUnicode_Check(result))
            {
                bytes = PyUnicode_AsUTF8AndSize(result, &length);
                if (bytes == nullptr)
                {
                    *err = fetchPythonError();
                    return UDF_ERROR;
                }
            }
            if (bytes == nullptr || length != 1)
            {
                *err = dupString("CHAR-returning UDF must return one byte, as bytes or str");
                return UDF_ERROR;
            }
            *reinterpret_cast<char*>(resultScalar) = bytes[0];
            return UDF_OK;
        }
        default:
            *err = dupString("unsupported UDF return type code");
            return UDF_ERROR;
    }
}

}

extern "C" UdfInstance* initialize_udf(const char* entrypoint, int argc, const int* arg_type_codes, int return_type_code, char** errormessage)
{
    if (errormessage != nullptr)
    {
        *errormessage = nullptr;
    }
    std::call_once(g_pyInit, initPythonOnce);
    if (!g_pyReady)
    {
        if (errormessage != nullptr)
        {
            *errormessage = dupString("the embedded Python interpreter could not be initialized");
        }
        return nullptr;
    }
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
        return nullptr;
    }

    PyObject* module = PyImport_ImportModule(full.substr(0, dot).c_str());
    if (module == nullptr)
    {
        if (errormessage != nullptr)
        {
            *errormessage = fetchPythonError();
        }
        PyGILState_Release(gil);
        return nullptr;
    }
    PyObject* function = PyObject_GetAttrString(module, full.substr(dot + 1).c_str());
    Py_DECREF(module);

    UdfCtx description;
    description.entrypoint = full;
    description.argTypes.assign(arg_type_codes, arg_type_codes + argc);
    description.returnType = return_type_code;
    UdfInstance* const udf = registerCallable(function, std::move(description), errormessage);
    PyGILState_Release(gil);
    return udf;
}

extern "C" UdfInstance* initialize_udf_from_source(
    const char* source, const char* function_name, int argc, const int* arg_type_codes, int return_type_code, char** errormessage)
{
    if (errormessage != nullptr)
    {
        *errormessage = nullptr;
    }
    std::call_once(g_pyInit, initPythonOnce);
    if (!g_pyReady)
    {
        if (errormessage != nullptr)
        {
            *errormessage = dupString("the embedded Python interpreter could not be initialized");
        }
        return nullptr;
    }
    const PyGILState_STATE gil = PyGILState_Ensure();

    const std::string sourceText = (source != nullptr) ? source : "";
    const std::string funcName = (function_name != nullptr) ? function_name : "";

    /* Executed in a fresh dict rather than imported as a module: each inline UDF gets its own private
     * namespace, so same-named helpers defined by two different inline UDF bodies never collide. */
    PyObject* moduleDict = PyDict_New();
    if (moduleDict == nullptr || PyDict_SetItemString(moduleDict, "__builtins__", PyEval_GetBuiltins()) != 0)
    {
        Py_XDECREF(moduleDict);
        if (errormessage != nullptr)
        {
            *errormessage = fetchPythonError();
        }
        PyGILState_Release(gil);
        return nullptr;
    }

    PyObject* execResult = PyRun_String(sourceText.c_str(), Py_file_input, moduleDict, moduleDict);
    if (execResult == nullptr)
    {
        Py_DECREF(moduleDict);
        if (errormessage != nullptr)
        {
            *errormessage = fetchPythonError();
        }
        PyGILState_Release(gil);
        return nullptr;
    }
    Py_DECREF(execResult);

    PyObject* function = PyDict_GetItemString(moduleDict, funcName.c_str()); /* borrowed */
    Py_XINCREF(function);
    Py_DECREF(moduleDict);

    UdfCtx description;
    description.source = sourceText;
    description.functionName = funcName;
    description.argTypes.assign(arg_type_codes, arg_type_codes + argc);
    description.returnType = return_type_code;
    UdfInstance* const udf = registerCallable(function, std::move(description), errormessage);
    PyGILState_Release(gil);
    return udf;
}

namespace
{
UdfCtx* findUdf(UdfInstance* udf)
{
    return reinterpret_cast<UdfCtx*>(udf);
}
}

extern "C" int execute_udf_row(
    UdfInstance* udf,
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
    UdfCtx* const ctx = findUdf(udf);
    if (ctx == nullptr)
    {
        if (errormessage != nullptr)
        {
            *errormessage = dupString("invalid UDF handle");
        }
        return UDF_ERROR;
    }

    const Attached attached;
    PyObject* const callable = callableFor(*ctx);
    if (callable == nullptr)
    {
        if (errormessage != nullptr)
        {
            *errormessage = fetchPythonError();
        }
        return UDF_ERROR;
    }
    const auto argc = static_cast<int>(ctx->argTypes.size());
    PyObject* args = PyTuple_New(argc);
    for (int i = 0; i < argc; ++i)
    {
        PyObject* arg
            = toPyArg(arg_values[i], arg_lens[i], arg_nulls[i], ctx->argTypes[i], ctx->bytesParameters[static_cast<size_t>(i)] != 0);
        if (arg == nullptr)
        {
            Py_DECREF(args);
            if (errormessage != nullptr)
            {
                *errormessage = (PyErr_Occurred() != nullptr) ? fetchPythonError() : dupString("unsupported argument type code");
            }
            PyErr_Clear();
            return UDF_ERROR;
        }
        PyTuple_SET_ITEM(args, i, arg); /* steals the reference */
    }

    PyObject* result = PyObject_CallObject(callable, args);
    Py_DECREF(args);
    if (result == nullptr)
    {
        /* The UDF raised: format it and report failure — the engine is never killed. */
        if (errormessage != nullptr)
        {
            *errormessage = fetchPythonError();
        }
        return UDF_ERROR;
    }

    char* resultError = nullptr;
    const int status = writeResult(result, ctx->returnType, result_scalar, result_string, result_string_len, result_null, &resultError);
    Py_DECREF(result);
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

extern "C" int execute_udf_batch(UdfInstance* udf, ArrowArray* arguments, ArrowArray* result, char** errormessage)
{
    *errormessage = nullptr;
    UdfCtx* const ctx = findUdf(udf);
    if (ctx == nullptr)
    {
        arguments->release(arguments);
        *errormessage = dupString("invalid UDF handle");
        return UDF_ERROR;
    }

    int status = UDF_ERROR;
    {
        const Attached attached;
        if (PyInterpreterState_Get() != PyInterpreterState_Main())
        {
            *errormessage = dupString("Arrow batches need the main interpreter, as pyarrow does not support subinterpreters");
        }
        else if (!prepareBatches(*ctx))
        {
            char* cause = fetchPythonError();
            *errormessage = dupString(
                std::string{"Arrow batches need pyarrow, e.g. from the venv of worker.python_udf.venv: "} + (cause != nullptr ? cause : ""));
            std::free(cause);
        }
        else
        {
            PyObject* const returned = PyObject_CallFunction(
                PyDict_GetItemString(g_batchNamespace, "_run"),
                "OOOOiKK",
                ctx->callable,
                ctx->argumentSchema,
                ctx->argumentTypeList,
                ctx->bytesParameterList,
                ctx->returnType,
                static_cast<unsigned long long>(reinterpret_cast<std::uintptr_t>(arguments)),
                static_cast<unsigned long long>(reinterpret_cast<std::uintptr_t>(result)));
            status = returned != nullptr ? UDF_OK : UDF_ERROR;
            if (returned == nullptr)
            {
                *errormessage = fetchPythonError();
            }
            Py_XDECREF(returned);
        }
    }

    /* pyarrow releases the arguments once it imported them, but not if it failed before. */
    if (arguments->release != nullptr)
    {
        arguments->release(arguments);
    }
    return status;
}

extern "C" int enter_udf_row(int isolated)
{
    if (isolated != 0 && enterIsolated())
    {
        return ENTERED_ISOLATED;
    }
    return static_cast<int>(ensureGil());
}

extern "C" void exit_udf_row(int state)
{
    if (state == ENTERED_ISOLATED)
    {
        PyEval_SaveThread();
        return;
    }
    PyGILState_Release(static_cast<PyGILState_STATE>(state));
}

extern "C" void cleanup_udf(UdfInstance* udf)
{
    const UdfCtx* const ctx = findUdf(udf);
    if (ctx == nullptr)
    {
        return;
    }
    const PyGILState_STATE gil = PyGILState_Ensure();
    Py_XDECREF(ctx->callable);
    Py_XDECREF(ctx->argumentSchema);
    Py_XDECREF(ctx->argumentTypeList);
    Py_XDECREF(ctx->bytesParameterList);
    PyGILState_Release(gil);
    delete ctx;
}

namespace
{
StageCtx* findStage(UdfStageInstance* stage)
{
    return reinterpret_cast<StageCtx*>(stage);
}

/* A new reference to a function that calls `functions`, one per UDF of `stage`, with the arguments of all UDFs one after the other
 * and returns their results as a tuple: `def fused(a0, a1, a2): return (f0(a0), f1(a1, a2),)`. Calling it is one call into Python
 * for all UDFs. nullptr with a Python error set on failure. GIL held. */
PyObject* fuse(const StageCtx& stage, const std::vector<PyObject*>& functions)
{
    std::string parameters;
    std::string calls;
    int argument = 0;
    for (size_t udf = 0; udf < functions.size(); ++udf)
    {
        std::string udfArguments;
        for (size_t i = 0; i < stage.udfs[udf]->argTypes.size(); ++i, ++argument)
        {
            const std::string name = "a" + std::to_string(argument);
            parameters += (argument == 0 ? "" : ", ") + name;
            udfArguments += (i == 0 ? "" : ", ") + name;
        }
        calls += "f" + std::to_string(udf) + "(" + udfArguments + "), ";
    }
    const std::string source = "def fused(" + parameters + "):\n    return (" + calls + ")\n";

    PyObject* globals = PyDict_New();
    if (globals == nullptr || PyDict_SetItemString(globals, "__builtins__", PyEval_GetBuiltins()) != 0)
    {
        Py_XDECREF(globals);
        return nullptr;
    }
    for (size_t udf = 0; udf < functions.size(); ++udf)
    {
        if (PyDict_SetItemString(globals, ("f" + std::to_string(udf)).c_str(), functions[udf]) != 0)
        {
            Py_DECREF(globals);
            return nullptr;
        }
    }
    PyObject* defined = PyRun_String(source.c_str(), Py_file_input, globals, globals);
    PyObject* fused = nullptr;
    if (defined != nullptr)
    {
        fused = PyDict_GetItemString(globals, "fused"); /* borrowed */
        Py_XINCREF(fused);
        Py_DECREF(defined);
    }
    Py_DECREF(globals);
    return fused;
}

/* The fused function of `stage` in the interpreter the thread is attached to, like callableFor. GIL held. */
PyObject* fusedFor(const StageCtx& stage)
{
    if (PyInterpreterState_Get() == PyInterpreterState_Main())
    {
        return stage.fused;
    }
    thread_local std::unordered_map<std::uint64_t, PyObject*> isolatedFused;
    if (const auto it = isolatedFused.find(stage.id); it != isolatedFused.end())
    {
        return it->second;
    }
    std::vector<PyObject*> functions;
    for (size_t udf = 0; udf < stage.udfs.size(); ++udf)
    {
        PyObject* function = callableFor(*stage.udfs[udf]);
        if (function == nullptr)
        {
            return nullptr;
        }
        functions.push_back(function);
    }
    PyObject* fused = fuse(stage, functions);
    if (fused != nullptr)
    {
        isolatedFused.emplace(stage.id, fused);
    }
    return fused;
}

/* Creates the batch objects of `stage` with its first batch, like prepareBatches. GIL of the main interpreter held. */
bool prepareStageBatches(StageCtx& stage)
{
    if (stage.argumentSchema != nullptr)
    {
        return true;
    }
    if (!defineBatchNamespace())
    {
        return false;
    }
    std::vector<int> argumentTypes;
    std::vector<int> resultTypes;
    PyObject* bytes = PyList_New(0);
    for (const auto* udf : stage.udfs)
    {
        argumentTypes.insert(argumentTypes.end(), udf->argTypes.begin(), udf->argTypes.end());
        resultTypes.push_back(udf->returnType);
        for (const char asBytes : udf->bytesParameters)
        {
            PyObject* flag = PyBool_FromLong(asBytes);
            if (bytes != nullptr && PyList_Append(bytes, flag) != 0)
            {
                Py_CLEAR(bytes);
            }
            Py_DECREF(flag);
        }
    }
    PyObject* types = typeCodeList(argumentTypes.data(), static_cast<int>(argumentTypes.size()));
    PyObject* results = typeCodeList(resultTypes.data(), static_cast<int>(resultTypes.size()));
    PyObject* schema
        = types != nullptr ? PyObject_CallFunctionObjArgs(PyDict_GetItemString(g_batchNamespace, "_schema"), types, nullptr) : nullptr;
    /* The calls above may have let another thread prepare the stage in the meantime. */
    if (types == nullptr || results == nullptr || bytes == nullptr || schema == nullptr || stage.argumentSchema != nullptr)
    {
        const bool prepared = stage.argumentSchema != nullptr;
        Py_XDECREF(types);
        Py_XDECREF(results);
        Py_XDECREF(bytes);
        Py_XDECREF(schema);
        return prepared;
    }
    stage.argumentTypeList = types;
    stage.resultTypeList = results;
    stage.bytesParameterList = bytes;
    stage.argumentSchema = schema;
    return true;
}
}

extern "C" UdfStageInstance* initialize_udf_stage(int count, UdfInstance* const* udfs, char** errormessage)
{
    *errormessage = nullptr;
    auto* stage = new StageCtx();
    stage->id = g_nextId++;
    for (int i = 0; i < count; ++i)
    {
        UdfCtx* const udf = findUdf(udfs[i]);
        if (udf == nullptr)
        {
            delete stage;
            *errormessage = dupString("invalid UDF handle");
            return nullptr;
        }
        stage->udfs.push_back(udf);
        stage->argumentCount += static_cast<int>(udf->argTypes.size());
    }

    /* The UDFs were initialized, so the interpreter is up. */
    const PyGILState_STATE gil = PyGILState_Ensure();
    std::vector<PyObject*> functions;
    for (const auto* udf : stage->udfs)
    {
        functions.push_back(udf->callable);
    }
    stage->fused = fuse(*stage, functions);
    if (stage->fused == nullptr)
    {
        *errormessage = fetchPythonError();
        PyGILState_Release(gil);
        delete stage;
        return nullptr;
    }
    PyGILState_Release(gil);
    return reinterpret_cast<UdfStageInstance*>(stage);
}

extern "C" int execute_udf_stage_row(
    UdfStageInstance* stage,
    const void* const* arg_values,
    const long long* arg_lens,
    const int* arg_nulls,
    void* result_scalars,
    char** result_strings,
    long long* result_string_lens,
    int* result_nulls,
    char** errormessage)
{
    *errormessage = nullptr;
    StageCtx* const ctx = findStage(stage);
    if (ctx == nullptr)
    {
        *errormessage = dupString("invalid UDF stage handle");
        return UDF_ERROR;
    }

    const Attached attached;
    PyObject* const fused = fusedFor(*ctx);
    if (fused == nullptr)
    {
        *errormessage = fetchPythonError();
        return UDF_ERROR;
    }
    PyObject* args = PyTuple_New(ctx->argumentCount);
    int argument = 0;
    for (const auto* udf : ctx->udfs)
    {
        for (size_t i = 0; i < udf->argTypes.size(); ++i, ++argument)
        {
            PyObject* arg = toPyArg(
                arg_values[argument], arg_lens[argument], arg_nulls[argument], udf->argTypes[i], udf->bytesParameters[i] != 0);
            if (arg == nullptr)
            {
                Py_DECREF(args);
                *errormessage = (PyErr_Occurred() != nullptr) ? fetchPythonError() : dupString("unsupported argument type code");
                PyErr_Clear();
                return UDF_ERROR;
            }
            PyTuple_SET_ITEM(args, argument, arg); /* steals the reference */
        }
    }

    PyObject* results = PyObject_CallObject(fused, args);
    Py_DECREF(args);
    if (results == nullptr)
    {
        *errormessage = fetchPythonError();
        return UDF_ERROR;
    }

    int status = UDF_OK;
    for (size_t udf = 0; udf < ctx->udfs.size(); ++udf)
    {
        result_strings[udf] = nullptr;
        result_nulls[udf] = 0;
    }
    for (size_t udf = 0; udf < ctx->udfs.size() && status == UDF_OK; ++udf)
    {
        char* resultError = nullptr;
        status = writeResult(
            PyTuple_GET_ITEM(results, static_cast<Py_ssize_t>(udf)),
            ctx->udfs[udf]->returnType,
            static_cast<char*>(result_scalars) + (udf * 8),
            &result_strings[udf],
            &result_string_lens[udf],
            &result_nulls[udf],
            &resultError);
        if (status == UDF_ERROR)
        {
            *errormessage = (resultError != nullptr) ? resultError : dupString("udf result marshalling error");
        }
    }
    Py_DECREF(results);
    if (status == UDF_ERROR)
    {
        for (size_t udf = 0; udf < ctx->udfs.size(); ++udf)
        {
            std::free(result_strings[udf]);
            result_strings[udf] = nullptr;
        }
    }
    return status;
}

extern "C" int execute_udf_stage_batch(UdfStageInstance* stage, ArrowArray* arguments, ArrowArray* result, char** errormessage)
{
    *errormessage = nullptr;
    StageCtx* const ctx = findStage(stage);
    if (ctx == nullptr)
    {
        arguments->release(arguments);
        *errormessage = dupString("invalid UDF stage handle");
        return UDF_ERROR;
    }

    int status = UDF_ERROR;
    {
        const Attached attached;
        if (PyInterpreterState_Get() != PyInterpreterState_Main())
        {
            *errormessage = dupString("Arrow batches need the main interpreter, as pyarrow does not support subinterpreters");
        }
        else if (!prepareStageBatches(*ctx))
        {
            char* cause = fetchPythonError();
            *errormessage = dupString(
                std::string{"Arrow batches need pyarrow, e.g. from the venv of worker.python_udf.venv: "} + (cause != nullptr ? cause : ""));
            std::free(cause);
        }
        else
        {
            PyObject* const returned = PyObject_CallFunction(
                PyDict_GetItemString(g_batchNamespace, "_run_fused"),
                "OOOOOKK",
                ctx->fused,
                ctx->argumentSchema,
                ctx->argumentTypeList,
                ctx->bytesParameterList,
                ctx->resultTypeList,
                static_cast<unsigned long long>(reinterpret_cast<std::uintptr_t>(arguments)),
                static_cast<unsigned long long>(reinterpret_cast<std::uintptr_t>(result)));
            status = returned != nullptr ? UDF_OK : UDF_ERROR;
            if (returned == nullptr)
            {
                *errormessage = fetchPythonError();
            }
            Py_XDECREF(returned);
        }
    }

    /* pyarrow releases the arguments once it imported them, but not if it failed before. */
    if (arguments->release != nullptr)
    {
        arguments->release(arguments);
    }
    return status;
}

extern "C" void cleanup_udf_stage(UdfStageInstance* stage)
{
    const StageCtx* const ctx = findStage(stage);
    if (ctx == nullptr)
    {
        return;
    }
    const PyGILState_STATE gil = PyGILState_Ensure();
    Py_XDECREF(ctx->fused);
    Py_XDECREF(ctx->argumentSchema);
    Py_XDECREF(ctx->argumentTypeList);
    Py_XDECREF(ctx->bytesParameterList);
    Py_XDECREF(ctx->resultTypeList);
    PyGILState_Release(gil);
    delete ctx;
}

/// NOLINTEND(google-runtime-int,cppcoreguidelines-owning-memory,cppcoreguidelines-no-malloc,misc-include-cleaner,cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic,cppcoreguidelines-pro-type-const-cast,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-pro-bounds-avoid-unchecked-container-access,readability-identifier-naming,concurrency-mt-unsafe)

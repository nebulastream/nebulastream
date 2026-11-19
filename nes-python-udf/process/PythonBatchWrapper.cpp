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

#include <PythonBatchWrapper.hpp>

#include <cstddef>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <PythonArrowColumns.hpp>
#include <UdfProcessCall.hpp>

namespace NES
{

namespace
{
/// A Python expression for arbitrary text, safe to splice into the generated code.
std::string pythonString(const std::string_view text)
{
    std::string hex;
    hex.reserve(text.size() * 2);
    for (const char character : text)
    {
        hex += fmt::format("{:02x}", static_cast<unsigned char>(character));
    }
    return fmt::format("bytes.fromhex(\"{}\").decode()", hex);
}

std::string pythonConstant(const UdfProcessArgument& argument)
{
    const auto text = pythonString(argument.constant.value());
    if (argument.constantType == "VARSIZED" || argument.constantType == "CHAR")
    {
        return argument.bytes ? fmt::format("{}.encode()", text) : text;
    }
    if (argument.constantType == "BOOLEAN")
    {
        return fmt::format("{}.upper() == \"TRUE\"", text);
    }
    if (argument.constantType == "FLOAT32" || argument.constantType == "FLOAT64")
    {
        return fmt::format("float({})", text);
    }
    return fmt::format("int({})", text);
}

std::string defineUdf(const size_t index, const UdfProcessCall& call)
{
    if (call.body.has_value())
    {
        auto definition = fmt::format("def _udf_{}({}):\n", index, fmt::join(call.parameterNames, ", "));
        /// One level deeper than the body's own indentation, whichever that is.
        for (const auto line : std::views::split(std::string_view{*call.body}, '\n'))
        {
            definition += fmt::format("    {}\n", std::string_view{line});
        }
        return definition;
    }
    const auto lastDot = call.entrypoint.rfind('.');
    if (lastDot == std::string::npos)
    {
        throw CannotLoadUdf("UDF '{}' needs ENTRYPOINT 'module.function', got '{}'", call.name, call.entrypoint);
    }
    return fmt::format(
        "_udf_{} = getattr(importlib.import_module({}), {})\n",
        index,
        pythonString(std::string_view{call.entrypoint}.substr(0, lastDot)),
        pythonString(std::string_view{call.entrypoint}.substr(lastDot + 1)));
}

/// The Python values of column `index` of an ARROW_IPC batch, as a list. A VARSIZED column becomes bytes or, failing on invalid
/// UTF-8, str.
std::string readArrowColumn(const size_t index, const ArrowBufferRef::Field& field, const bool asBytes)
{
    if (field.type.type == DataType::Type::VARSIZED)
    {
        return fmt::format("_binary_column(batch.column({}), {})", index, asBytes ? "True" : "False");
    }
    if (field.type.type == DataType::Type::CHAR)
    {
        return fmt::format("_char_column(batch.column({}), {})", index, asBytes ? "True" : "False");
    }
    return fmt::format("batch.column({}).to_pylist()", index);
}

/// A Python expression turning the list `r` of results into the Arrow array of column `index`.
std::string arrowResultColumn(const size_t index, const ArrowBufferRef::Field& field)
{
    switch (field.type.type)
    {
        case DataType::Type::VARSIZED:
            return "pa.array([None if v is None else _binary(v) for v in r], type=pa.binary())";
        case DataType::Type::CHAR:
            return "pa.array([None if v is None else _char(v) for v in r], type=pa.binary(1))";
        case DataType::Type::BOOLEAN:
            return "pa.array([None if v is None else bool(v) for v in r], type=pa.bool_())";
        default:
            return fmt::format("pa.array(r, type=batch.schema.field({}).type)", index);
    }
}

/// The `array` type code of a fixed-width number, empty for every other type.
std::string_view arrayCode(const ArrowBufferRef::Field& field)
{
    switch (field.type.type)
    {
        case DataType::Type::INT8:
            return "b";
        case DataType::Type::UINT8:
            return "B";
        case DataType::Type::INT16:
            return "h";
        case DataType::Type::UINT16:
            return "H";
        case DataType::Type::INT32:
            return "i";
        case DataType::Type::UINT32:
            return "I";
        case DataType::Type::INT64:
            return "q";
        case DataType::Type::UINT64:
            return "Q";
        case DataType::Type::FLOAT32:
            return "f";
        case DataType::Type::FLOAT64:
            return "d";
        case DataType::Type::BOOLEAN:
        case DataType::Type::VARSIZED:
        case DataType::Type::CHAR:
            return "";
        case DataType::Type::UNDEFINED:
            break;
    }
    throw CannotLoadUdf("UDF processes do not support fields of type {}", field.type);
}

/// The bytes of a value of a fixed-width number, 1 for CHAR; only for fields whose data region holds one value per row.
size_t valueSize(const ArrowBufferRef::Field& field)
{
    switch (field.type.type)
    {
        case DataType::Type::INT8:
        case DataType::Type::UINT8:
        case DataType::Type::CHAR:
            return 1;
        case DataType::Type::INT16:
        case DataType::Type::UINT16:
            return 2;
        case DataType::Type::INT32:
        case DataType::Type::UINT32:
        case DataType::Type::FLOAT32:
            return 4;
        default:
            return 8;
    }
}

bool isVarSized(const ArrowBufferRef::Field& field)
{
    return field.type.type == DataType::Type::VARSIZED;
}

/// The Arrow buffers of a field: validity and data, and for VARSIZED validity, offsets and values.
size_t numberOfBuffers(const ArrowBufferRef::Field& field)
{
    return isVarSized(field) ? 3 : 2;
}

/// The names the generated code gives the buffers of field `index`: its validity, its data or offsets, and its VARSIZED values.
std::string validityOf(const size_t index)
{
    return fmt::format("v{}", index);
}

std::string dataOf(const size_t index)
{
    return fmt::format("d{}", index);
}

std::string valuesOf(const size_t index)
{
    return fmt::format("s{}", index);
}

/// Binds the buffers of field `index` to their names, as RAW_BUFFERS and SHARED_MEMORY frame them: a flat list of the buffers of
/// all fields, in which the field starts at `position`.
std::string bindFramedBuffers(const size_t index, const ArrowBufferRef::Field& field, const size_t position)
{
    if (isVarSized(field))
    {
        return fmt::format(
            "    {}, {}, {} = buffers[{}], buffers[{}], buffers[{}]\n",
            validityOf(index),
            dataOf(index),
            valuesOf(index),
            position,
            position + 1,
            position + 2);
    }
    return fmt::format("    {}, {} = buffers[{}], buffers[{}]\n", validityOf(index), dataOf(index), position, position + 1);
}

/// Binds the buffers of field `index` to their names with SHARED_MEMORY_ZERO_COPY, where the header of the batch says where they are.
/// `readsValues` is false for a result, whose VARSIZED values do not exist yet.
std::string bindPoolBuffers(const size_t index, const ArrowBufferRef::Field& field, const bool readsValues)
{
    const auto location = [&](const size_t region)
    { return fmt::format("header[{}], header[{}]", (6 * index) + (2 * region), (6 * index) + (2 * region) + 1); };
    /// A column that cannot hold NULLs has no bitmap
    auto code = field.type.nullable ? fmt::format("    {} = _view(bases, {}, bitmap)\n", validityOf(index), location(0)) : std::string{};
    if (isVarSized(field))
    {
        code += fmt::format("    {} = _view(bases, {}, (n + 1) * 4)\n", dataOf(index), location(1));
        if (readsValues)
        {
            code += fmt::format("    {0} = _view(bases, {1}, _end({2}, n))\n", valuesOf(index), location(2), dataOf(index));
        }
    }
    else if (field.type.type == DataType::Type::BOOLEAN)
    {
        code += fmt::format("    {} = _view(bases, {}, bitmap)\n", dataOf(index), location(1));
    }
    else
    {
        code += fmt::format("    {} = _view(bases, {}, n * {})\n", dataOf(index), location(1), valueSize(field));
    }
    return code;
}

/// A Python expression for the values of the argument column `index`: what the schema says the column holds decides how it is read,
/// so the process does not ask per batch. `reader` names the function that reads a VARSIZED or CHAR column as bytes or as str, which
/// BYTES in the declaration of the UDF decides.
std::string readColumn(const size_t index, const ArrowBufferRef::Field& field, const std::string_view reader)
{
    /// A column that cannot hold NULLs has no bitmap to look at.
    const auto validity = field.type.nullable ? validityOf(index) : std::string{"_EMPTY"};
    if (isVarSized(field))
    {
        return fmt::format("{}({}, {}, {}, n)", reader, validity, dataOf(index), valuesOf(index));
    }
    if (field.type.type == DataType::Type::CHAR)
    {
        return fmt::format("{}({}, {}, n)", reader, validity, dataOf(index));
    }
    const auto values = field.type.type == DataType::Type::BOOLEAN ? fmt::format("_bools({}, n)", dataOf(index))
                                                                   : fmt::format("_fixed({}, \"{}\")", dataOf(index), arrayCode(field));
    return field.type.nullable ? fmt::format("_nullable({}, {}, n)", values, validity) : values;
}

/// The function that reads a VARSIZED or CHAR column as bytes and the one that reads it as str.
std::pair<std::string_view, std::string_view> readers(const ArrowBufferRef::Field& field)
{
    return isVarSized(field) ? std::pair<std::string_view, std::string_view>{"_binaries", "_strings"}
                             : std::pair<std::string_view, std::string_view>{"_char_bytes", "_char_strings"};
}

/// A Python expression for the buffers of result column `index`, made of the list `r` of results, as RAW_BUFFERS and SHARED_MEMORY
/// send them back.
std::string framedResult(const ArrowBufferRef::Field& field)
{
    switch (field.type.type)
    {
        case DataType::Type::VARSIZED:
            return "_result_binary(r)";
        case DataType::Type::CHAR:
            return "_result_char(r)";
        case DataType::Type::BOOLEAN:
            return "_result_bool(r)";
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
            return fmt::format("_result_fixed(r, \"{}\", 0.0)", arrayCode(field));
        default:
            return fmt::format("_result_fixed(r, \"{}\", 0)", arrayCode(field));
    }
}

/// A Python expression that writes the list `r` of results into result column `index` of the batch, see SHARED_MEMORY_IN_PLACE.
std::string placedResult(const size_t index, const ArrowBufferRef::Field& field)
{
    switch (field.type.type)
    {
        case DataType::Type::VARSIZED:
            return fmt::format("_place_binary(r, {}, {})", validityOf(index), dataOf(index));
        case DataType::Type::CHAR:
            return fmt::format("_place_char(r, {}, {})", validityOf(index), dataOf(index));
        case DataType::Type::BOOLEAN:
            return fmt::format("_place_bool(r, {}, {})", validityOf(index), dataOf(index));
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
            return fmt::format("_place_fixed(r, \"{}\", 0.0, {}, {})", arrayCode(field), validityOf(index), dataOf(index));
        default:
            return fmt::format("_place_fixed(r, \"{}\", 0, {}, {})", arrayCode(field), validityOf(index), dataOf(index));
    }
}

/// Reads and writes the columns of RAW_BUFFERS and SHARED_MEMORY batches in plain Python, see generatePythonBatchWrapper. Each
/// function handles one kind of column; the generated _process_batch calls the ones the schema asks for. The readers go through
/// `array`, as indexing and converting a memoryview is an order of magnitude slower on PyPy.
constexpr std::string_view RAW_COLUMNS = R"(

import array

_EMPTY = memoryview(b"")


def _bits(data, n):
    return [(data[i >> 3] >> (i & 7)) & 1 for i in range(n)]


def _pack_bits(flags):
    packed = bytearray((len(flags) + 7) // 8)
    for i, flag in enumerate(flags):
        if flag:
            packed[i >> 3] |= 1 << (i & 7)
    return bytes(packed)


def _nullable(values, validity, n):
    # An empty bitmap means that no value is NULL
    if len(validity):
        return [v if valid else None for v, valid in zip(values, _bits(validity, n))]
    return values


def _fixed(data, code):
    values = array.array(code)
    values.frombytes(data)
    return values


def _bools(data, n):
    return [bool(bit) for bit in _bits(data, n)]


def _binaries(validity, offsets, data, n):
    offsets = _fixed(offsets, "i")
    data = bytes(data)
    return _nullable([data[start:end] for start, end in zip(offsets, offsets[1:])], validity, n)


def _strings(validity, offsets, data, n):
    # Decoding validates UTF-8. ASCII values are decoded at once: the offsets then also are positions in the text.
    offsets = _fixed(offsets, "i")
    data = bytes(data)
    try:
        text = data.decode()
    except UnicodeDecodeError:
        text = ""
    if len(text) == len(data):
        return _nullable([text[start:end] for start, end in zip(offsets, offsets[1:])], validity, n)
    values = _nullable([data[start:end] for start, end in zip(offsets, offsets[1:])], validity, n)
    return [None if v is None else v.decode() for v in values]


def _char_bytes(validity, data, n):
    data = bytes(data)
    return _nullable([data[i:i + 1] for i in range(n)], validity, n)


def _char_strings(validity, data, n):
    # A single byte only decodes if it is ASCII
    return [None if v is None else v.decode() for v in _char_bytes(validity, data, n)]


def _validity(r):
    return _pack_bits([v is not None for v in r]) if None in r else b""


def _numbers(r, code, zero):
    if None in r:
        r = [zero if v is None else v for v in r]
    return array.array(code, r)


def _offsets(values):
    offsets = array.array("i", [0]) * (len(values) + 1)
    total = 0
    for i, value in enumerate(values):
        total += len(value)
        offsets[i + 1] = total
    if total >= 2 ** 31:
        raise ValueError("the VARSIZED results of a batch exceed 2 GiB")
    return offsets


def _result_fixed(r, code, zero):
    return _validity(r), _numbers(r, code, zero).tobytes()


def _result_bool(r):
    return _validity(r), _pack_bits([v is not None and bool(v) for v in r])


def _result_char(r):
    return _validity(r), b"".join(b"\0" if v is None else _char(v) for v in r)


def _result_binary(r):
    values = [b"" if v is None else _binary(v) for v in r]
    return _validity(r), _offsets(values).tobytes(), b"".join(values)


)";

constexpr std::string_view RUNTIME = R"(

def _failure(error):
    message = "".join(traceback.format_exception_only(type(error), error)).strip().encode()
    sock.sendall(struct.pack("<QQ", 0, len(message)) + message)


def _main():
    reader = pa.ipc.open_stream(sock.makefile("rb"))
    # Large enough for a whole answer, which then reaches the worker in one write
    output = sock.makefile("wb", buffering=1 << 20)
    writer = None
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            return
        try:
            result = _process_batch(batch)
        except Exception as error:
            _failure(error)
            continue
        output.write(struct.pack("<Q", 1))
        if writer is None:
            writer = pa.ipc.new_stream(output, result.schema)
        writer.write_batch(result)
        output.flush()


try:
    import pyarrow as pa
    _define_udfs()
except BaseException as error:
    _failure(error)
    raise SystemExit(1)
sock.sendall(struct.pack("<Q", 1))
_main()
)";

constexpr std::string_view RAW_RUNTIME = R"(

def _failure(error):
    message = "".join(traceback.format_exception_only(type(error), error)).strip().encode()
    sock.sendall(struct.pack("<QQ", 0, len(message)) + message)


def _frame(buffers):
    parts = []
    for buffer in buffers:
        size = len(buffer)
        parts.append(struct.pack("<Q", size))
        parts.append(buffer)
        if size % 8:
            parts.append(bytes(8 - size % 8))
    return b"".join(parts)


def _main():
    reader = sock.makefile("rb")
    while True:
        header = reader.read(16)
        if len(header) < 16:
            return
        rows, size = struct.unpack("<QQ", header)
        body = memoryview(reader.read(size))
        if len(body) < size:
            return
        buffers = []
        position = 0
        while position < size:
            (length,) = struct.unpack_from("<Q", body, position)
            buffers.append(body[position + 8:position + 8 + length])
            position += 8 + (length + 7) // 8 * 8
        try:
            payload = _frame(_process_batch(rows, buffers))
        except Exception as error:
            _failure(error)
            continue
        # One write per answer, which the worker then reads with one call
        sock.sendall(struct.pack("<QQQ", 1, rows, len(payload)) + payload)


try:
    _define_udfs()
except BaseException as error:
    _failure(error)
    raise SystemExit(1)
sock.sendall(struct.pack("<Q", 1))
_main()
)";

/// Exchanges batches through the rings in the shared memory of the process, its file descriptor 4, see UdfProcessProtocol::SHARED_MEMORY.
/// The socket only carries the start-up and wake-ups: each side spins for a while when it has nothing to do, then sleeps, and the other
/// side writes a byte to the socket only if it finds it asleep. Python has no fences, so a wake-up can be missed when a side goes to
/// sleep just as the other publishes; that costs at most one sleep interval, after which the sleeper checks the rings again.
/// SHARED_MEMORY_COPY or SHARED_MEMORY_IN_PLACE follow, then SHARED_MEMORY_START.
constexpr std::string_view SHARED_MEMORY_COMMON = R"(

import mmap, select, time

_CONTROL = 4096
# Words of the control block, each on a cache line of its own
_REQUEST_TAIL, _REQUEST_HEAD, _ANSWER_TAIL, _ANSWER_HEAD, _PROCESS_SLEEPS, _WORKER_SLEEPS = 0, 8, 16, 24, 32, 40
_WRAP = 0xFFFFFFFFFFFFFFFF
_SPIN_SECONDS = 50e-6
# A process spins longer while requests come in short gaps, up to this long, so that it does not need to be woken for each
_MAX_SPIN_SECONDS = 2e-4
_SLEEP_SECONDS = 1e-3
_gap = 0.0

# A host process that serves several UDF operators sets _SHARED_FD, _POOL and _HOSTED before it runs this program, see
# PythonProcesses::start; a process of its own has the shared memory as file descriptor 4 and the pool as 5.
_shared = mmap.mmap(globals().get("_SHARED_FD", 4), 0)
_ring = (len(_shared) - _CONTROL) // 2
# Aligned uint64 words, which a memoryview reads and writes with single loads and stores
_control = memoryview(_shared)[:_CONTROL].cast("Q")
_requests = memoryview(_shared)[_CONTROL:_CONTROL + _ring]
_answers = memoryview(_shared)[_CONTROL + _ring:]


def _failure(error):
    message = "".join(traceback.format_exception_only(type(error), error)).strip().encode()
    sock.sendall(struct.pack("<QQ", 0, len(message)) + message)


def _woken():
    # False once the worker closed the socket
    try:
        return bool(sock.recv(4096))
    except BlockingIOError:
        return True
    except ConnectionError:
        return False


def _wake_worker():
    if _control[_WORKER_SLEEPS]:
        _control[_WORKER_SLEEPS] = 0
        try:
            sock.send(b"\x01")
        except (BlockingIOError, ConnectionError):
            # A worker that closed the socket needs no wake-up; the next wait for a request sees the closed socket
            pass


def _await_request(head):
    # Spins for twice the usual wait for a request if that is short, as a wake-up costs both sides a system call, otherwise briefly.
    # Requests that are already there do not count, as they come in bursts.
    global _gap
    if _control[_REQUEST_TAIL] != head:
        return True
    spin = 2 * _gap if 2 * _gap <= _MAX_SPIN_SECONDS else _SPIN_SECONDS
    started = time.perf_counter()
    spins = 0
    deadline = None
    while _control[_REQUEST_TAIL] == head:
        spins += 1
        if spins & 63:
            continue
        now = time.perf_counter()
        if deadline is None:
            deadline = now + max(spin, _SPIN_SECONDS)
            continue
        if now < deadline:
            continue
        _control[_PROCESS_SLEEPS] = 1
        if _control[_REQUEST_TAIL] == head:
            readable, _, _ = select.select([sock], [], [], _SLEEP_SECONDS)
            if readable and not _woken():
                return False
        _control[_PROCESS_SLEEPS] = 0
        deadline = None
    _gap += (min(time.perf_counter() - started, 10 * _MAX_SPIN_SECONDS) - _gap) / 8
    return True


def _await_space(tail, needed):
    while _ring - (tail - _control[_ANSWER_HEAD]) < needed:
        _wake_worker()
        time.sleep(_SPIN_SECONDS)


def _reserve(tail, record):
    # The tail and position of an answer of `record` bytes once there is room for it, after the end of the ring if it does not fit there
    if record > _ring:
        raise SystemExit("an answer of " + str(record) + " bytes does not fit the shared memory of " + str(_ring) + " bytes")
    position = tail % _ring
    skip = _ring - position if _ring - position < record else 0
    _await_space(tail, skip + record)
    if skip:
        struct.pack_into("<Q", _answers, position, _WRAP)
        tail += skip
        position = 0
    return tail, position


def _publish(tail):
    _control[_ANSWER_TAIL] = tail
    _wake_worker()
    return tail


def _failed(tail, error):
    # [record size][0][message length][0][message, padded to 8 bytes]
    message = "".join(traceback.format_exception_only(type(error), error)).strip().encode()
    record = 32 + (len(message) + 7) // 8 * 8
    tail, position = _reserve(tail, record)
    struct.pack_into("<QQQQ", _answers, position, record, 0, len(message), 0)
    _answers[position + 32:position + 32 + len(message)] = message
    return _publish(tail + record)


def _bytes(buffer):
    view = memoryview(buffer)
    return view if view.format == "B" else view.cast("B")
)";

/// SHARED_MEMORY: the columns travel in the rings, framed as in RAW_BUFFERS.
constexpr std::string_view SHARED_MEMORY_COPY = R"(

def _answer(tail, rows, buffers):
    # [record size][1][rows][body size], then the body
    pieces = [_bytes(buffer) for buffer in buffers]
    size = sum(8 + (view.nbytes + 7) // 8 * 8 for view in pieces)
    record = 32 + size
    tail, position = _reserve(tail, record)
    struct.pack_into("<QQQQ", _answers, position, record, 1, rows, size)
    offset = position + 32
    for view in pieces:
        struct.pack_into("<Q", _answers, offset, view.nbytes)
        _answers[offset + 8:offset + 8 + view.nbytes] = view
        offset += 8 + (view.nbytes + 7) // 8 * 8
    return _publish(tail + record)


def _step():
    # Handles the next request; False if there is none
    global _head, _tail
    head = _head
    if _control[_REQUEST_TAIL] == head:
        return False
    position = head % _ring
    (record,) = struct.unpack_from("<Q", _requests, position)
    if record == _WRAP:
        _head = head + _ring - position
        _control[_REQUEST_HEAD] = _head
        return True
    rows, size = struct.unpack_from("<QQ", _requests, position + 8)
    # The buffers stay in the ring, which the worker reuses only after the answer, as the unchanged columns go back from there
    body = _requests[position + 24:position + 24 + size]
    buffers = []
    offset = 0
    while offset < size:
        (length,) = struct.unpack_from("<Q", body, offset)
        buffers.append(body[offset + 8:offset + 8 + length])
        offset += 8 + (length + 7) // 8 * 8
    try:
        _tail = _answer(_tail, rows, _process_batch(rows, buffers))
    except Exception as error:
        _tail = _failed(_tail, error)
    _head = head + record
    _control[_REQUEST_HEAD] = _head
    return True
)";

/// SHARED_MEMORY_ZERO_COPY: the pool of the worker is the file descriptor 5, and the rings only say where the columns are in it.
/// A request is [record size][rows][children][value area offset][value area size][buffer offset], then per child buffer [offset]
/// [length], then the children that are not in the pool, whose offset has the bit _INLINE set and counts from there. The process reads
/// where the columns are from the header of the buffer, see ArrowBufferRef; the generated _process_batch only looks up the columns
/// that its UDFs read or write. A value area is a buffer of the pool for VARSIZED results, used after the ones before it are full; a
/// size of 0 means that the request brings none.
/// The results go into their columns in the batch, see _place_fixed and the like. An answer is [record size][1][rows][count], then per
/// call [nulls][where][offset][length]: whether the bitmap holds NULLs, and where the VARSIZED values are: 0 for none, 1 for `offset`
/// in the pool, 2 for `offset` behind the entries, if they fit no value area.
/// _HEADER, the header of a buffer, precedes it: per field [buffer][offset] as uint32 for its validity, data and values, see
/// ArrowBufferRef::Location.
constexpr std::string_view SHARED_MEMORY_IN_PLACE = R"(

import collections

_pool = globals().get("_POOL") or memoryview(mmap.mmap(5, 0))
_INLINE = 1 << 63
_NONE = 0xFFFFFFFF


def _view(bases, buffer, offset, size):
    # bases holds where the buffer and its children are
    if buffer == _NONE or size == 0:
        return _EMPTY
    view, start = bases[buffer]
    return view[start + offset:start + offset + size]


def _end(offsets, n):
    # The values end where the last offset says
    return struct.unpack_from("<i", offsets, 4 * n)[0] if len(offsets) else 0


_areas = collections.deque()
_area_used = 0


def _values(data):
    # (where, offset, length, data that goes into the answer)
    global _area_used
    size = len(data)
    if size == 0:
        return 0, 0, 0, None
    if _areas and size <= _areas[-1][1]:
        while True:
            start, area_size = _areas[0]
            if _area_used + size <= area_size:
                offset = start + _area_used
                _pool[offset:offset + size] = data
                _area_used += (size + 7) // 8 * 8
                return 1, offset, size, None
            if len(_areas) == 1:
                break
            _areas.popleft()
            _area_used = 0
    return 2, 0, size, data


def _place_nulls(r, validity):
    # Writes the bitmap of a result column only if there are NULLs, as the worker drops it otherwise
    nulls = None in r
    if nulls:
        validity[:] = _pack_bits([v is not None for v in r])
    return nulls


def _place_fixed(r, code, zero, validity, data):
    nulls = _place_nulls(r, validity)
    data.cast(code)[:] = _numbers(r, code, zero)
    return nulls, 0, 0, 0, None


def _place_bool(r, validity, data):
    nulls = _place_nulls(r, validity)
    data[:] = _pack_bits([v is not None and bool(v) for v in r])
    return nulls, 0, 0, 0, None


def _place_char(r, validity, data):
    nulls = _place_nulls(r, validity)
    data[:] = b"".join(b"\0" if v is None else _char(v) for v in r)
    return nulls, 0, 0, 0, None


def _place_binary(r, validity, offsets):
    nulls = _place_nulls(r, validity)
    values = [b"" if v is None else _binary(v) for v in r]
    offsets.cast("i")[:] = _offsets(values)
    return (nulls,) + _values(b"".join(values))


def _answer(tail, rows, placed):
    inline = []
    inline_size = 0
    entries = []
    for nulls, where, offset, length, data in placed:
        if data is not None:
            offset = inline_size
            inline.append(data)
            inline_size += (length + 7) // 8 * 8
        entries.append((1 if nulls else 0, where, offset, length))
    record = 32 + 32 * len(entries) + inline_size
    tail, position = _reserve(tail, record)
    struct.pack_into("<QQQQ", _answers, position, record, 1, rows, len(entries))
    offset = position + 32
    for entry in entries:
        struct.pack_into("<QQQQ", _answers, offset, *entry)
        offset += 32
    for data in inline:
        _answers[offset:offset + len(data)] = data
        offset += (len(data) + 7) // 8 * 8
    return _publish(tail + record)


def _step():
    # Handles the next request; False if there is none
    global _head, _tail
    head = _head
    if _control[_REQUEST_TAIL] == head:
        return False
    position = head % _ring
    (record,) = struct.unpack_from("<Q", _requests, position)
    if record == _WRAP:
        _head = head + _ring - position
        _control[_REQUEST_HEAD] = _head
        return True
    rows, children, area, area_size, main = struct.unpack_from("<QQQQQ", _requests, position + 8)
    if area_size:
        _areas.append((area, area_size))
    entries = position + 48
    inline = entries + 16 * children
    bases = [(_pool, main)]
    for index in range(children):
        offset, _ = struct.unpack_from("<QQ", _requests, entries + 16 * index)
        bases.append((_requests, inline + offset - _INLINE) if offset & _INLINE else (_pool, offset))
    try:
        _tail = _answer(_tail, rows, _process_batch(rows, bases))
    except Exception as error:
        _tail = _failed(_tail, error)
    _head = head + record
    _control[_REQUEST_HEAD] = _head
    return True
)";

constexpr std::string_view SHARED_MEMORY_START = R"(

# The bytes of the request ring this process took, and of the answer ring it published
_head = 0
_tail = 0


def _main():
    sock.setblocking(False)
    while _await_request(_head):
        _step()


try:
    _define_udfs()
except BaseException as error:
    _failure(error)
    raise SystemExit(1)
sock.sendall(struct.pack("<Q", 1))
# A host process calls _step itself, in turn with the ones of its other UDF operators
if not globals().get("_HOSTED"):
    _main()
)";
}

std::string
generatePythonBatchWrapper(const ArrowBufferRef& layout, const std::vector<UdfProcessCall>& calls, const UdfProcessProtocol protocol)
{
    const auto& fields = layout.getFields();

    std::string program = "import importlib, os, struct, sys, traceback\n\n"
                          "sys.path[:0] = [path for path in os.environ.get(\"NES_UDF_PATH\", \"\").split(\":\") if path]\n\n\n"
                          "def _define_udfs():\n"
                          "    namespace = globals()\n"
                          "    exec(_UDF_SOURCE, namespace)\n\n\n";
    /// _binary and _char, which check a VARSIZED or CHAR result, do not use pyarrow.
    program += PythonArrowColumnsSource;
    /// SHARED_MEMORY frames the bodies of its batches as RAW_BUFFERS does.
    const bool raw = protocol != UdfProcessProtocol::ARROW_IPC;
    /// SHARED_MEMORY_ZERO_COPY writes the results into their columns and returns what _answer needs of each.
    const bool inPlace = protocol == UdfProcessProtocol::SHARED_MEMORY_ZERO_COPY;
    if (raw)
    {
        program += RAW_COLUMNS;
    }

    /// Where the buffers of each field start in the flat list of a framed batch
    std::vector<size_t> framedPositions;
    size_t framedBuffers = 0;
    for (const auto& field : fields)
    {
        framedPositions.push_back(framedBuffers);
        framedBuffers += raw ? numberOfBuffers(field) : 0;
    }

    std::string udfs;
    std::string body;
    if (inPlace)
    {
        body = "def _process_batch(n, bases):\n"
               "    header = _HEADER.unpack_from(*bases[0])\n"
               "    bitmap = (n + 7) // 8\n"
               "    placed = []\n";
    }
    else if (raw)
    {
        body = fmt::format(
            "def _process_batch(n, buffers):\n"
            "    if len(buffers) != {0}:\n"
            "        raise ValueError(\"a batch with \" + str(len(buffers)) + \" buffers instead of {0}\")\n",
            framedBuffers);
    }
    else
    {
        body = "def _process_batch(batch):\n    n = batch.num_rows\n    columns = batch.columns\n";
    }

    /// The buffers of a column are looked up once, where the first call needs them.
    std::vector<bool> bound(fields.size(), false);
    const auto bind = [&](const size_t index, const bool readsValues)
    {
        if (!raw || bound.at(index))
        {
            return;
        }
        bound.at(index) = true;
        body += inPlace ? bindPoolBuffers(index, fields.at(index), readsValues)
                        : bindFramedBuffers(index, fields.at(index), framedPositions.at(index));
    };

    for (const auto& [callIndex, call] : std::views::enumerate(calls))
    {
        udfs += defineUdf(callIndex, call);

        /// The columns the call iterates over, and what it passes for each parameter: a value of a column, or a constant.
        std::vector<std::string> columns;
        std::vector<std::string> parameters;
        /// The column variables of NOT NULL arguments: a NULL in one of them gives a NULL result without a call.
        std::vector<std::string> notNullChecks;
        for (const auto& [argumentIndex, argument] : std::views::enumerate(call.arguments))
        {
            if (!argument.field.has_value())
            {
                const auto name = fmt::format("_constant_{}_{}", callIndex, argumentIndex);
                udfs += fmt::format("{} = {}\n", name, pythonConstant(argument));
                parameters.push_back(name);
                continue;
            }
            const auto& field = fields.at(*argument.field);
            if (!raw)
            {
                columns.push_back(readArrowColumn(*argument.field, field, argument.bytes));
            }
            else if (isVarSized(field) || field.type.type == DataType::Type::CHAR)
            {
                /// BYTES in the declaration of the UDF decides if the column is read as bytes or as str.
                const auto [bytesReader, strReader] = readers(field);
                bind(*argument.field, true);
                columns.push_back(readColumn(*argument.field, field, argument.bytes ? bytesReader : strReader));
            }
            else
            {
                bind(*argument.field, true);
                columns.push_back(readColumn(*argument.field, field, ""));
            }
            parameters.push_back(fmt::format("x{}", columns.size() - 1));
            if (argument.notNull && field.type.nullable)
            {
                notNullChecks.push_back(fmt::format("x{} is None", columns.size() - 1));
            }
        }

        const auto& result = fields.at(call.resultField);
        INVARIANT(result.type.nullable, "The result column of UDF '{}' must be nullable", call.name);
        const auto udfCall = fmt::format("_udf_{}({})", callIndex, fmt::join(parameters, ", "));
        const auto invocation
            = notNullChecks.empty() ? udfCall : fmt::format("(None if {} else {})", fmt::join(notNullChecks, " or "), udfCall);
        if (columns.empty())
        {
            body += fmt::format("    r = [{} for _ in range(n)]\n", invocation);
        }
        else if (columns.size() == 1)
        {
            body += fmt::format("    r = [{} for x0 in {}]\n", invocation, columns.front());
        }
        else
        {
            const auto variables = std::views::iota(size_t{0}, columns.size())
                | std::views::transform([](const size_t column) { return fmt::format("x{}", column); });
            body += fmt::format("    r = [{} for {} in zip({})]\n", invocation, fmt::join(variables, ", "), fmt::join(columns, ", "));
        }

        if (inPlace)
        {
            bind(call.resultField, false);
            body += fmt::format("    placed.append({})\n", placedResult(call.resultField, result));
        }
        else if (raw)
        {
            const auto position = framedPositions.at(call.resultField);
            body += fmt::format("    buffers[{}:{}] = {}\n", position, position + numberOfBuffers(result), framedResult(result));
            /// A later call that reads the column gets the results.
            bound.at(call.resultField) = false;
        }
        else
        {
            body += fmt::format("    columns[{}] = {}\n", call.resultField, arrowResultColumn(call.resultField, result));
        }
    }
    body += inPlace ? "    return placed\n"
        : raw       ? "    return buffers\n"
                    : "    return pa.RecordBatch.from_arrays(columns, schema=batch.schema)\n";

    program += fmt::format("_UDF_SOURCE = {}\n\n\n", pythonString(udfs));
    switch (protocol)
    {
        case UdfProcessProtocol::ARROW_IPC:
            program += body;
            program += RUNTIME;
            break;
        case UdfProcessProtocol::RAW_BUFFERS:
            program += body;
            program += RAW_RUNTIME;
            break;
        case UdfProcessProtocol::SHARED_MEMORY:
            program += body;
            program += SHARED_MEMORY_COMMON;
            program += SHARED_MEMORY_COPY;
            program += SHARED_MEMORY_START;
            break;
        case UdfProcessProtocol::SHARED_MEMORY_ZERO_COPY:
            program += fmt::format("_HEADER = struct.Struct(\"<\" + \"II\" * {})\n\n\n", 3 * fields.size());
            program += body;
            program += SHARED_MEMORY_COMMON;
            program += SHARED_MEMORY_IN_PLACE;
            program += SHARED_MEMORY_START;
            break;
    }
    return program;
}

}

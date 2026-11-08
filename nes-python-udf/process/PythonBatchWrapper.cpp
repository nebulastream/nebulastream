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
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <PythonArrowColumns.hpp>
#include <PythonBytesParameters.hpp>
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

std::string pythonConstant(const UdfProcessArgument& argument, const std::string_view asBytes)
{
    const auto text = pythonString(argument.constant.value());
    if (argument.constantType == "VARSIZED" || argument.constantType == "CHAR")
    {
        return fmt::format("{0}.encode() if {1} else {0}", text, asBytes);
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

/// The Python values of column `index` of the batch, as a list. A VARSIZED column becomes bytes or, failing on invalid UTF-8, str.
std::string readColumn(
    const size_t index, const ArrowBufferRef::Field& field, const std::string_view asBytes, const UdfProcessProtocol protocol)
{
    if (protocol != UdfProcessProtocol::ARROW_IPC)
    {
        return fmt::format("_raw_column(columns[{0}], _KINDS[{0}], n, {1})", index, asBytes);
    }
    if (field.type.type == DataType::Type::VARSIZED)
    {
        return fmt::format("_binary_column(batch.column({}), {})", index, asBytes);
    }
    if (field.type.type == DataType::Type::CHAR)
    {
        return fmt::format("_char_column(batch.column({}), {})", index, asBytes);
    }
    return fmt::format("batch.column({}).to_pylist()", index);
}

/// A Python expression turning the list `r` of results into column `index`: an Arrow array, or without ARROW_IPC its buffers.
std::string resultColumn(const size_t index, const ArrowBufferRef::Field& field, const UdfProcessProtocol protocol)
{
    if (protocol != UdfProcessProtocol::ARROW_IPC)
    {
        return fmt::format("_raw_result(_KINDS[{}], r)", index);
    }
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

/// How the raw-buffer runtime reads and writes a field: its kind and, for a fixed-width number, its `array` type code.
std::string rawKind(const ArrowBufferRef::Field& field)
{
    switch (field.type.type)
    {
        case DataType::Type::BOOLEAN:
            return "(\"bool\", None)";
        case DataType::Type::VARSIZED:
            return "(\"binary\", None)";
        case DataType::Type::CHAR:
            return "(\"char\", None)";
        case DataType::Type::INT8:
            return "(\"fixed\", \"b\")";
        case DataType::Type::UINT8:
            return "(\"fixed\", \"B\")";
        case DataType::Type::INT16:
            return "(\"fixed\", \"h\")";
        case DataType::Type::UINT16:
            return "(\"fixed\", \"H\")";
        case DataType::Type::INT32:
            return "(\"fixed\", \"i\")";
        case DataType::Type::UINT32:
            return "(\"fixed\", \"I\")";
        case DataType::Type::INT64:
            return "(\"fixed\", \"q\")";
        case DataType::Type::UINT64:
            return "(\"fixed\", \"Q\")";
        case DataType::Type::FLOAT32:
            return "(\"fixed\", \"f\")";
        case DataType::Type::FLOAT64:
            return "(\"fixed\", \"d\")";
        case DataType::Type::UNDEFINED:
            break;
    }
    throw CannotLoadUdf("UDF processes do not support fields of type {}", field.type);
}

/// Reads and writes the columns of RAW_BUFFERS batches in plain Python, see generatePythonBatchWrapper. _KINDS holds rawKind of every
/// field.
constexpr std::string_view RAW_COLUMNS = R"(

def _bits(data, n):
    return [(data[i >> 3] >> (i & 7)) & 1 for i in range(n)]


def _pack_bits(flags):
    packed = bytearray((len(flags) + 7) // 8)
    for i, flag in enumerate(flags):
        if flag:
            packed[i >> 3] |= 1 << (i & 7)
    return bytes(packed)


def _split(buffers):
    columns = []
    position = 0
    for kind, _ in _KINDS:
        count = 3 if kind == "binary" else 2
        columns.append(buffers[position:position + count])
        position += count
    if position != len(buffers):
        raise ValueError("a batch with " + str(len(buffers)) + " buffers instead of " + str(position))
    return columns


def _raw_column(buffers, kind, n, as_bytes):
    kind, code = kind
    if kind == "fixed":
        values = buffers[1].cast(code).tolist()
    elif kind == "bool":
        values = [bool(bit) for bit in _bits(buffers[1], n)]
    elif kind == "binary":
        offsets = buffers[1].cast("i")
        data = bytes(buffers[2])
        values = [data[offsets[i]:offsets[i + 1]] for i in range(n)]
    else:
        data = bytes(buffers[1])
        values = [data[i:i + 1] for i in range(n)]
    if len(buffers[0]):
        values = [v if valid else None for v, valid in zip(values, _bits(buffers[0], n))]
    if kind in ("binary", "char") and not as_bytes:
        # Decoding validates UTF-8, and a single byte only decodes if it is ASCII.
        values = [None if v is None else v.decode() for v in values]
    return values


def _raw_result(kind, r):
    kind, code = kind
    validity = b"" if None not in r else _pack_bits([v is not None for v in r])
    if kind == "fixed":
        zero = 0.0 if code in "fd" else 0
        return [validity, array.array(code, [zero if v is None else v for v in r]).tobytes()]
    if kind == "bool":
        return [validity, _pack_bits([v is not None and bool(v) for v in r])]
    if kind == "char":
        return [validity, b"".join(b"\0" if v is None else _char(v) for v in r)]
    values = [b"" if v is None else _binary(v) for v in r]
    offsets = [0]
    for value in values:
        offsets.append(offsets[-1] + len(value))
    if offsets[-1] >= 2 ** 31:
        raise ValueError("the VARSIZED results of a batch exceed 2 GiB")
    return [validity, array.array("i", offsets).tobytes(), b"".join(values)]


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


def _frame(columns):
    parts = []
    for buffers in columns:
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
    import array
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
_MAX_SPIN_SECONDS = 5e-3
_SLEEP_SECONDS = 1e-3
_gap = 0.0

_shared = mmap.mmap(4, 0)
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


def _wake_worker():
    if _control[_WORKER_SLEEPS]:
        _control[_WORKER_SLEEPS] = 0
        try:
            sock.send(b"\x01")
        except BlockingIOError:
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

def _answer(tail, rows, columns):
    # [record size][1][rows][body size], then the body
    pieces = [_bytes(buffer) for buffers in columns for buffer in buffers]
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


def _main():
    sock.setblocking(False)
    head = 0
    tail = 0
    while True:
        if not _await_request(head):
            return
        position = head % _ring
        (record,) = struct.unpack_from("<Q", _requests, position)
        if record == _WRAP:
            head += _ring - position
            _control[_REQUEST_HEAD] = head
            continue
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
            tail = _answer(tail, rows, _process_batch(rows, buffers))
        except Exception as error:
            tail = _failed(tail, error)
        head += record
        _control[_REQUEST_HEAD] = head
)";

/// SHARED_MEMORY_ZERO_COPY: the pool of the worker is the file descriptor 5, and the rings only say where the columns are in it.
/// A request is [record size][rows][children][value area offset][value area size][buffer offset], then per child buffer [offset]
/// [length], then the children that are not in the pool, whose offset has the bit _INLINE set and counts from there. The process reads
/// where the columns are from the header of the buffer, see ArrowBufferRef. A value area is a buffer of the pool for VARSIZED results,
/// used after the ones before it are full; a size of 0 means that the request brings none.
/// The results go into their columns in the batch, see _place. An answer is [record size][1][rows][count], then per call [nulls][where]
/// [offset][length]: whether the bitmap holds NULLs, and where the VARSIZED values are: 0 for none, 1 for `offset` in the pool, 2 for
/// `offset` behind the entries, if they fit no value area.
constexpr std::string_view SHARED_MEMORY_IN_PLACE = R"(

import array, collections

_pool = memoryview(mmap.mmap(5, 0))
_INLINE = 1 << 63
# The header of a buffer: per field [buffer][offset] as uint32 for its validity, data and values, see ArrowBufferRef::Location
_HEADER = struct.Struct("<" + "II" * 3 * len(_KINDS))
_MAIN, _NONE = 0, 0xFFFFFFFF
_EMPTY = memoryview(b"")
_SIZES = [array.array(code).itemsize if kind == "fixed" else 1 for kind, code in _KINDS]


def _regions(rows, bases):
    # The buffers of the columns in the order of _split, read off the header. bases holds where the buffer and its children are.
    header = _HEADER.unpack_from(*bases[_MAIN])
    bitmap = (rows + 7) // 8
    buffers = []
    for field, (kind, _) in enumerate(_KINDS):
        if kind == "bool":
            sizes = (bitmap, bitmap)
        elif kind == "binary":
            sizes = (bitmap, (rows + 1) * 4, None)
        else:
            sizes = (bitmap, rows * _SIZES[field])
        for region, size in enumerate(sizes):
            buffer, offset = header[6 * field + 2 * region], header[6 * field + 2 * region + 1]
            if size is None:
                # The values end where the last offset says
                offsets = buffers[-1]
                size = struct.unpack_from("<i", offsets, 4 * rows)[0] if len(offsets) else 0
            if buffer == _NONE or size == 0:
                buffers.append(_EMPTY)
                continue
            view, start = bases[buffer]
            buffers.append(view[start + offset:start + offset + size])
    return buffers
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


def _place(kind, r, buffers):
    # Writes the results into the column `buffers` of the batch: its bitmap only if there are NULLs, as the worker drops it otherwise
    kind, code = kind
    nulls = None in r
    if nulls:
        buffers[0][:] = _pack_bits([v is not None for v in r])
    if kind == "fixed":
        if nulls:
            zero = 0.0 if code in "fd" else 0
            r = [zero if v is None else v for v in r]
        buffers[1].cast(code)[:] = array.array(code, r)
        return nulls, 0, 0, 0, None
    if kind == "bool":
        buffers[1][:] = _pack_bits([v is not None and bool(v) for v in r])
        return nulls, 0, 0, 0, None
    if kind == "char":
        buffers[1][:] = b"".join(b"\0" if v is None else _char(v) for v in r)
        return nulls, 0, 0, 0, None
    values = [b"" if v is None else _binary(v) for v in r]
    offsets = array.array("i", [0]) * (len(values) + 1)
    total = 0
    for i, value in enumerate(values):
        total += len(value)
        if total >= 2 ** 31:
            raise ValueError("the VARSIZED results of a batch exceed 2 GiB")
        offsets[i + 1] = total
    buffers[1].cast("i")[:] = offsets
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


def _main():
    sock.setblocking(False)
    head = 0
    tail = 0
    while True:
        if not _await_request(head):
            return
        position = head % _ring
        (record,) = struct.unpack_from("<Q", _requests, position)
        if record == _WRAP:
            head += _ring - position
            _control[_REQUEST_HEAD] = head
            continue
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
            tail = _answer(tail, rows, _process_batch(rows, _regions(rows, bases)))
        except Exception as error:
            tail = _failed(tail, error)
        head += record
        _control[_REQUEST_HEAD] = head
)";

constexpr std::string_view SHARED_MEMORY_START = R"(

try:
    import array
    _define_udfs()
except BaseException as error:
    _failure(error)
    raise SystemExit(1)
sock.sendall(struct.pack("<Q", 1))
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
    program += PythonBytesParametersSource;
    /// _binary and _char, which check a VARSIZED or CHAR result, do not use pyarrow.
    program += PythonArrowColumnsSource;
    /// SHARED_MEMORY frames the bodies of its batches as RAW_BUFFERS does.
    const bool raw = protocol != UdfProcessProtocol::ARROW_IPC;
    if (raw)
    {
        program += fmt::format(
            "_KINDS = [{}]\n", fmt::join(fields | std::views::transform([](const auto& field) { return rawKind(field); }), ", "));
        program += RAW_COLUMNS;
    }

    std::string udfs;
    /// SHARED_MEMORY_ZERO_COPY writes the results into their columns and returns what _answer needs of each.
    const bool inPlace = protocol == UdfProcessProtocol::SHARED_MEMORY_ZERO_COPY;
    std::string body = raw ? "def _process_batch(n, buffers):\n    columns = _split(buffers)\n"
                           : "def _process_batch(batch):\n    n = batch.num_rows\n    columns = batch.columns\n";
    if (inPlace)
    {
        body += "    placed = []\n";
    }
    for (const auto& [callIndex, call] : std::views::enumerate(calls))
    {
        udfs += defineUdf(callIndex, call);
        udfs += fmt::format("_udf_{0}_bytes = _bytes_parameters(_udf_{0}, {1})\n", callIndex, call.arguments.size());

        std::vector<std::string> arguments;
        for (const auto& [argumentIndex, argument] : std::views::enumerate(call.arguments))
        {
            const auto asBytes = fmt::format("_udf_{}_bytes[{}]", callIndex, argumentIndex);
            if (argument.field.has_value())
            {
                arguments.push_back(readColumn(*argument.field, fields.at(*argument.field), asBytes, protocol));
            }
            else
            {
                const auto name = fmt::format("_constant_{}_{}", callIndex, argumentIndex);
                udfs += fmt::format("{} = {}\n", name, pythonConstant(argument, asBytes));
                arguments.push_back(fmt::format("[{}] * n", name));
            }
        }

        const auto& result = fields.at(call.resultField);
        INVARIANT(result.type.nullable, "The result column of UDF '{}' must be nullable", call.name);
        if (arguments.empty())
        {
            body += fmt::format("    r = [_udf_{}() for _ in range(n)]\n", callIndex);
        }
        else
        {
            body += fmt::format("    r = [_udf_{}(*row) for row in zip({})]\n", callIndex, fmt::join(arguments, ", "));
        }
        body += inPlace ? fmt::format("    placed.append(_place(_KINDS[{0}], r, columns[{0}]))\n", call.resultField)
                        : fmt::format("    columns[{}] = {}\n", call.resultField, resultColumn(call.resultField, result, protocol));
    }
    body += inPlace ? "    return placed\n"
        : raw       ? "    return columns\n"
                    : "    return pa.RecordBatch.from_arrays(columns, schema=batch.schema)\n";

    program += fmt::format("_UDF_SOURCE = {}\n\n\n", pythonString(udfs));
    program += body;
    switch (protocol)
    {
        case UdfProcessProtocol::ARROW_IPC:
            program += RUNTIME;
            break;
        case UdfProcessProtocol::RAW_BUFFERS:
            program += RAW_RUNTIME;
            break;
        case UdfProcessProtocol::SHARED_MEMORY:
            program += SHARED_MEMORY_COMMON;
            program += SHARED_MEMORY_COPY;
            program += SHARED_MEMORY_START;
            break;
        case UdfProcessProtocol::SHARED_MEMORY_ZERO_COPY:
            program += SHARED_MEMORY_COMMON;
            program += SHARED_MEMORY_IN_PLACE;
            program += SHARED_MEMORY_START;
            break;
    }
    return program;
}

}

import importlib, os, struct, sys, traceback

sys.path[:0] = [path for path in os.environ.get("NES_UDF_PATH", "").split(":") if path]


def _define_udfs():
    namespace = globals()
    exec(_UDF_SOURCE, namespace)




def _binary_column(column, as_bytes):
    # Casting to string validates UTF-8.
    return column.to_pylist() if as_bytes else column.cast(pa.string()).to_pylist()


def _char_column(column, as_bytes):
    # Decoding a single byte as UTF-8 fails for anything but ASCII.
    values = column.to_pylist()
    return values if as_bytes else [None if v is None else v.decode() for v in values]


def _char(value):
    value = _binary(value)
    if len(value) != 1:
        raise ValueError("a CHAR UDF must return one byte, not " + str(len(value)))
    return value


def _binary(value):
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError("a VARSIZED UDF must return bytes or str, not " + type(value).__name__)




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


_UDF_SOURCE = bytes.fromhex("646566205f7564665f302878293a0a202020200a202020202020202068203d20780a2020202020202020666f72205f20696e2072616e676528313030293a0a20202020202020202020202068203d202868202a2031313033353135323435202b20313233343529202520323134373438333634380a202020202020202072657475726e20680a202020200a").decode()


_HEADER = struct.Struct("<" + "II" * 6)


def _process_batch(n, bases):
    header = _HEADER.unpack_from(*bases[0])
    bitmap = (n + 7) // 8
    placed = []
    d0 = _view(bases, header[2], header[3], n * 8)
    r = [_udf_0(x0) for x0 in _fixed(d0, "q")]
    v1 = _view(bases, header[6], header[7], bitmap)
    d1 = _view(bases, header[8], header[9], n * 8)
    placed.append(_place_fixed(r, "q", 0, v1, d1))
    return placed


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

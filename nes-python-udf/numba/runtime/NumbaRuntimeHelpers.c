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

/* The parts of Numba's runtime that compiled UDFs call besides NRT and the typed dict and list: the Unicode type tables of
 * Numba's _helperlib.c (the two functions copied from there), the hash seeds that Numba otherwise reads from the Python
 * process, the Mersenne Twister of Numba's _random.c (copied; seeded from getentropy instead of Python's os.urandom), and
 * stand-ins for the CPython functions of Numba's exceptions with run-time values. Those run only on the error
 * path of such an exception: the stand-ins lose the exception's value, and the UDF fails with NES's generic message. */
#include <stdint.h>
#include <stddef.h>
#include <unistd.h>
#include "_unicodetype_db.h"

/* Hidden: the worker exports its symbols, and the embedded CPython of IN_PROCESS UDFs must not bind to these. The JIT finds
 * them through the symbol table of NumbaRuntime.cpp, which maps Numba's names (PyErr_SetString, ...) to the nes_numba_
 * stand-ins. */
#define NES_NUMBA_HIDDEN __attribute__((visibility("hidden")))

NES_NUMBA_HIDDEN void numba_gettyperecord(Py_UCS4 code, int* upper, int* lower, int* title, unsigned char* decimal, unsigned char* digit,
                         unsigned short* flags)
{
    int index;
    const numba_PyUnicode_TypeRecord* record;
    if (code >= 0x110000)
    {
        index = 0;
    }
    else
    {
        index = index1[(code >> SHIFT)];
        index = index2[(index << SHIFT) + (code & ((1 << SHIFT) - 1))];
    }
    record = &numba_PyUnicode_TypeRecords[index];
    *upper = record->upper;
    *lower = record->lower;
    *title = record->title;
    *decimal = record->decimal;
    *digit = record->digit;
    *flags = record->flags;
}

NES_NUMBA_HIDDEN Py_UCS4 numba_get_PyUnicode_ExtendedCase(int code)
{
    return numba_PyUnicode_ExtendedCase[code];
}

/* Fixed seeds: a string's hash only has to agree within the worker. */
NES_NUMBA_HIDDEN uint64_t _numba_hashsecret_djbx33a_suffix = 0x5bd1e995U;
NES_NUMBA_HIDDEN uint64_t _numba_hashsecret_siphash_k0 = 0x0706050403020100ULL;
NES_NUMBA_HIDDEN uint64_t _numba_hashsecret_siphash_k1 = 0x0f0e0d0c0b0a0908ULL;

/* Numba's random state (_random.c): the layout must stay as Numba's randomimpl accesses it. */
#define MT_N 624
#define MT_M 397
#define MT_MATRIX_A 0x9908b0dfU
#define MT_UPPER_MASK 0x80000000U
#define MT_LOWER_MASK 0x7fffffffU

typedef struct
{
    int index;
    unsigned int mt[MT_N];
    int has_gauss;
    double gauss;
    int is_initialized;
} rnd_state_t;

NES_NUMBA_HIDDEN void numba_rnd_shuffle(rnd_state_t* state)
{
    int i;
    unsigned int y;
    for (i = 0; i < MT_N - MT_M; i++)
    {
        y = (state->mt[i] & MT_UPPER_MASK) | (state->mt[i + 1] & MT_LOWER_MASK);
        state->mt[i] = state->mt[i + MT_M] ^ (y >> 1) ^ (-(int)(y & 1) & MT_MATRIX_A);
    }
    for (; i < MT_N - 1; i++)
    {
        y = (state->mt[i] & MT_UPPER_MASK) | (state->mt[i + 1] & MT_LOWER_MASK);
        state->mt[i] = state->mt[i + (MT_M - MT_N)] ^ (y >> 1) ^ (-(int)(y & 1) & MT_MATRIX_A);
    }
    y = (state->mt[MT_N - 1] & MT_UPPER_MASK) | (state->mt[0] & MT_LOWER_MASK);
    state->mt[MT_N - 1] = state->mt[MT_M - 1] ^ (y >> 1) ^ (-(int)(y & 1) & MT_MATRIX_A);
}

NES_NUMBA_HIDDEN void numba_rnd_init(rnd_state_t* state, unsigned int seed)
{
    unsigned int position;
    for (position = 0; position < MT_N; position++)
    {
        state->mt[position] = seed;
        seed = (1812433253U * (seed ^ (seed >> 30)) + position + 1) & 0xffffffffU;
    }
    state->index = MT_N;
    state->has_gauss = 0;
    state->gauss = 0.0;
    state->is_initialized = 1;
}

static void rnd_init_by_array(rnd_state_t* state, const unsigned int key[], size_t length)
{
    size_t i = 1;
    size_t j = 0;
    size_t k = MT_N > length ? MT_N : length;
    unsigned int* mt = state->mt;
    numba_rnd_init(state, 19650218U);
    for (; k; k--)
    {
        mt[i] = (mt[i] ^ ((mt[i - 1] ^ (mt[i - 1] >> 30)) * 1664525U)) + key[j] + (unsigned int)j;
        i++;
        j++;
        if (i >= MT_N)
        {
            mt[0] = mt[MT_N - 1];
            i = 1;
        }
        if (j >= length)
        {
            j = 0;
        }
    }
    for (k = MT_N - 1; k; k--)
    {
        mt[i] = (mt[i] ^ ((mt[i - 1] ^ (mt[i - 1] >> 30)) * 1566083941U)) - (unsigned int)i;
        i++;
        if (i >= MT_N)
        {
            mt[0] = mt[MT_N - 1];
            i = 1;
        }
    }
    mt[0] = 0x80000000U;
    state->index = MT_N;
    state->has_gauss = 0;
    state->gauss = 0.0;
    state->is_initialized = 1;
}

/* Seeded from the system's entropy, as Numba does through os.urandom. */
static rnd_state_t* rnd_ready(rnd_state_t* state)
{
    if (!state->is_initialized)
    {
        unsigned int key[MT_N];
        size_t offset;
        for (offset = 0; offset < sizeof(key); offset += 256)
        {
            size_t size = sizeof(key) - offset < 256 ? sizeof(key) - offset : 256;
            if (getentropy((char*)key + offset, size) != 0)
            {
                numba_rnd_init(state, (unsigned int)(uintptr_t)state);
                return state;
            }
        }
        rnd_init_by_array(state, key, MT_N);
    }
    return state;
}

static _Thread_local rnd_state_t numba_py_random_state;
static _Thread_local rnd_state_t numba_np_random_state;
static _Thread_local rnd_state_t numba_internal_random_state;

NES_NUMBA_HIDDEN rnd_state_t* numba_get_py_random_state(void)
{
    return rnd_ready(&numba_py_random_state);
}

NES_NUMBA_HIDDEN rnd_state_t* numba_get_np_random_state(void)
{
    return rnd_ready(&numba_np_random_state);
}

NES_NUMBA_HIDDEN rnd_state_t* numba_get_internal_random_state(void)
{
    return rnd_ready(&numba_internal_random_state);
}

NES_NUMBA_HIDDEN void* nes_numba_PyExc_RuntimeError = 0;

NES_NUMBA_HIDDEN void nes_numba_PyErr_SetString(void* type, const char* message)
{
    (void)type;
    (void)message;
}

NES_NUMBA_HIDDEN int64_t nes_numba_PyObject_Hash(void* object)
{
    (void)object;
    return 0;
}

NES_NUMBA_HIDDEN void* nes_numba_PyTuple_Pack(int64_t count, ...)
{
    (void)count;
    return 0;
}

NES_NUMBA_HIDDEN void* nes_numba_PyUnicode_FromKindAndData(int kind, const void* data, int64_t size)
{
    (void)kind;
    (void)data;
    (void)size;
    return 0;
}

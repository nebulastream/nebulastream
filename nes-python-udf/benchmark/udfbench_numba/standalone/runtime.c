/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* The parts of Numba's runtime that compiled UDFs call, without Python: the Unicode type tables of _helperlib.c, the hash
 * seeds that Numba reads from the Python process, and stand-ins for the CPython functions of Numba's exceptions with run-time
 * values (error paths only: they lose the exception's value). NRT, the typed list and the typed dict are Numba's own C
 * files, compiled as they are. */
#include <stdint.h>
#include <stddef.h>
#include "_unicodetype_db.h"

void numba_gettyperecord(Py_UCS4 code, int *upper, int *lower, int *title, unsigned char *decimal, unsigned char *digit,
                         unsigned short *flags)
{
    int index;
    const numba_PyUnicode_TypeRecord *rec;
    if (code >= 0x110000)
        index = 0;
    else
    {
        index = index1[(code >> SHIFT)];
        index = index2[(index << SHIFT) + (code & ((1 << SHIFT) - 1))];
    }
    rec = &numba_PyUnicode_TypeRecords[index];
    *upper = rec->upper;
    *lower = rec->lower;
    *title = rec->title;
    *decimal = rec->decimal;
    *digit = rec->digit;
    *flags = rec->flags;
}

Py_UCS4 numba_get_PyUnicode_ExtendedCase(int code) { return numba_PyUnicode_ExtendedCase[code]; }

uint64_t _numba_hashsecret_djbx33a_suffix = 0x5bd1e995u;
uint64_t _numba_hashsecret_siphash_k0 = 0x0706050403020100ull;
uint64_t _numba_hashsecret_siphash_k1 = 0x0f0e0d0c0b0a0908ull;

void *PyExc_RuntimeError = 0;
void PyErr_SetString(void *type, const char *message) { (void)type; (void)message; }
int64_t PyObject_Hash(void *object) { (void)object; return 0; }
void *PyTuple_Pack(int64_t n, ...) { (void)n; return 0; }
void *PyUnicode_FromKindAndData(int kind, const void *data, int64_t size) { (void)kind; (void)data; (void)size; return 0; }

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

/* The CPython types and limit that Numba's typed dict and list (third_party/numba/cext) use (they call no CPython function), and
 * the standard C headers that CPython's Python.h includes and Numba's files rely on. */
#pragma once

#include <assert.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

typedef ssize_t Py_ssize_t;
typedef Py_ssize_t Py_hash_t;
#define PY_SSIZE_T_MAX SSIZE_MAX

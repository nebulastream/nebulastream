/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef __APEX_MEMMOVE_H__
#define __APEX_MEMMOVE_H__

//	apex_memmove written by Trevor Herselman in 2014

//	FORCE `CDECL` calling convention on 32-bit builds on our function pointers, because we need it to match the original `std::memmove` definition; in-case the user specified a different default function calling convention! (I specified __fastcall as my default calling convention and got errors! So I needed to add this!)
#if !defined(__x86_64__) && !defined(_M_X64) && (defined(__i386) || defined(_M_IX86)) && (defined(_MSC_VER) || defined(__GNUC__))
#if defined(_MSC_VER)
#define APEXCALL __cdecl//	32-bit on Visual Studio
#else
#define APEXCALL __attribute__((__cdecl__))//	32-bit on GCC / LLVM (Clang)
#endif
#else
#define APEXCALL//	64-bit - __fastcall is default on 64-bit!
#endif

#ifdef __cplusplus

#include <cstddef>//	for `size_t`

namespace apex {
extern void*(APEXCALL* memcpy)(void* dst, const void* src, size_t size);
extern void*(APEXCALL* memmove)(void* dst, const void* src, size_t size);
}// namespace apex

#define apex_memcpy apex::memcpy
#define apex_memmove apex::memmove

#else

#include <stddef.h>//	ANSI/ISO C	-	for `size_t`

extern void*(APEXCALL* apex_memcpy)(void* dst, const void* src, size_t size);
extern void*(APEXCALL* apex_memmove)(void* dst, const void* src, size_t size);

#endif// __cplusplus

#endif// __APEX_MEMMOVE_H__
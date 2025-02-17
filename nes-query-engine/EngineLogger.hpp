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

#pragma once
#include <Util/Logger/Logger.hpp>
#include <magic_enum.hpp>

/// This allows the log level of the query engine to be controlled independently of other components. By default, the level is set to info.

///NOLINTBEGIN
#ifdef ENGINE_LOG_LEVEL_TRACE
    #define ENGINE_LOG_TRACE(...) NES_TRACE(__VA_ARGS__)
    #define ENGINE_IF_LOG_TRACE(...) __VA_ARGS__
    #define ENGINE_LOG_DEBUG(...) NES_DEBUG(__VA_ARGS__)
    #define ENGINE_IF_LOG_DEBUG(...) __VA_ARGS__
    #define ENGINE_LOG_INFO(...) NES_INFO(__VA_ARGS__)
    #define ENGINE_IF_LOG_INFO(...) __VA_ARGS__
    #define ENGINE_LOG_WARNING(...) NES_WARNING(__VA_ARGS__)
    #define ENGINE_IF_LOG_WARNING(...) __VA_ARGS__
    #define ENGINE_LOG_ERROR(...) NES_ERROR(__VA_ARGS__)
    #define ENGINE_IF_LOG_ERROR(...) __VA_ARGS__
#elifdef ENGINE_LOG_LEVEL_DEBUG
    #define ENGINE_LOG_TRACE(...)
    #define ENGINE_IF_LOG_TRACE(...)
    #define ENGINE_LOG_DEBUG(...) NES_DEBUG(__VA_ARGS__)
    #define ENGINE_IF_LOG_DEBUG(...) __VA_ARGS__
    #define ENGINE_LOG_INFO(...) NES_INFO(__VA_ARGS__)
    #define ENGINE_IF_LOG_INFO(...) __VA_ARGS__
    #define ENGINE_LOG_WARNING(...) NES_WARNING(__VA_ARGS__)
    #define ENGINE_IF_LOG_WARNING(...) __VA_ARGS__
    #define ENGINE_LOG_ERROR(...) NES_ERROR(__VA_ARGS__)
    #define ENGINE_IF_LOG_ERROR(...) __VA_ARGS__
#elifdef ENGINE_LOG_LEVEL_INFO
    #define ENGINE_LOG_TRACE(...)
    #define ENGINE_IF_LOG_TRACE(...)
    #define ENGINE_LOG_DEBUG(...)
    #define ENGINE_IF_LOG_DEBUG(...)
    #define ENGINE_LOG_INFO(...) NES_INFO(__VA_ARGS__)
    #define ENGINE_IF_LOG_INFO(...) __VA_ARGS__
    #define ENGINE_LOG_WARNING(...) NES_WARNING(__VA_ARGS__)
    #define ENGINE_IF_LOG_WARNING(...) __VA_ARGS__
    #define ENGINE_LOG_ERROR(...) NES_ERROR(__VA_ARGS__)
    #define ENGINE_IF_LOG_ERROR(...) __VA_ARGS__
#elifdef ENGINE_LOG_LEVEL_ERROR
    #define ENGINE_LOG_TRACE(...)
    #define ENGINE_IF_LOG_TRACE(...)
    #define ENGINE_LOG_DEBUG(...)
    #define ENGINE_IF_LOG_DEBUG(...)
    #define ENGINE_LOG_INFO(...)
    #define ENGINE_IF_LOG_INFO(...)
    #define ENGINE_LOG_WARNING(...)
    #define ENGINE_IF_LOG_WARNING(...)
    #define ENGINE_LOG_ERROR(...) NES_ERROR(__VA_ARGS__)
    #define ENGINE_IF_LOG_ERROR(...) __VA_ARGS__
#else
    #define ENGINE_LOG_TRACE(...)
    #define ENGINE_IF_LOG_TRACE(...)
    #define ENGINE_LOG_DEBUG(...)
    #define ENGINE_IF_LOG_DEBUG(...)
    #define ENGINE_IF_LOG_INFO(...) __VA_ARGS__
    #define ENGINE_LOG_INFO(...) NES_INFO(__VA_ARGS__)
    #define ENGINE_LOG_WARNING(...) NES_WARNING(__VA_ARGS__)
    #define ENGINE_IF_LOG_WARNING(...) __VA_ARGS__
    #define ENGINE_LOG_ERROR(...) NES_ERROR(__VA_ARGS__)
    #define ENGINE_IF_LOG_ERROR(...) __VA_ARGS__
#endif
///NOLINTEND

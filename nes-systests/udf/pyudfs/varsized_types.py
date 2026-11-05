# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""UDFs whose annotations choose how a VARSIZED argument arrives: bytes for raw bytes, anything else for UTF-8 text."""

from typing import Optional


def text_length(text: str):
    return len(text)


def byte_length(data: bytes):
    return len(data)


def optional_byte_length(data: Optional[bytes]):
    return None if data is None else len(data)


def type_name(value):
    return type(value).__name__


def echo_bytes(data: bytes):
    return data


def describe(data: bytes, text: str):
    return f"{len(data)}/{text}"


# A CHAR arrives like a VARSIZED value of one byte: a one-character str, or bytes when annotated so.


def char_code(char: str):
    return ord(char)


def optional_char_code(char: Optional[str]):
    return None if char is None else ord(char)


def char_byte(char: bytes):
    return char[0]


def next_char(char: str):
    return chr(ord(char) + 1)


def char_from_bytes(char: bytes):
    return char


def too_long(char: str):
    return char + char

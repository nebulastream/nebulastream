# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A UDF with a bytes annotation behind a decorator. It is apart from varsized_types.py, which the compiling bridges also load and
which therefore holds no decorators."""

import functools


def _logged(function):
    @functools.wraps(function)
    def wrapper(*arguments):
        return function(*arguments)

    return wrapper


# A wrapped function has no parameter names of its own; they come from the function it wraps
@_logged
def wrapped_byte_length(data: bytes):
    return len(data)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""re for nopython mode, for patterns that are string literals: re.compile, re.findall, and the pattern methods findall,
match, search and sub (with \\g<n> templates), and the match methods start, end and group.

Python's own parser (re._parser) parses the pattern when Numba types the call, so the syntax is Python's. The parse tree is
compiled into a program for a backtracking machine that tries alternatives in Python's order, so it finds Python's matches.
Supported: literals, classes with ranges and \\d \\w \\s (Unicode, as Python's re for str patterns), ., ^, $, \\A, \\Z, \\b,
\\B, groups, alternation, greedy and lazy repetition, and lookarounds whose body is one character or class. Flags other than
re.UNICODE, and anything else, fail when the UDF compiles, not at run time."""

import re
import re._constants as sre
import re._parser as sre_parse

import numpy as np
from numba import types
from numba.core.imputils import lower_constant
from numba.core.extending import intrinsic, models, typeof_impl, overload, overload_method, register_jitable, register_model
from numba.cpython.unicode import _get_code_point
from numba.cpython.unicode_support import (
    _PyUnicode_IsAlpha,
    _PyUnicode_IsDecimalDigit,
    _PyUnicode_IsDigit,
    _PyUnicode_IsNumeric,
    _PyUnicode_IsSpace,
)
from numba.experimental import structref

# Instructions: rows of (operation, a, b).
CHAR, NOT_CHAR, ANY, CLASS, SPLIT, JUMP, SAVE, MATCH, AT, LOOK = range(10)
# AT kinds.
AT_BEGINNING, AT_END, AT_BOUNDARY, AT_NON_BOUNDARY, AT_BEGINNING_STRING, AT_END_STRING = range(6)
# Class items: rows of (class, kind, low, high).
ITEM_RANGE, ITEM_DIGIT, ITEM_NOT_DIGIT, ITEM_WORD, ITEM_NOT_WORD, ITEM_SPACE, ITEM_NOT_SPACE = range(7)

_CATEGORIES = {
    sre.CATEGORY_DIGIT: ITEM_DIGIT,
    sre.CATEGORY_NOT_DIGIT: ITEM_NOT_DIGIT,
    sre.CATEGORY_WORD: ITEM_WORD,
    sre.CATEGORY_NOT_WORD: ITEM_NOT_WORD,
    sre.CATEGORY_SPACE: ITEM_SPACE,
    sre.CATEGORY_NOT_SPACE: ITEM_NOT_SPACE,
}
_ATS = {
    sre.AT_BEGINNING: AT_BEGINNING,
    sre.AT_END: AT_END,
    sre.AT_BOUNDARY: AT_BOUNDARY,
    sre.AT_NON_BOUNDARY: AT_NON_BOUNDARY,
    sre.AT_BEGINNING_STRING: AT_BEGINNING_STRING,
    sre.AT_END_STRING: AT_END_STRING,
}


class CompiledPattern:
    """A pattern compiled for the machine: program, class items, class negation, number of capture slots."""

    def __init__(self, pattern):
        parsed = sre_parse.parse(pattern)
        if parsed.state.flags & ~(sre.SRE_FLAG_UNICODE):
            raise NotImplementedError(f"re flags of {pattern!r} are not supported in nopython mode")
        # The parser counts group 0.
        self.groups = parsed.state.groups - 1
        self.program = []
        self.items = []
        self.negated = []
        self._emit_sequence(parsed, pattern)
        self.program.append((MATCH, 0, 0))
        self.program_array = np.array(self.program, dtype=np.int64)
        self.items_array = np.array(self.items if self.items else [(-1, 0, 0, 0)], dtype=np.int64)
        self.negated_array = np.array(self.negated if self.negated else [0], dtype=np.int64)

    def _class(self, members):
        index = len(self.negated)
        negate = 0
        for operation, argument in members:
            if operation == sre.NEGATE:
                negate = 1
            elif operation == sre.LITERAL:
                self.items.append((index, ITEM_RANGE, argument, argument))
            elif operation == sre.RANGE:
                self.items.append((index, ITEM_RANGE, argument[0], argument[1]))
            elif operation == sre.CATEGORY:
                self.items.append((index, _CATEGORIES[argument], 0, 0))
            else:
                raise NotImplementedError(f"class member {operation} is not supported in nopython mode")
        self.negated.append(negate)
        return index

    def _single_class(self, body, pattern):
        """The class of a lookaround body of one character or class."""
        if len(body) != 1:
            raise NotImplementedError(f"lookarounds of more than one character in {pattern!r} are not supported in nopython mode")
        operation, argument = body[0]
        if operation == sre.IN:
            return self._class(argument)
        if operation == sre.LITERAL:
            return self._class([(sre.LITERAL, argument)])
        raise NotImplementedError(f"lookaround body {operation} in {pattern!r} is not supported in nopython mode")

    def _emit_sequence(self, sequence, pattern):
        for operation, argument in sequence:
            self._emit(operation, argument, pattern)

    def _emit(self, operation, argument, pattern):
        program = self.program
        if operation == sre.LITERAL:
            program.append((CHAR, argument, 0))
        elif operation == sre.NOT_LITERAL:
            program.append((NOT_CHAR, argument, 0))
        elif operation == sre.ANY:
            program.append((ANY, 0, 0))
        elif operation == sre.IN:
            program.append((CLASS, self._class(argument), 0))
        elif operation == sre.AT:
            program.append((AT, _ATS[argument], 0))
        elif operation == sre.SUBPATTERN:
            group, add_flags, del_flags, body = argument
            if add_flags or del_flags:
                raise NotImplementedError(f"inline flags in {pattern!r} are not supported in nopython mode")
            if group is not None:
                program.append((SAVE, 2 * group, 0))
            self._emit_sequence(body, pattern)
            if group is not None:
                program.append((SAVE, 2 * group + 1, 0))
        elif operation == sre.BRANCH:
            _, alternatives = argument
            jumps = []
            for index, alternative in enumerate(alternatives):
                if index < len(alternatives) - 1:
                    split = len(program)
                    program.append((SPLIT, split + 1, -1))
                    self._emit_sequence(alternative, pattern)
                    jumps.append(len(program))
                    program.append((JUMP, -1, 0))
                    program[split] = (SPLIT, split + 1, len(program))
                else:
                    self._emit_sequence(alternative, pattern)
            for jump in jumps:
                program[jump] = (JUMP, len(program), 0)
        elif operation in (sre.MAX_REPEAT, sre.MIN_REPEAT):
            minimum, maximum, body = argument
            greedy = operation == sre.MAX_REPEAT
            for _ in range(minimum):
                self._emit_sequence(body, pattern)
            if maximum == sre.MAXREPEAT:
                if body.getwidth()[0] == 0:
                    raise NotImplementedError(f"unbounded repetition of an empty match in {pattern!r} is not supported")
                loop = len(program)
                program.append((SPLIT, -1, -1))
                self._emit_sequence(body, pattern)
                program.append((JUMP, loop, 0))
                after = len(program)
                program[loop] = (SPLIT, loop + 1, after) if greedy else (SPLIT, after, loop + 1)
            else:
                splits = []
                for _ in range(maximum - minimum):
                    split = len(program)
                    program.append((SPLIT, -1, -1))
                    splits.append(split)
                    self._emit_sequence(body, pattern)
                after = len(program)
                for split in splits:
                    program[split] = (SPLIT, split + 1, after) if greedy else (SPLIT, after, split + 1)
        elif operation in (sre.ASSERT, sre.ASSERT_NOT):
            direction, body = argument
            negate = 1 if operation == sre.ASSERT_NOT else 0
            behind = 2 if direction < 0 else 0
            program.append((LOOK, self._single_class(body, pattern), negate | behind))
        else:
            raise NotImplementedError(f"{operation} in {pattern!r} is not supported in nopython mode")


@register_jitable
def _is_word(code):
    return (
        code == 95
        or _PyUnicode_IsAlpha(code)
        or _PyUnicode_IsDecimalDigit(code)
        or _PyUnicode_IsDigit(code)
        or _PyUnicode_IsNumeric(code)
    )


@register_jitable
def _in_class(items, negated, index, code):
    found = False
    for row in range(items.shape[0]):
        if items[row, 0] != index:
            continue
        kind = items[row, 1]
        if kind == ITEM_RANGE:
            found = items[row, 2] <= code <= items[row, 3]
        elif kind == ITEM_DIGIT:
            found = _PyUnicode_IsDecimalDigit(code)
        elif kind == ITEM_NOT_DIGIT:
            found = not _PyUnicode_IsDecimalDigit(code)
        elif kind == ITEM_WORD:
            found = _is_word(code)
        elif kind == ITEM_NOT_WORD:
            found = not _is_word(code)
        elif kind == ITEM_SPACE:
            found = _PyUnicode_IsSpace(code)
        else:
            found = not _PyUnicode_IsSpace(code)
        if found:
            break
    return found != (negated[index] == 1)


@register_jitable
def _word_at(text, position):
    return 0 <= position < len(text) and _is_word(_get_code_point(text, position))


@register_jitable
def _run(program, items, negated, text, start, captures):
    """The end of the match that starts at `start`, or -1. Sets the capture slots of the match."""
    length = len(text)
    stack = np.empty((64, 3), dtype=np.int64)
    depth = 0
    pc = 0
    position = start
    while True:
        operation = program[pc, 0]
        argument = program[pc, 1]
        failed = False
        if operation == CHAR:
            if position < length and _get_code_point(text, position) == argument:
                position += 1
                pc += 1
            else:
                failed = True
        elif operation == NOT_CHAR:
            if position < length and _get_code_point(text, position) != argument:
                position += 1
                pc += 1
            else:
                failed = True
        elif operation == ANY:
            if position < length and _get_code_point(text, position) != 10:
                position += 1
                pc += 1
            else:
                failed = True
        elif operation == CLASS:
            if position < length and _in_class(items, negated, argument, _get_code_point(text, position)):
                position += 1
                pc += 1
            else:
                failed = True
        elif operation == SPLIT:
            if depth == stack.shape[0]:
                larger = np.empty((2 * stack.shape[0], 3), dtype=np.int64)
                larger[:depth] = stack[:depth]
                stack = larger
            stack[depth, 0] = 0
            stack[depth, 1] = program[pc, 2]
            stack[depth, 2] = position
            depth += 1
            pc = argument
        elif operation == JUMP:
            pc = argument
        elif operation == SAVE:
            if depth == stack.shape[0]:
                larger = np.empty((2 * stack.shape[0], 3), dtype=np.int64)
                larger[:depth] = stack[:depth]
                stack = larger
            stack[depth, 0] = 1
            stack[depth, 1] = argument
            stack[depth, 2] = captures[argument]
            depth += 1
            captures[argument] = position
            pc += 1
        elif operation == MATCH:
            return position
        elif operation == AT:
            if argument == AT_BEGINNING or argument == AT_BEGINNING_STRING:
                ok = position == 0
            elif argument == AT_END:
                ok = position == length or (position == length - 1 and _get_code_point(text, position) == 10)
            elif argument == AT_END_STRING:
                ok = position == length
            elif argument == AT_BOUNDARY:
                ok = _word_at(text, position - 1) != _word_at(text, position)
            else:
                ok = _word_at(text, position - 1) == _word_at(text, position)
            if ok:
                pc += 1
            else:
                failed = True
        else:
            flags = program[pc, 2]
            at = position - 1 if flags & 2 else position
            present = 0 <= at < length and _in_class(items, negated, argument, _get_code_point(text, at))
            if present != ((flags & 1) == 1):
                pc += 1
            else:
                failed = True
        if failed:
            resumed = False
            while depth > 0:
                depth -= 1
                if stack[depth, 0] == 1:
                    captures[stack[depth, 1]] = stack[depth, 2]
                else:
                    pc = stack[depth, 1]
                    position = stack[depth, 2]
                    resumed = True
                    break
            if not resumed:
                return -1


@register_jitable
def _search(program, items, negated, slots, text, start):
    """The captures of the first match at or after `start`: slots 0 and 1 hold its span, slot 0 is -1 without a match."""
    captures = np.full(slots, -1, dtype=np.int64)
    for begin in range(start, len(text) + 1):
        captures[:] = -1
        end = _run(program, items, negated, text, begin, captures)
        if end >= 0:
            captures[0] = begin
            captures[1] = end
            return captures
    captures[:] = -1
    return captures


@register_jitable
def _findall(program, items, negated, slots, groups, text):
    result = [""]
    result.pop()
    position = 0
    while position <= len(text):
        captures = _search(program, items, negated, slots, text, position)
        if captures[0] < 0:
            break
        group = 1 if groups == 1 else 0
        begin = captures[2 * group]
        result.append(text[begin:captures[2 * group + 1]] if begin >= 0 else "")
        position = captures[1] if captures[1] > captures[0] else captures[1] + 1
    return result


# Match objects.
@structref.register
class MatchType(types.StructRef):
    pass


class Match(structref.StructRefProxy):
    pass


structref.define_proxy(Match, MatchType, ["text", "captures"])


@register_jitable
def _make_match(text, captures):
    return Match(text, captures)


@overload_method(MatchType, "start")
def _match_start(match, group=0):
    return lambda match, group=0: match.captures[2 * group]


@overload_method(MatchType, "end")
def _match_end(match, group=0):
    return lambda match, group=0: match.captures[2 * group + 1]


@overload_method(MatchType, "group")
def _match_group(match, group=0):
    def implementation(match, group=0):
        begin = match.captures[2 * group]
        if begin < 0:
            return None
        return match.text[begin:match.captures[2 * group + 1]]
    return implementation


# Numba cannot unify a structref with None, so match and search always return a Match; one without a match has start -1
# and is false, so `if match:` behaves as in Python (`match is None` does not).
@overload(bool)
def _match_bool(match):
    if isinstance(match, MatchType):
        return lambda match: match.captures[0] >= 0
    return None


# re flags (re.UNICODE): Numba would lower an IntFlag member as its name. The pattern's flags are checked when it compiles;
# the argument itself carries nothing.
class RegexFlagType(types.Opaque):
    def __init__(self):
        super().__init__(name="RegexFlag")


regex_flag_type = RegexFlagType()
register_model(RegexFlagType)(models.OpaqueModel)


@typeof_impl.register(re.RegexFlag)
def _typeof_regex_flag(value, context):
    if value & ~re.UNICODE:
        raise NotImplementedError(f"re flag {value!r} is not supported in nopython mode")
    return regex_flag_type


@lower_constant(RegexFlagType)
def _lower_regex_flag(context, builder, flag_type, value):
    return context.get_constant_null(flag_type)


# Pattern objects: the pattern is part of the type, the value carries nothing.
class PatternType(types.Opaque):
    def __init__(self, pattern):
        self.pattern = pattern
        super().__init__(name=f"RegexPattern({pattern!r})")


register_model(PatternType)(models.OpaqueModel)


@intrinsic
def _make_pattern(typingctx, pattern):
    if not isinstance(pattern, types.StringLiteral):
        return None

    def codegen(context, builder, signature, arguments):
        return context.get_constant_null(signature.return_type)

    return PatternType(pattern.literal_value)(pattern), codegen


_compiled = {}


def _compile(pattern):
    if pattern not in _compiled:
        _compiled[pattern] = CompiledPattern(pattern)
    return _compiled[pattern]


@overload(re.compile)
def _re_compile(pattern, flags=0):
    if not isinstance(pattern, types.StringLiteral):
        return None
    _compile(pattern.literal_value)
    return lambda pattern, flags=0: _make_pattern(pattern)


def _arrays(pattern_type):
    compiled = _compile(pattern_type.pattern)
    return compiled.program_array, compiled.items_array, compiled.negated_array, 2 * (compiled.groups + 1), compiled.groups


@overload_method(PatternType, "findall")
def _pattern_findall(pattern, text):
    program, items, negated, slots, groups = _arrays(pattern)
    return lambda pattern, text: _findall(program, items, negated, slots, groups, text)


@overload_method(PatternType, "search")
def _pattern_search(pattern, text):
    program, items, negated, slots, _ = _arrays(pattern)

    def implementation(pattern, text):
        return _make_match(text, _search(program, items, negated, slots, text, 0))

    return implementation


@overload_method(PatternType, "match")
def _pattern_match(pattern, text, pos=0):
    program, items, negated, slots, _ = _arrays(pattern)

    def implementation(pattern, text, pos=0):
        captures = np.full(slots, -1, dtype=np.int64)
        end = _run(program, items, negated, text, pos, captures)
        if end < 0:
            captures[:] = -1
            return _make_match(text, captures)
        captures[0] = pos
        captures[1] = end
        return _make_match(text, captures)

    return implementation


@overload_method(PatternType, "sub")
def _pattern_sub(pattern, template, text):
    if not isinstance(template, types.StringLiteral):
        return None
    program, items, negated, slots, _ = _arrays(pattern)
    pieces = sre_parse.parse_template(template.literal_value, re.compile(pattern.pattern))
    literal_pieces = [piece for index, piece in enumerate(pieces) if index % 2 == 0]
    literals = "".join(literal_pieces)
    offsets = np.cumsum([0] + [len(piece) for piece in literal_pieces]).astype(np.int64)
    references = np.array([piece for index, piece in enumerate(pieces) if index % 2 == 1], dtype=np.int64)
    count = len(references)

    def implementation(pattern, template, text):
        parts = [""]
        position = 0
        last = 0
        while position <= len(text):
            captures = _search(program, items, negated, slots, text, position)
            if captures[0] < 0:
                break
            parts.append(text[last:captures[0]])
            for index in range(count):
                parts.append(literals[offsets[index]:offsets[index + 1]])
                group = references[index]
                if captures[2 * group] >= 0:
                    parts.append(text[captures[2 * group]:captures[2 * group + 1]])
            parts.append(literals[offsets[count]:offsets[count + 1]])
            last = captures[1]
            position = captures[1] if captures[1] > captures[0] else captures[1] + 1
        parts.append(text[last:])
        return "".join(parts)

    return implementation


@overload(re.findall)
def _re_findall(pattern, text, flags=0):
    if not isinstance(pattern, types.StringLiteral):
        return None
    compiled = _compile(pattern.literal_value)
    program, items, negated = compiled.program_array, compiled.items_array, compiled.negated_array
    slots, groups = 2 * (compiled.groups + 1), compiled.groups
    return lambda pattern, text, flags=0: _findall(program, items, negated, slots, groups, text)

#!/usr/bin/env python
# coding=utf-8

# MIT License
#
# Copyright (c) 2017 Niels Lohmann
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


"""
Standalone mutation testing tool for C++ files.

This module provides mutation testing functionality without requiring Flask or database dependencies.
It can be used as a standalone script or imported as a module.
"""

import os
import json
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple


class Replacement:
    """Represents a code replacement/mutation."""
    
    def __init__(self, start_col: int = None, end_col: int = None, old_val: str = None, new_val: str = None):
        self.start_col = start_col  # type: int
        self.end_col = end_col  # type: int
        self.old_val = old_val  # type: str
        self.new_val = new_val  # type: str

    def apply(self, line: str) -> str:
        """Apply this replacement to a line of code."""
        # if the whole line is to be replaced, return the new value
        if self.end_col - self.start_col == len(line)-1:
            return self.new_val

        return '{prefix}{replacement}{suffix}'.format(
            prefix=line[:self.start_col],
            replacement=self.new_val,
            suffix=line[self.end_col:]
        )

    def __repr__(self):
        return '[({begin_col}:{end_col}) "{old_val}" -> "{new_val}"]'.format(
            begin_col=self.start_col,
            end_col=self.end_col,
            old_val=self.old_val,
            new_val=self.new_val
        )


class StringLiteralFinder:
    """Helper class to detect if a position is inside a string literal."""
    
    def __init__(self, line: str):
        self.line = line
        self.string_ranges = self._find_string_ranges()
    
    def _find_string_ranges(self) -> List[Tuple[int, int]]:
        """Find all string literal ranges in the line."""
        ranges = []
        in_string = False
        string_start = 0
        quote_char = None
        i = 0
        
        while i < len(self.line):
            char = self.line[i]
            
            if not in_string:
                if char in ['"', "'"]:
                    in_string = True
                    string_start = i
                    quote_char = char
            else:
                if char == '\\':
                    # Skip escaped character
                    i += 1
                elif char == quote_char:
                    ranges.append((string_start, i + 1))
                    in_string = False
                    quote_char = None
            
            i += 1
        
        return ranges
    
    def is_in_string_literal(self, pos: int) -> bool:
        """Check if position is inside a string literal."""
        for start, end in self.string_ranges:
            if start <= pos < end:
                return True
        return False


class SimplePattern:
    """Pattern-based mutator for simple text replacements."""
    
    def __init__(self, replacement_patterns):
        self.replacement_patterns = replacement_patterns  # type: Dict[str, List[str]]

    def mutate(self, line):
        result = []  # type: List[Replacement]

        string_finder = StringLiteralFinder(line)

        for replacement_pattern in self.replacement_patterns.keys():
            for occurrence in [match for match in re.finditer(re.compile(replacement_pattern), line)]:
                if string_finder.is_in_string_literal(occurrence.start()):
                    continue

                for replacement_str in self.replacement_patterns[replacement_pattern]:
                    result.append(Replacement(start_col=occurrence.start(),
                                              end_col=occurrence.end(),
                                              old_val=line[occurrence.start():occurrence.end()],
                                              new_val=replacement_str))

        return result


# Mutator classes
class LineDeletionMutator:
    mutator_id = 'lineDeletion'
    description = 'Deletes a whole line.'
    tags = ['naive']

    def find_mutations(self, line):
        return [Replacement(start_col=0,
                            end_col=len(line) - 1,
                            old_val=line,
                            new_val=None)]


class LogicalOperatorMutator:
    mutator_id = 'logicalOperator'
    description = 'Replaces logical operators.'
    tags = ['operator', 'logical']

    def __init__(self):
        self.pattern = SimplePattern({
            ' && ': [' || '],
            r' \|\| ': [' && ']
        })

    def find_mutations(self, line):
        return self.pattern.mutate(line)


class ComparisonOperatorMutator:
    mutator_id = 'comparisonOperator'
    description = 'Replaces comparison operators.'
    tags = ['operator', 'comparison']

    def __init__(self):
        self.pattern = SimplePattern({
            ' == ': [' != ', ' < ', ' > ', ' <= ', ' >= '],
            ' != ': [' == ', ' < ', ' > ', ' <= ', ' >= '],
            ' < ': [' == ', ' != ', ' > ', ' <= ', ' >= '],
            ' > ': [' == ', ' != ', ' < ', ' <= ', ' >= '],
            ' <= ': [' == ', ' != ', ' < ', ' > ', ' >= '],
            ' >= ': [' == ', ' != ', ' < ', ' > ', ' <= ']
        })

    def find_mutations(self, line):
        return self.pattern.mutate(line)


class IncDecOperatorMutator:
    mutator_id = 'incDecOperator'
    description = 'Replaces increment/decrement operators.'
    tags = ['operator']

    def __init__(self):
        self.pattern = SimplePattern({
            r'\+\+': ['--'],
            '--': ['++']
        })

    def find_mutations(self, line):
        return self.pattern.mutate(line)


class AssignmentOperatorMutator:
    mutator_id = 'assignmentOperator'
    description = 'Replaces assignment operators.'
    tags = ['operator']

    def __init__(self):
        self.pattern = SimplePattern({
            ' = ': [' += ', ' -= ', ' *= ', ' /= ', ' %= '],
            r' \+= ': [' = ', ' -= ', ' *= ', ' /= ', ' %= '],
            ' -= ': [' = ', ' += ', ' *= ', ' /= ', ' %= '],
            r' \*= ': [' = ', ' += ', ' -= ', ' /= ', ' %= '],
            ' /= ': [' = ', ' += ', ' -= ', ' *= ', ' %= '],
            ' %= ': [' = ', ' += ', ' -= ', ' *= ', ' /= ']
        })

    def find_mutations(self, line):
        return self.pattern.mutate(line)


class ArithmeticOperatorMutator:
    mutator_id = 'arithmeticOperator'
    description = 'Replaces arithmetic operators.'
    tags = ['operator', 'arithmetic']

    def __init__(self):
        self.pattern = SimplePattern({
            r' \+ ': [' - ', ' * ', ' / ', ' % '],
            ' - ': [' + ', ' * ', ' / ', ' % '],
            r' \* ': [' + ', ' - ', ' / ', ' % '],
            ' / ': [' + ', ' - ', ' * ', ' % '],
            ' % ': [' + ', ' - ', ' * ', ' / ']
        })

    def find_mutations(self, line):
        return self.pattern.mutate(line)


class BooleanLiteralMutator:
    mutator_id = 'booleanLiteral'
    description = 'Replaces boolean literals.'
    tags = ['literal', 'boolean']

    def __init__(self):
        self.pattern = SimplePattern({
            'true': ['false'],
            'false': ['true']
        })

    def find_mutations(self, line):
        return self.pattern.mutate(line)


class DecimalNumberLiteralMutator:
    mutator_id = 'decimalNumberLiteral'
    description = 'Replaces decimal number literals.'
    tags = ['literal', 'number']

    def __init__(self):
        self.regex = r'([0-9]+\.?[0-9]*)'

    def find_mutations(self, line):
        result = []  # type: List[Replacement]

        string_finder = StringLiteralFinder(line)

        for occurrence in [match for match in re.finditer(re.compile(self.regex), line)]:
            if string_finder.is_in_string_literal(occurrence.start()):
                continue

            number_str = occurrence.group(1)

            # use JSON as means to cope with both int and double
            try:
                number_val = json.loads(number_str)
            except ValueError:
                continue

            replacements = list({number_val + 1, number_val - 1, -number_val, 0} - {number_val})
            for replacement in replacements:
                result.append(Replacement(start_col=occurrence.start(1),
                                          end_col=occurrence.end(1),
                                          old_val=line[occurrence.start(1):occurrence.end(1)],
                                          new_val=str(replacement)))

        return result


def get_mutators():
    """Get all available mutators."""
    mutators = [
        LineDeletionMutator(),
        LogicalOperatorMutator(),
        ComparisonOperatorMutator(),
        IncDecOperatorMutator(),
        AssignmentOperatorMutator(),
        ArithmeticOperatorMutator(),
        BooleanLiteralMutator(),
        DecimalNumberLiteralMutator(),
    ]

    return {mutator.mutator_id: mutator for mutator in mutators}


class StandaloneSourceFile:
    """
    A standalone version of SourceFile that works directly with filesystem files
    instead of database File objects, and outputs patches to files instead of 
    storing in database.
    """
    
    def __init__(self, file_path: str, first_line: int = 1, last_line: int = -1):
        self.file_path = Path(file_path).resolve()
        self.filename = str(self.file_path)
        
        # Read file content
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                self.full_content = [x.rstrip() for x in f.read().split('\n')]
        except Exception as e:
            raise ValueError(f"Could not read file {self.file_path}: {e}")
        
        # the line numbers stored here are human-readable; to use them as indices to
        # self.full_content, we need to subtract -1
        self.first_line = first_line
        self.last_line = last_line

        if self.last_line == -1:
            self.last_line = len(self.full_content)

        # read the relevant content
        self.content = '\n'.join(self.full_content[self.first_line - 1:self.last_line])  # type: str

    def generate_patches(self, output_dir: str) -> List[Dict[str, Any]]:
        """
        Generate patches and write them to files in the output directory.
        Returns a list of patch metadata.
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        mutators = get_mutators()
        patches = []
        patch_counter = 0

        for line_number, line_raw in self._get_lines():
            for mutator_name, mutator in mutators.items():
                for mutation in mutator.find_mutations(line_raw):
                    patch_counter += 1
                    patch_text = self._create_patch(line_number, mutation)
                    
                    # Create patch metadata
                    patch_info = {
                        'id': patch_counter,
                        'kind': mutator_name,
                        'line': line_number,
                        'column_start': mutation.start_col,
                        'column_end': mutation.end_col,
                        'code_original': mutation.old_val,
                        'code_replacement': mutation.new_val,
                        'file_path': str(self.file_path),
                        'patch_file': f"patch_{patch_counter:06d}.patch"
                    }
                    
                    # Write patch to file
                    patch_file_path = output_path / patch_info['patch_file']
                    with open(patch_file_path, 'w', encoding='utf-8') as f:
                        f.write(patch_text)
                    
                    patches.append(patch_info)

        # Write patch metadata to JSON file
        metadata_file = output_path / 'patches_metadata.json'
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump({
                'source_file': str(self.file_path),
                'first_line': self.first_line,
                'last_line': self.last_line,
                'total_patches': len(patches),
                'patches': patches
            }, f, indent=2)
        
        return patches

    def _get_lines(self):
        """
        Generator that yields line numbers and content for lines that should be mutated.
        Skips comments, empty lines, preprocessor directives, etc.
        """
        in_comment = False

        for line_number in range(self.first_line, self.last_line):
            line_raw = self.full_content[line_number - 1]
            line_stripped = line_raw.strip()

            # skip line comments and preprocessor directives
            if line_stripped.startswith('//') or line_stripped.startswith('#'):
                continue

            # skip empty lines or "bracket onlys"
            if line_stripped in ['', '{', '}', '};', '});', ')']:
                continue

            # recognize the beginning of a line comment
            if line_stripped.startswith('/*'):
                in_comment = True
                if line_stripped.endswith('*/'):
                    in_comment = False
                continue

            # skip assertions
            if line_stripped.startswith('assert(') or line_stripped.startswith('static_assert('):
                continue

            # skip "private" or "protected" declaration
            if line_stripped.startswith('private:') or line_stripped.startswith('protected:'):
                continue

            # recognize the end of a line comment
            if line_stripped.endswith('*/'):
                in_comment = False
                continue

            # return line to mutate
            if not in_comment:
                yield line_number, line_raw

    def _create_patch(self, line_number: int, replacement: Replacement) -> str:
        """
        Create a unified diff patch for the given line and replacement.
        """
        # get file date in the format we need to write it to the patch
        try:
            original_file_date = datetime.fromtimestamp(os.path.getmtime(self.filename)).strftime('%Y-%m-%d %H:%M:%S.%f')
        except OSError:
            original_file_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # passed line_number is human-readable
        index_line_number = line_number - 1
        old_line = self.full_content[index_line_number]
        new_line = replacement.apply(old_line)

        # number of lines before and after the changed line
        context_lines = 3
        context_before = self.full_content[max(0, index_line_number - context_lines):index_line_number]
        context_after = self.full_content[
                        index_line_number + 1:min(len(self.full_content), index_line_number + context_lines + 1)]

        patch_lines = []

        # first line: we want to change the source file
        patch_lines.append('--- {filename} {date}'.format(filename=self.filename, date=original_file_date) + os.linesep)
        # second line: the new file has the same name, but is changed now
        patch_lines.append('+++ {filename} {date}'.format(filename=self.filename, date=datetime.now()) + os.linesep)
        # third line: summarize the changes regarding to displayed lines
        patch_lines.append('@@ -{lineno},{context_length} +{lineno},{context_length_shortened} @@'.format(
            lineno=line_number - len(context_before),
            context_length=len(context_before) + len(context_after) + 1,
            context_length_shortened=len(context_before) + len(context_after) + (1 if new_line else 0)
        ) + os.linesep)

        # add context before
        for context_line in context_before:
            patch_lines.append(' ' + context_line + os.linesep)

        # add the line to be changed
        patch_lines.append('-' + old_line + os.linesep)

        # add the replacement line (if not deleted)
        if new_line is not None:
            patch_lines.append('+' + new_line + os.linesep)

        # add context after
        for context_line in context_after:
            patch_lines.append(' ' + context_line + os.linesep)

        return ''.join(patch_lines)


if __name__ == "__main__":
    # This allows the module to be used as a standalone script
    import sys
    from argparse import ArgumentParser
    
    def main():
        from argparse import RawDescriptionHelpFormatter
        argument_parser = ArgumentParser(
            description="Generate mutation testing patches for C++ files.",
            formatter_class=RawDescriptionHelpFormatter
        )
        
        argument_parser.add_argument(
            "-o", "--output-dir", 
            type=str, 
            required=True,
            help="Directory where patch files will be written"
        )
        
        argument_parser.add_argument(
            "--first-line", 
            type=int, 
            default=1,
            help="First line to process (1-based, default: 1)"
        )
        
        argument_parser.add_argument(
            "--last-line", 
            type=int, 
            default=-1,
            help="Last line to process (1-based, -1 for end of file, default: -1)"
        )
        
        argument_parser.add_argument(
            "--verbose", "-v",
            action="store_true",
            help="Enable verbose output"
        )
        
        argument_parser.add_argument(
            'files', 
            type=str, 
            metavar="FILE", 
            nargs='+',
            help="C++ source files to process"
        )
        
        arguments = argument_parser.parse_args()
        
        # Validate inputs
        if arguments.first_line < 1:
            print("Error: --first-line must be >= 1", file=sys.stderr)
            sys.exit(1)
        
        if arguments.last_line != -1 and arguments.last_line < arguments.first_line:
            print("Error: --last-line must be >= --first-line or -1", file=sys.stderr)
            sys.exit(1)
        
        # Validate files and create output directory
        validated_files = []
        for file_path in arguments.files:
            path = Path(file_path)
            if not path.exists():
                print(f"Error: File '{file_path}' does not exist.", file=sys.stderr)
                sys.exit(1)
            validated_files.append(path.resolve())
        
        output_path = Path(arguments.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        if arguments.verbose:
            print(f"Processing {len(validated_files)} file(s)")
            print(f"Output directory: {output_path}")
            print()
        
        total_patches = 0
        
        # Process each file
        for file_path in validated_files:
            if arguments.verbose:
                print(f"Processing '{file_path.name}'...")
            
            try:
                # Create a subdirectory for each file to avoid conflicts
                file_output_dir = output_path / f"{file_path.stem}_{file_path.suffix[1:]}_patches"
                
                source_file = StandaloneSourceFile(
                    str(file_path), 
                    arguments.first_line, 
                    arguments.last_line
                )
                
                patches = source_file.generate_patches(str(file_output_dir))
                file_patch_count = len(patches)
                total_patches += file_patch_count
                
                if arguments.verbose:
                    print(f"  Generated {file_patch_count} patches in '{file_output_dir.name}/'")
                else:
                    print(f"Generated {file_patch_count} patches for '{file_path.name}'")
                    
            except Exception as e:
                print(f"Error processing '{file_path}': {e}", file=sys.stderr)
                sys.exit(1)
        
        print()
        print(f"Successfully generated {total_patches} patches total.")
        print(f"Patches written to: {output_path}")
    
    main()

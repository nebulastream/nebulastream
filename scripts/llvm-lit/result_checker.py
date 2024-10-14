import sys
import re

def read_input_file(filename):
    """Reads the input file and returns sections separated by '----'."""
    with open(filename, 'r') as file:
        content = file.read()
    # Split the content based on '----' and ignore everything before the first '----'
    sections = content.split('----')[1:]
    normalized_sections = []
    for section in sections:
        # Normalize each section by converting spaces to commas, stripping whitespace,
        # and ignoring everything after the first empty line
        normalized_section = normalize_section(section.strip(), delimiter=' ')
        if normalized_section:
            normalized_sections.append(normalized_section)
    return normalized_sections

def read_expected_file(filename):
    """Reads the expected output file and returns sections separated by pattern."""
    sections = []
    with open(filename, 'r') as file:
        section = []
        for line in file:
            line = line.strip()
            if re.match(r'.*\$.*:.*', line):
                if section:
                    sections.append(normalize_section("\n".join(section).strip(), delimiter=','))
                section = [line]  # Start a new section with the current line
            else:
                section.append(line)
        if section:
            sections.append(normalize_section("\n".join(section).strip(), delimiter=','))
    return sections

def normalize_section(section, delimiter):
    """Normalizes a section by converting its content to a common format."""
    lines = section.splitlines()
    normalized_lines = []
    for line in lines:
        if not line.strip():
            # Stop processing at the first empty line, which marks the end of the section
            break
        # Skip lines that look like they are format descriptors (e.g., "window$id:INTEGER(64 bits)")
        if re.match(r'.*\$.*:.*', line):
            continue
        normalized_lines.append(",".join(line.split(delimiter)).strip())
    return "\n".join(normalized_lines)

def compare_sections(input_sections, expected_sections):
    """Compares the outputs from the query and expected output."""
    if len(input_sections) != len(expected_sections):
        print(f"Number of query outputs do not match! Query outputs: {len(input_sections)}, Expected outputs: {len(expected_sections)}")
        return False

    all_match = True
    for i, (input_section, expected_section) in enumerate(zip(input_sections, expected_sections)):
        input_lines = input_section.splitlines()
        expected_lines = expected_section.splitlines()

        input_lines.sort()
        expected_lines.sort()

        if input_lines != expected_lines:
            print(f"Query {i + 1} does not match!")
            print("\nExpected:")
            print(expected_section)
            print("\nGot:")
            print(input_section)
            print("\nDifferences:")
            for j, (exp_line, inp_line) in enumerate(zip(expected_lines, input_lines)):
                if exp_line != inp_line:
                    print(f"Line {j + 1} differs:")
                    print(f"Expected: {exp_line}")
                    print(f"Got: {inp_line}")
                    print("-" * 50)
            # Handle case where one has more lines than the other
            if len(expected_lines) > len(input_lines):
                print("\nExtra lines in Expected:")
                for line in expected_lines[len(input_lines):]:
                    print(line)
            elif len(input_lines) > len(expected_lines):
                print("\nExtra lines in Got:")
                for line in input_lines[len(expected_lines):]:
                    print(line)
            print("\n" + "="*50 + "\n")

    return all_match

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 result_checker.py <input_file> <expected_output_file>")
        sys.exit(1)

    input_filename = sys.argv[1]
    expected_filename = sys.argv[2]

    # Read sections from input file and expected output file
    input_sections = read_input_file(input_filename)
    expected_sections = read_expected_file(expected_filename)

    # Compare sections
    if compare_sections(input_sections, expected_sections):
        sys.exit(0)  # Success
    else:
        sys.exit(1)  # Failure

if __name__ == "__main__":
    main()

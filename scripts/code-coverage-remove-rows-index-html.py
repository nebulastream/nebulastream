import sys
from bs4 import BeautifulSoup


def split_wrapper(index, line):
    return line.split("nebulastream-public/nebulastream-public", 1)[index]


def main():
    # We expect that this script is called with the following arguments
    # Arg[1]: Absolute path to the index.html file
    # Arg[2]: Absolute path where the script should write the output index.html file
    # Arg[3]: Absolute path to the file that contains the changed files.
    #          We expect that each line contains the absolute path to the changed file

    if len(sys.argv) != 4:
        print("Usage: python code-coverage-remove-rows-index-html.py <index_file> <output_file> <tested_files_path>")
        exit(1)


    index_file = sys.argv[1]
    output_file = sys.argv[2]
    tested_files = sys.argv[3]

    # Read base path to nebulastream-public/nebulastream-public
    with open(tested_files, "r", encoding="utf-8") as file:
        line = file.readline.strip()

        expected_path = "nebulastream-public/nebulastream-public"
        assert expected_path in line, f"Expected '{expected_path}' in the line, but it was not found. Line content: '{line}'"

        base_path = split_wrapper(0, line)

        # removes __w as the rest of the script excpects it to be gone
        base_path = base_path.rstrip("__w")  # Removes "__w" from the end

    # Read the list of tested files
    with open(tested_files, "r", encoding="utf-8") as file:
        tested_files_list = [split_wrapper(-1, line.strip()).rstrip(".html") for line in file.readlines()]

    print(f"Tested files: {tested_files_list}")

    # Read the index.html file and parse it with BeautifulSoup, so that we can later manipulate it
    with open(index_file, "r", encoding="utf-8") as file:
        soup = BeautifulSoup(file, "html.parser")

    # Locate the table containing the data
    table = soup.find('table')
    assert table, "No table found in the HTML file."

    # Extract rows
    rows = table.find_all('tr')[1:]

    # We have to change the path of the style.css file in the index.html
    # This is necessary because the index.html file is in a different relative path than the style.css file
    table_style = soup.find('link')
    table_style['href'] = "build/ccov/all-merged/style.css"

    # Iterate over table rows and filter based on filenames
    for row in rows:
        filename_cell = row.find('td')  # Assuming the filename is in the first column
        filename = filename_cell.text.strip()
        # We have to remove everything before the "nebulastream-public/nebulastream-public" part
        filename = split_wrapper(-1, filename)
        print(f"Filename: {filename}")
        if filename not in tested_files_list:
            row.decompose()  # Remove the row
        else:
            filename_cell_tag = filename_cell.find('a')
            base_path = base_path.rstrip("__w/")
            filename_cell_tag['href'] = base_path + "/" + filename_cell.text.strip() + ".html"

            # debug commands remove after script works
            print(base_path)
            print(filename_cell.text.strip())
            print(base_path + filename_cell.text.strip() + ".html")

    # Save the modified HTML to a new file
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(str(soup))


if __name__ == "__main__":
    main()

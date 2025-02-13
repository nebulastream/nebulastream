import sys
from bs4 import BeautifulSoup


def main():

    if len(sys.argv) != 4:
        print("Usage: python code-coverage-remove-rows-index-html.py <index_file> <output_file> <changed_files_path>")
        sys.exit(1)

    # path to the index.html file
    index_file = sys.argv[1]
    # path where the script should write the output index.html file
    output_file = sys.argv[2]
    # path to the file that contains the changed files, which will be used to filter index.html
    changed_files_path = sys.argv[3]

    # Read the list of changed files
    with open(changed_files_path, "r", encoding="utf-8") as file:
        changed_files = [line.strip() for line in file.readlines()]

    print(f"Changed files: {changed_files}")

    # Read the index.html file and parse it with BeautifulSoup, so that we can later manipulate it
    with open(index_file, "r", encoding="utf-8") as file:
        soup = BeautifulSoup(file, "html.parser")

    # Locate the table containing the data
    table = soup.find('table')
    assert table, "Expected table in the HTML file."

    # Iterate over table rows and remove rows that report cov for unchanged files.
    rows = table.find_all('tr')
    for row in rows:
        a = row.find('a')

        # skip table header, footer
        if not a:
            continue

        filename = a.get("href")
        if filename not in changed_files:
            row.decompose()  # Remove the row

        # remove path prefix /.../nebulastream-public/ (if in string)
        a.string = a.text.split("nebulastream-public/")[-1]

    # Save the modified HTML to a new file
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(str(soup))


if __name__ == "__main__":
    main()

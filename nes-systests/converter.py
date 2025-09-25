import sys

def main():
    if len(sys.argv) != 2:
        print("Missing file path")
        sys.exit()
    path = sys.argv[1]
    out = []

    with open(path) as file:
        lines = file.readlines()

        for line in lines:
            if line.lower().startswith("source"):
                create_source_queries(out, line)
                pass
            elif line.lower().startswith("sink"):
                create_sink_queries(out, line)
                pass
            else:
                out.append(line)

    with open(path, "w") as file:
        file.writelines(out)


def create_source_queries(out, line):
    query = "CREATE LOGICAL SOURCE "
    tokens = line.split(" ")
    source_name = tokens[1]
    query += source_name + "("
    for i in range(2, len(tokens)-1, 2):
        query += f"{tokens[i+1]} {tokens[i]}, "
    query = query[:-2] + ");\n"
    out.append(query)

    out.append(f"CREATE PHYSICAL SOURCE FOR {source_name} TYPE File;\n")
    out.append("INLINE\n")


def create_sink_queries(out, line):
    query = "CREATE SINK "
    tokens = line.split(" ")
    sink_name = tokens[1]
    query += sink_name + "("
    for i in range(2, len(tokens)-1, 2):
        query += f"{tokens[i+1].replace("$", ".")} {tokens[i]}, "
    query = query[:-2] + ")  TYPE File;\n"
    out.append(query)


if __name__ == "__main__":
    main()
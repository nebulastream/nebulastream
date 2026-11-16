import array, ast, sys, time
k = int(sys.argv[1])
src = open("/usr/lib/pypy3.11/inspect.py").read()
tree = ast.parse(src)
mod = ast.Module(body=tree.body[:k], type_ignores=[])
ns = {"__name__": "inspect_probe"}
exec(compile(mod, "inspect_probe", "exec"), ns)
fn = {}
exec("def udf(x):\n    h = x\n    for _ in range(100):\n        h = (h * 1103515245 + 12345) % 2147483648\n    return h\n", fn)
udf = fn["udf"]
batch = array.array("q", range(2731))
for phase in range(2):
    t = time.time(); rows = 0
    while time.time() - t < 2:
        r = [udf(x) for x in batch]; rows += len(batch)
print((time.time() - t) / rows * 1e9)

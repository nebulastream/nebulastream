# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
class Item:
    def __init__(self, name: str, w: int):
        self.name = name
        self.w = w
    def __eq__(self, other):
        return self.name.lower() == other.name.lower()
    def __lt__(self, other):
        return (self.w, self.name) < (other.w, other.name)
    def __hash__(self):
        return hash(self.name.lower())
    def __repr__(self):
        return f"Item({self.name!r}, {self.w})"

def udf(s, n):
    words = s.split() or ["x"]
    items = [Item(w, (n + i) % 3) for i, w in enumerate(words)]
    uniq: list[Item] = []
    for it in items:
        if it not in uniq:
            uniq.append(it)
    srt = sorted(items)
    return str(len(uniq)) + "|" + repr(srt[:3]) + "|" + str(items[0] in items[1:]) + "|" + str(max(items))

# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[5], [-3], [99999]]
class Shape:
    def __init__(self, n: int):
        self.n = n

    def label(self):
        return self.name + ":" + str(self.area())


class Square(Shape):
    name = "square"

    def area(self):
        return self.n * self.n


class Line(Shape):
    @property
    def name(self):
        return "line" + str(self.n)

    def area(self):
        return 0


def udf(n):
    return Square(n).label() + "|" + Line(n).label()

# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
class Shape:
    def __init__(self, side: int):
        self.side = side

    def area(self):
        return self.side * self.side


class Square(Shape):
    def area(self):
        return 4 * self.side


def udf(s):
    return str(Shape(len(s)).area()) + "," + str(Square(len(s)).area())

# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"], ["12"]]
class Shape:
    def __init__(self, side: int):
        self.side = side

    def area(self):
        return self.side * self.side

    @classmethod
    def unit(cls):
        return cls(1)

    @classmethod
    def of(cls, s):
        return cls(len(s)) if s else cls.unit()


class Square(Shape):
    def area(self):
        return 4 * self.side


def udf(s):
    return str(Shape.of(s).area()) + "," + str(Square.of(s).area()) + "," + str(Square.unit().area())

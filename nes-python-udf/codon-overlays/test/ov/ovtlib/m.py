__nes_remove__ = ["doesnotexist_native"]


def helper(s):
    return s.upper()


@__nes_patch__
class Box:
    def __init__(self, s):
        self.s = s

    def show(self):
        return "<" + self.s + ">"


def added(s):
    return helper(s) + kept(s)

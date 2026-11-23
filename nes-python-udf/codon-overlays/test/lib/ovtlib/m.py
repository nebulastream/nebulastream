import doesnotexist_native


def helper(s):
    return doesnotexist_native.upper(s)


class Box:
    def __init__(self, s):
        self.s = s
        self.get = lambda: s

    def show(self):
        return "<" + self.get() + ">"


def kept(s):
    return s + "!"

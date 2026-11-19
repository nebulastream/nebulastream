from lpython import i32

def title_words(s: str) -> str:
    words: list[str] = s.split()
    result: str = ""
    i: i32
    for i in range(len(words)):
        w: str = words[i]
        if i > 0:
            result = result + " "
        head: str = w[:1]
        tail: str = w[1:]
        result = result + head.upper() + tail.lower()
    return result

print(title_words("hello world 42"))

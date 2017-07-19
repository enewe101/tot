def test_read(path):
    f = open(path)
    lines = f.readlines()
    timestamp = float(lines[0])
    tokens = '\n'.join(lines[1:]).split()
    return [(timestamp, tokens)]



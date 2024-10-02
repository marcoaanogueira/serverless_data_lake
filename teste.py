import tempfile

with tempfile.NamedTemporaryFile(delete=False, mode="w+") as f:
    f.write('{"a": 1, "b": 2.0, "c": 1}\n')
    f.write('{"a": 3, "b": 3.0, "c": 2}\n')
    f.write('{"a": 5, "b": 4.0, "c": 3}\n')
    f.write('{"a": 7, "b": 5.0, "c": 4}\n')

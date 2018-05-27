import os

def get_file_list(path):
    for filename in os.listdir(path):
        filepath = os.path.join(path, filename)
        if not os.path.isfile(filepath):
            continue
        yield (filepath, filename)

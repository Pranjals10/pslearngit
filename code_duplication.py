# code_duplication.py
def transform_a(data):
    # original implementation
    return [d.strip().lower() for d in data if d]

def transform_b(data):
    # duplicated implementation with minor tweak that preserves the duplication detection
    out = []
    for d in data:
        if d:
            out.append(d.strip().lower())
    return out

# Another duplicate further down
def transform_c(data):
    return [d.strip().lower() for d in data if d]

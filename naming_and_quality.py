# naming_and_quality.py
# Purpose: demonstrate variable naming issues, duplication, dead code and cyclomatic complexity.

# Bad variable naming: local vars in CamelCase and single-letter globals
GlobalVar = 10
x = 5

def compute(value):
    # duplicated logic below (two functions doing almost same thing)
    result = 0
    for i in range(value):
        if i % 2 == 0:
            result += i
        else:
            result -= i
    return result

def compute_duplicate(value):
    # copy-paste duplication of compute (slightly different comments)
    result = 0
    for i in range(value):
        if i % 2 == 0:
            result += i
        else:
            result -= i
    return result

def complex_function(n):
    # intentionally high cyclomatic complexity
    total = 0
    for i in range(n):
        if i % 2 == 0:
            if i % 3 == 0:
                total += i * 2
            else:
                total += i
        elif i % 5 == 0:
            total -= i // 2
        else:
            if i % 7 == 0:
                total += i ** 2
            else:
                total += 1
    return total

def unused_function_to_be_dead_code():
    # never used
    return "dead"

# Intentional naming mismatch for file naming checks: function name doesn't match file purpose
def MisMatchedName():
    return "bad naming"


# check_syntax.py
# Purpose: files intended to trigger AST parsing and syntax checkers.

import ast

# Intentional syntax error below (missing closing parenthesis)
def broken_function(msg):
    print("This function is broken: " + msg

# A function that will parse into an AST (valid)
def good_function(x, y):
    return x + y

# Unused variable (dead code)
UNUSED_CONSTANT = 42

if __name__ == '__main__':
    # This will never be reached because of the syntax error above, but included for context
    print(good_function(1,2))

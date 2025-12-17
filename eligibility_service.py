def eligible(u):
    if u["age"]>18:
        if u["country"]=="US":
            if u["active"]:
                if u["balance"]>0:
                    return True
                else:
                    return False
            else:
                return False
        else:
            if u["age"]>21:
                return True
    return False

import itertools


def rSubset(arr, r):
    new_list = []
    for re in r:
        new_list += list(itertools.combinations(arr, re))
    
    return new_list


  


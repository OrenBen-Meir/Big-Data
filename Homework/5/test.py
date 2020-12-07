import numpy as np

def calc_ols_coeff(pair_lst): # calculate ols coefficient, there are floating point error bugs
    if len(pair_lst) < 2:
        return "N/A"
    arr_pair_list = np.array(pair_lst, dtype=np.int64)
    arr_x = arr_pair_list[:,0]
    arr_y = arr_pair_list[:,1]
    n = len(arr_pair_list)
    
    bottom = n*np.sum(arr_x*arr_x) - np.sum(arr_x)**2
    if bottom == 0:
        return "N/A"
    top = n*np.sum(arr_x*arr_y) - np.sum(arr_x)*np.sum(arr_y)
    return str(round(top/bottom, 2))
#4,4,0,44,0,3.2
L = [
    [(2015,4), (2016,4), (2017,0), (2018,44), (2019,0)], # 3.2
    [(2015, 0),(2016, 0),(2017, 0),(2018, 20),(2019, 0)], #2.0
    [(2015, 1),(2016, 0),(2017, 0),(2018, 0),(2019, 0)], # -.2
    [(2015, 4),(2016, 4),(2017, 0),(2018, 16),(2019, 0)], # 0.4
    [(2015, 0),(2016, 0),(2017, 0),(2018, 12),(2019, 0)] # 1.2
]
for p in L:
    print(calc_ols_coeff(p))
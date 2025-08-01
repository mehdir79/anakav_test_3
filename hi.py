from first_models import (
    session,
    cities,
    tests,
    parameters,
    test_parameter,
    time_priod,
    work_order_stats,
    work_orders,
)


# with session.begin() as con:
#     ncity = cities("esfahan" , 10)
#     ntest = tests("test 8" , test_num= 8 , majmo_name= " jam")
#     ntp = test_parameter(8 , 1)
#     ntp1 = test_parameter(8 , 2)
    
#     con.add_all([ncity,  ntest ,ntp ,ntp1 ])
with session.begin() as con:

    # wo = work_orders(city_id= 17, test_id=8 , period_id= 1)
    wos = work_order_stats(337, 1 , 40)
    wos2 = work_order_stats(337, 2 , 40)
    con.add_all([  wos, wos2])
    # con.add(wo)
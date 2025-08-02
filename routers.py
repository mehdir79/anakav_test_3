from fastapi import APIRouter, HTTPException
import pandas as pd
from typing import Optional
from sqlalchemy import and_
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
import numpy as np

router = APIRouter()

@router.get("/cities/get_cities")
def get_cities_df():
    with session.begin() as con:
        cities_query = con.query(cities).all()
        citiesdict = {}
        i = 0
        for ct in cities_query:
            citiesdict[i] = {"city_name": ct.name, "code_omor": ct.code_omor}
            i += 1
        
        return pd.DataFrame.from_dict(citiesdict ,orient="index").to_dict(orient="records")
    
@router.post("/cities/creat_city")
def creat_city(name : str , code_omor : int):
    with session.begin() as con:
        try:
            n_city = cities(name=name , code_omor= code_omor)
            con.add(n_city)
            return {"massage" : "city added now you can add records for this city"}
        except:
            return {"massage" : "something went wrong"}
    
@router.delete("/cities/delete_city")
def delete_city(name : str):
    with session.begin() as con:
        try:
            d_city = con.query(cities).where(cities.name == name).first()
            if d_city:
                con.delete(d_city)
                return {"massage" : "deleted successfully"}
            else:
                return {"massage" : "city not found"}
        except:
            return {"massage" : "something went wrong"}

@router.put("/cities/edit_city")
def edit_city(name : str , code_omor : int , new_name : str , new_code_omor:int):
    with session.begin() as con:
        try:
            the_city = con.query(cities).where(and_(cities.name == name , cities.code_omor == code_omor)).first()
            if the_city:
                the_city.name = new_name
                the_city.code_omor = new_code_omor
                con.commit()
                return {"massage" : "city edited successfully"}
            else:
                return {"massage" : "city not found"}    
               
        except:
            return {"massage" : "something went wrong"}
    

@router.get("/tests/get_tests")
def get_tests():
    with session.begin() as con:
        tests_query = con.query(tests).all()
        testsdict = {}
        i = 0
        for tst in tests_query:
            testsdict[i] = {
                "test_id": tst.test_id,
                "test_num": tst.test_num,
                "test_name": tst.test_name,
                "majmo_name": tst.majmo_name,
            }
            i += 1
        
        return pd.DataFrame.from_dict(testsdict ,orient="index").to_dict(orient="records")
    
@router.post("/tests/add_test")
def add_test(test_name : str , test_num : int, test_majmo : str ):
    with session.begin() as con:
        try:
            n_test = tests(name= test_name , test_num= test_num , majmo_name=test_majmo)
            con.add(n_test)
            return {"massage": "test added successfully"}
        except:
            return HTTPException(400 , "something went wrong maybe test_num already exists of the test_name is duplicate")


@router.put("/tests/edit_test")
def edit_test(test_name : Optional[str] , test_num : Optional[int], new_test_name :Optional[str], new_test_num : Optional[int] ,new_test_majmo : Optional[str]):
    with session.begin() as con:
        try:
            if test_name and test_num:
                test = con.query(tests).where(and_(tests.test_name == test_name , tests.test_num == test_num)).first()
                if test:
                    if new_test_majmo or new_test_name or new_test_num:
                        if new_test_num:
                            test.test_num = new_test_num
                        if new_test_name:
                            test.test_name = new_test_name
                        if new_test_majmo:
                            test.majmo_name = new_test_majmo
                        con.commit()
                        return {"massage" : "test edited successfully"}
                    else:
                        return {"massage" : "you should enter at least one parameter to change"}
                return {"massage" : "test does not exist"}
            elif test_name:
                test = con.query(tests).where(tests.test_name == test_name).first()
                if test:
                    if new_test_majmo or new_test_name or new_test_num:
                        if new_test_num:
                            test.test_num = new_test_num
                        if new_test_name:
                            test.test_name = new_test_name
                        if new_test_majmo:
                            test.majmo_name = new_test_majmo
                        con.commit()
                        return {"massage" : "test edited successfully"}
                    else:
                        return {"massage" : "you should enter at least one parameter to change"}
                return {"massage" : "test does not exist"}
            elif test_num:
                test = con.query(tests).where(tests.test_num == test_num).first()
                if test:
                    if new_test_majmo or new_test_name or new_test_num:
                        if new_test_num:
                            test.test_num = new_test_num
                        if new_test_name:
                            test.test_name = new_test_name
                        if new_test_majmo:
                            test.majmo_name = new_test_majmo
                        con.commit()
                        return {"massage" : "test edited successfully"}
                    else:
                        return {"massage" : "you should enter at least one parameter to change"}
                return {"massage" : "test does not exist"}
            else:
                return {"massage" : "enter test_name or test_num"}
        except:
            return HTTPException(400 , "something went wrong maybe test_num already exists of the test_name is duplicate")

@router.delete("/tests/delete_test")
def delete_test(test_name : Optional[str] , test_num : Optional[int]):
    with session.begin() as con:
        try:
            if test_name and test_num:
                test = con.query(tests).where(and_(tests.test_name == test_name , tests.test_num == test_num)).first()
                if test:
                    con.delete(test)
                    return{"massage" : "test deleted successfully"}
                else:
                    return{"massage" : "test not found"}
            elif test_name:
                test = con.query(tests).where(tests.test_name == test_name).first()
                if test:
                    con.delete(test)
                    return{"massage" : "test deleted successfully"}
                else:
                    return{"massage" : "test not found"}
            elif test_num:
                test = con.query(tests).where(tests.test_num == test_num).first()
                if test:
                    con.delete(test)
                    return{"massage" : "test deleted successfully"}
                else:
                    return{"massage" : "test not found"}
            else:
                return{"massage" : "enter test_name or test_num"}
        except:
            return HTTPException(400 , "something went wrong")

    
    

@router.get("/parameters/get_parameters")
def get_parameters():
    with session.begin() as con:
        parameters_query = con.query(parameters).all()
        parametersdict = {}
        i = 0
        for parameter in parameters_query:
            parametersdict[i] = {
                "parameter_id": parameter.parameter_id,
                "parameter_name": parameter.parameter_name,
            }
            i += 1
        return pd.DataFrame.from_dict(parametersdict ,orient="index").to_dict(orient="records")

@router.post("/parameters/add_parameter")
def add_parameter(parameter_name : str):
    with session.begin() as con:
        try:
            if_par = con.query(parameters).where(parameters.parameter_name == parameter_name).first()
            if not if_par:
                n_parameter = parameters(parameter_name=parameter_name)
                con.add(n_parameter)
                return{"massage" : "parameter added successfully"}
            else:
                return{"massage" : "parameter exists"}
        except:
            return HTTPException(400 , "something went wrong")

@router.put("/parameters/edit_parameter")
def edit_parameter(parameter_name : str , new_parameter_name : str):
    with session.begin() as con:
        try:
            par = con.query(parameters).where(parameters.parameter_name == parameter_name).first()
            if par:
                par.parameter_name = new_parameter_name
                con.commit()
                return{"massage" : "parameter edited successfully"}
            else:
                return{"massage" : "parameter does not exist"}
        except:
            return HTTPException(400 , "something went wrong")

@router.delete("/parameters/delete_parameter")
def delete_parameter(parameter_name : str):
    with session.begin() as con:
        try:
            par = con.query(parameters).where(parameters.parameter_name == parameter_name).first()
            if par:
                con.delete(par)
                return{"massage" : "parameter deleted successfully"}
            else:
                return{"massage" : "parameter does not exist"}
        except:
            return HTTPException(400 , "something went wrong")


@router.get("/test_parameters/get_test_parameters")
def get_test_parameters(test_num : int):
    with session.begin() as con:
        tests_pars = {}
        i = 0
        test = con.query(tests).where(tests.test_num == test_num).first()
        if test:
            test_par : test_parameter
            for test_par in test.test_parameter_r:
                tests_pars[i] = test_par.parameter_r.parameter_name
                i+=1
            return tests_pars
        else:
            return {"massage" : "test does not exist"}


@router.post("/test_parameters/add_parameter_to_test")
def add_parameter_to_test(test_num : int , parameter_name : str):
    with session.begin() as con:
        try:
            test = con.query(tests).where(tests.test_num == test_num).first()
            par = con.query(parameters).where(parameters.parameter_name == parameter_name).first()
            if test:
                if par:
                    if_exist = con.query(test_parameter).where(and_(test_parameter.parameter_id == par.parameter_id , test_parameter.test_id == test.test_id)).first()
                    if not if_exist:
                        n_test_parameter = test_parameter(test_id=test.test_id ,parameter_id= par.parameter_id)
                        con.add(n_test_parameter)
                        return{"massage" : "parameter added to test successfully"}
                    else:
                        return {"massage" : "the test already has the parameter"}
                return {"massage" : "parameter does not exist try adding it first"}
            else:
                return{"massage" : "test does not exist try adding it"}
        except:
            return HTTPException(400, "something went wrong")


@router.delete("/test_parameters/delete_parameter_from_test")
def delete_parameter_from_test(test_num : int , parameter_name : str):
    with session.begin() as con:
        try:
            test = con.query(tests).where(tests.test_num == test_num).first()
            par = con.query(parameters).where(parameters.parameter_name == parameter_name).first()
            if test:
                if par:
                    if_exist = con.query(test_parameter).where(and_(test_parameter.parameter_id == par.parameter_id , test_parameter.test_id == test.test_id)).first()
                    if if_exist:
                        con.delete(if_exist)
                        return{"massage" : "parameter deleted from test successfully"}
                    else:
                        return {"massage" : "the test does not have the parameter"}
                return {"massage" : "parameter does not exist try adding it first"}
            else:
                return{"massage" : "test does not exist try adding it"}
        except:
            return HTTPException(400, "something went wrong")
        
#no editing for some reasons!


@router.get("/time_perids/get_time_perids")
def get_time_periods():
    with session.begin() as con:
        time_periods_query = con.query(time_priod).all()
        time_periodsdict = {}
        i = 0
        for time_period in time_periods_query:
            time_periodsdict[i] = {
                "year": time_period.year,
                "month": time_period.month,
            }
            i += 1
        return pd.DataFrame.from_dict(time_periodsdict ,orient="index").to_dict(orient="records")

@router.post("/time_perids/add_time_perids")
def add_time_periods(year : int, month : int):
    with session.begin() as con:
        try:
            time_period_query = con.query(time_priod).where(and_(time_priod.month == month , time_priod.year == year)).first()
            if not time_period_query:
                n_tp = time_priod(year = year , month= month)
                con.add(n_tp)
                return{"massage":"time period added successfully"}
            else:
                return {"maasage" : "time period exists"}
        except:
            return HTTPException(400,"something went wrong")


@router.put("/time_perids/edit_time_perids")
def edit_time_periods(year : int, month : int , new_year :int, new_month:int):
    with session.begin() as con:
        try:
            time_period_query = con.query(time_priod).where(and_(time_priod.month == month , time_priod.year == year)).first()
            time_period_query1 = con.query(time_priod).where(and_(time_priod.month == new_month , time_priod.year == new_year)).first()
            if time_period_query :
                if not time_period_query1:
                    time_period_query.month = new_month
                    time_period_query.year = new_year
                    con.commit()
                    return{"massage":"time period editted successfully"}
                else:
                    return{"massage" : "the new time period exists try deleting the current"}
            else:
                return {"maasage" : "time period does not exist"}
        except:
            return HTTPException(400,"something went wrong")

@router.delete("/time_perids/delete_time_perids")
def delete_time_periods(year : int, month : int):
    with session.begin() as con:
        try:
            time_period_query = con.query(time_priod).where(and_(time_priod.month == month , time_priod.year == year)).first()
            if time_period_query:
                con.delete(time_period_query)
                return{"massage":"time period deleted successfully"}
            else:
                return {"maasage" : "time period does not exist"}
        except:
            return HTTPException(400,"something went wrong")

@router.get("/workorders/get_workorders")
def get_workorders(test_num : Optional[int] = None , city_name : Optional[str] =None , year: Optional[int]=None , month : Optional[int]=None):
    with session.begin() as con:
        work_orders_result_dict = {}
        if test_num and city_name and year and month:
            city = con.query(cities).where(cities.name == city_name).first()
            if city:
                tp_q = con.query(time_priod).where(and_(time_priod.year == year,time_priod.month == month)).first()
                if tp_q:
                    test = con.query(tests).where(tests.test_num == test_num).first()
                    if test:
                        worders = con.query(work_orders).where(and_(work_orders.city_id == city.city_id , work_orders.test_id == test.test_id , work_orders.period_id == tp_q.period_id) ).all()
                        if worders:
                            for wo in worders:
                                worderdict = {}
                                worderdict["نام شهر"] = wo.city_r.name
                                worderdict["شماره تست"] = wo.test_r.test_num
                                worderdict["سال و ماه"] = {"سال":wo.period_r.year , "ماه":wo.period_r.month}
                            return pd.DataFrame.from_dict(work_orders_result_dict , orient="index").to_dict(orient="records")
                        else:
                            return {"massage" : "no work order for this city in this time period and test"}
                    else:
                        return{"test does not exist"}
                else:
                    return{"massage" : "time period does not exist"}
            else:
                return{"massage" : "city does not exist"}
                    
        elif city_name and year and month:
            city = con.query(cities).where(cities.name == city_name).first()
            if city:
                tp_q = con.query(time_priod).where(and_(time_priod.year == year,time_priod.month == month)).first()
                if tp_q:
                    worders = con.query(work_orders).where(and_(work_orders.city_id == city.city_id ,  work_orders.period_id == tp_q.period_id) ).all()
                    if worders:
                        i = 0
                        for wo in worders:
                            worderdict = {}
                            worderdict["نام شهر"] = wo.city_r.name
                            worderdict["شماره تست"] = wo.test_r.test_num
                            worderdict["سال و ماه"] = {"سال":wo.period_r.year , "ماه":wo.period_r.month}
                            work_orders_result_dict[i] = worderdict
                            i+=1
                        return pd.DataFrame.from_dict(work_orders_result_dict , orient="index").to_dict(orient="records")
                    else:
                        return {"massage" : "no work order for this city in this time period and test"}
                else:
                    return{"massage" : "time period does not exist"}
            else:
                return{"massage" : "city does not exist"}

            
        elif test_num and city_name:
            city = con.query(cities).where(cities.name == city_name).first()
            if city:
                test = con.query(tests).where(tests.test_num == test_num).first()
                if test:
                    worders = con.query(work_orders).where(and_(work_orders.city_id == city.city_id , work_orders.test_id == test.test_id ) ).all()
                    if worders:
                        i = 0
                        for wo in worders:
                            worderdict = {}
                            worderdict["نام شهر"] = wo.city_r.name
                            worderdict["شماره تست"] = wo.test_r.test_num
                            worderdict["سال و ماه"] = {"سال":wo.period_r.year , "ماه":wo.period_r.month}
                            work_orders_result_dict[i] = worderdict
                            i+=1
                        return pd.DataFrame.from_dict(work_orders_result_dict , orient="index").to_dict(orient="records")
                    else:
                        return {"massage" : "no work order for this city in this time period and test"}
                else:
                    return{"test does not exist"}
            else:
                return{"massage" : "city does not exist"}



        elif test_num and year and month:
            test = con.query(tests).where(tests.test_num == test_num).first()
            if test:
                tp_q = con.query(time_priod).where(and_(time_priod.year == year,time_priod.month == month)).first()
                if tp_q:
                    worders = con.query(work_orders).where(and_( work_orders.test_id == test.test_id , work_orders.period_id == tp_q.period_id) ).all()
                    if worders:
                        i = 0
                        for wo in worders:
                            worderdict = {}
                            worderdict["نام شهر"] = wo.city_r.name
                            worderdict["شماره تست"] = wo.test_r.test_num
                            worderdict["سال و ماه"] = {"سال":wo.period_r.year , "ماه":wo.period_r.month}
                            work_orders_result_dict[i] = worderdict
                            i+=1
                        return pd.DataFrame.from_dict(work_orders_result_dict , orient="index").to_dict(orient="records")
                    else:
                        return {"massage" : "no work order for this city in this time period and test"}
                else:
                    return{"massage" : "time period does not exist"}
            else:
                return{"test does not exist"}


        elif year and month:
            tp_q = con.query(time_priod).where(and_(time_priod.year == year,time_priod.month == month)).first()
            if tp_q:
                worders = con.query(work_orders).where( work_orders.period_id == tp_q.period_id).all()
                if worders:
                    i = 0
                    for wo in worders:
                        worderdict = {}
                        worderdict["نام شهر"] = wo.city_r.name
                        worderdict["شماره تست"] = wo.test_r.test_num
                        worderdict["سال و ماه"] = {"سال":wo.period_r.year , "ماه":wo.period_r.month}
                        work_orders_result_dict[i] = worderdict
                        i+=1
                    return pd.DataFrame.from_dict(work_orders_result_dict , orient="index").to_dict(orient="records")
                else:
                    return {"massage" : "no work order for this city in this time period and test"}
            else:
                return{"massage" : "time period does not exist"}
            

        elif city_name:
            city = con.query(cities).where(cities.name == city_name).first()
            if city:
                worders = con.query(work_orders).where(work_orders.city_id == city.city_id).all()
                if worders:
                    i = 0
                    for wo in worders:
                        worderdict = {}
                        worderdict["نام شهر"] = wo.city_r.name
                        worderdict["شماره تست"] = wo.test_r.test_num
                        worderdict["سال و ماه"] = {"سال":wo.period_r.year , "ماه":wo.period_r.month}
                        work_orders_result_dict[i] = worderdict
                        i+=1
                    return pd.DataFrame.from_dict(work_orders_result_dict , orient="index").to_dict(orient="records")
                else:
                    return {"massage" : "no work order for this city in this time period and test"}
            else:
                return{"massage" : "city does not exist"}
            

        elif test_num:
            test = con.query(tests).where(tests.test_num == test_num).first()
            if test:
                worders = con.query(work_orders).where(work_orders.test_id == test.test_id).all()
                print(len(worders))
                if worders:
                    i = 0
                    for wo in worders:
                        worderdict = {}
                        worderdict["نام شهر"] = wo.city_r.name
                        worderdict["شماره تست"] = wo.test_r.test_num
                        worderdict["سال و ماه"] = {"سال":wo.period_r.year , "ماه":wo.period_r.month}
                        work_orders_result_dict[i] = worderdict
                        i+=1
                    return pd.DataFrame.from_dict(work_orders_result_dict , orient="index").to_dict(orient="records")
                else:
                    return {"massage" : "no work order for this city in this time period and test"}
            else:
                return{"test does not exist"}
        elif year:
            tp_q1 = con.query(time_priod).where(time_priod.year == year).all()
            if tp_q1:
                i = 0
                for tp_q in tp_q1:
                    worders = con.query(work_orders).where( work_orders.period_id == tp_q.period_id).all()
                    if worders:
                        for wo in worders:
                            worderdict = {}
                            worderdict["نام شهر"] = wo.city_r.name
                            worderdict["شماره تست"] = wo.test_r.test_num
                            worderdict["سال و ماه"] = {"سال":wo.period_r.year , "ماه":wo.period_r.month}
                            work_orders_result_dict[i] = worderdict
                            i+=1
                    else:
                        continue
                return pd.DataFrame.from_dict(work_orders_result_dict , orient="index").to_dict(orient="records")
            else:
                return{"time period does not exist"}
        elif not year and not month and not city_name and not test_num:
            worders = con.query(work_orders).all()
            if worders:
                i = 0
                for wo in worders:
                    worderdict = {}
                    worderdict["نام شهر"] = wo.city_r.name
                    worderdict["شماره تست"] = wo.test_r.test_num
                    worderdict["سال و ماه"] = {"سال":wo.period_r.year , "ماه":wo.period_r.month}
                    work_orders_result_dict[i] = worderdict
                    i+=1
                return pd.DataFrame.from_dict(work_orders_result_dict , orient="index").to_dict(orient="records")
            else:
                return {"massage" : "no work order for this city in this time period and test"}


                

@router.post("/work_orders/add_work_order")
def add_work_order(city_name : str, test_num : int, year : int , month : int ):
    with session.begin() as con:
        ct = con.query(cities).where(cities.name == city_name).first()
        tst = con.query(tests).where(tests.test_num == test_num).first()
        tp = con.query(time_priod).where(and_(time_priod.month == month , time_priod.year == year)).first()
        if ct and tst and tp:
            if_wo = con.query(work_orders).where(and_(work_orders.city_id == ct.city_id , work_orders.test_id == tst.test_id , work_orders.period_id == tp.period_id)).first()
            if not if_wo:
                n_work_order = work_orders(test_id=tst.test_id , city_id= ct.city_id , period_id= tp.period_id)
                con.add(n_work_order)
                return {"massage" : "work order added"}
            else:
                return{"masage" : "work order already exists"}
        else:
            return{"masage" : "inputs are wrong or dont exist"}

@router.delete("/work_orders/delete_work_order")
def delete_work_order(city_name : str, test_num : int, year : int , month : int ):
    with session.begin() as con:
        ct = con.query(cities).where(cities.name == city_name).first()
        tst = con.query(tests).where(tests.test_num == test_num).first()
        tp = con.query(time_priod).where(and_(time_priod.month == month , time_priod.year == year)).first()
        if ct and tst and tp:
            if_wo = con.query(work_orders).where(and_(work_orders.city_id == ct.city_id , work_orders.test_id == tst.test_id , work_orders.period_id == tp.period_id)).first()
            if if_wo:
                con.delete(if_wo)
                return {"massage" : "work order deleted"}
            else:
                return{"masage" : "work order does not exists"}
        else:
            return{"masage" : "inputs are wrong or dont exist"}

import numpy as np

def clean_nans(obj):
    if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return 0.0  
    elif isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nans(i) for i in obj]
    else:
        return obj


@router.get("/workorders_stats/get_workorders_stats")
def get_specific_test_data(
    city_name: Optional[str] = None, test_num: Optional[int] = None, year: Optional[int] = None, month: Optional[int] = None
):
    with session.begin() as con:
        test_result_dict = {}
        if city_name and test_num and year and month:
            ct = con.query(cities).where(cities.name == city_name).first()
            i = 0
            worderdict = {}
            if ct:
                if len(ct.work_orders_r) != 0:
                    wo : work_orders
                    for wo in ct.work_orders_r:
                        majmo = 0
                        if wo.test_r.test_num == test_num:
                            if wo.period_r.year == year and wo.period_r.month == month:
                                wos : work_order_stats
                                if len(wo.work_order_stats_r) != 0:
                                    worderdict = {"نام شهر": ct.name, "شماره تست":wo.test_r.test_num , "سال" : wo.period_r.year , "ماه" : wo.period_r.month }
                                    for wos in wo.work_order_stats_r:
                                        worderdict[wos.parameter_r.parameter_name] = wos.count
                                        majmo +=wos.count
                                    worderdict[wo.test_r.majmo_name] = majmo
                                    test_result_dict[i] = worderdict
                                    i+=1
                                else:
                                    continue
                            else:
                                continue
                        else:
                            continue
                    if i == 0 :
                        return {"masssage" : "there is no stats for this work order"}
                    else:
                        return clean_nans(pd.DataFrame.from_dict(test_result_dict , orient="index").to_dict(orient="records"))
                else:
                    return{"massager" : "this city has no work order"}
            else:
                return{"massage" : "no such city"}
                        
        elif test_num and year and month:
            ct_q = con.query(cities).all()
            i = 0
            worderdict = {}
            c=0
            if ct_q:
                for ct in ct_q:
                    if len(ct.work_orders_r) != 0:
                        c+=1
                        wo : work_orders
                        for wo in ct.work_orders_r: 
                            majmo = 0
                            if wo.test_r.test_num == test_num:
                                if wo.period_r.year == year and wo.period_r.month == month:
                                    wos : work_order_stats
                                    if len(wo.work_order_stats_r) != 0:
                                        worderdict = {"نام شهر": ct.name, "شماره تست":wo.test_r.test_num , "سال" : wo.period_r.year , "ماه" : wo.period_r.month }
                                        for wos in wo.work_order_stats_r:
                                            worderdict[wos.parameter_r.parameter_name] = wos.count
                                            majmo +=wos.count
                                        worderdict[wo.test_r.majmo_name] = majmo
                                        test_result_dict[i] = worderdict
                                        i+=1
                                else:
                                    continue
                            else:
                                continue
                    else:
                        continue
                if c == 0:
                    return{"massage" : "no work order"}
                elif i == 0 :
                    return {"masssage" : "there is no stats for this work order"}
                else:
                    return clean_nans(pd.DataFrame.from_dict(test_result_dict , orient="index").to_dict(orient="records"))
            else:
                return{"massage" : "no city"}
            
                        
        

        elif test_num and city_name:
            ct = con.query(cities).where(cities.name == city_name).first()
            i = 0
            worderdict = {}
            c = 0
            if ct:
                print(ct.name)
                if len(ct.work_orders_r) != 0:
                    c+=1
                    wo : work_orders
                    for wo in ct.work_orders_r: 
                        majmo = 0
                        if wo.test_r.test_num == test_num:
                            wos : work_order_stats
                            if len(wo.work_order_stats_r) != 0:
                                worderdict = {"نام شهر": ct.name, "شماره تست":wo.test_r.test_num , "سال" : wo.period_r.year , "ماه" : wo.period_r.month }
                                for wos in wo.work_order_stats_r:
                                    worderdict[wos.parameter_r.parameter_name] = wos.count
                                    majmo += wos.count
                                worderdict[wo.test_r.majmo_name] = majmo
                                test_result_dict[i] = worderdict
                                i+=1
                            else:
                                continue
                        else:
                            continue
                if c==0:
                    return{"masssager" : "there is no work orders for this city"}
                elif i == 0 :
                    return {"masssage" : "there is no stats for this work order"}
                else:
                    return clean_nans(pd.DataFrame.from_dict(test_result_dict , orient="index").to_dict(orient="records"))
            else:
                return {"massage": "no such city exists"}
        elif city_name and year and month:
            ct_q = con.query(cities).all()
            i = 0
            c=0
            worderdict = {}
            if ct_q:
                for ct in ct_q:
                    if len(ct.work_orders_r) != 0:
                        c+=1
                        wo : work_orders
                        for wo in ct.work_orders_r: 
                            if wo.period_r.year == year and wo.period_r.month == month:
                                wos : work_order_stats
                                if len(wo.work_order_stats_r) != 0:
                                    worderdict = {"نام شهر": ct.name, "شماره تست":wo.test_r.test_num , "سال" : wo.period_r.year , "ماه" : wo.period_r.month }
                                    for wos in wo.work_order_stats_r:
                                        worderdict[wos.parameter_r.parameter_name] = wos.count
                                    test_result_dict[i] = worderdict
                                    i+=1
                                else:
                                    continue
                            else:
                                continue
                        if i == 0 :
                            return {"masssage" : "there is no stats for this work order"}
                        else:
                            return clean_nans(pd.DataFrame.from_dict(test_result_dict , orient="index").to_dict(orient="records"))
                    else:
                        continue
                if c == 0:
                    return{"massage" : "there is no work order fo cities"}
            else:
                return{"massage" : "there is no city"}
                        
        elif city_name:
            ct = con.query(cities).where(cities.name == city_name).first()
            i = 0
            worderdict = {}
            if ct:
                if len(ct.work_orders_r) != 0:
                    wo : work_orders
                    for wo in ct.work_orders_r:
                        i+=1
                        wos : work_order_stats
                        if len(wo.work_order_stats_r) != 0:
                            worderdict = {"نام شهر": ct.name, "شماره تست":wo.test_r.test_num , "سال" : wo.period_r.year , "ماه" : wo.period_r.month }
                            for wos in wo.work_order_stats_r:
                                worderdict[wos.parameter_r.parameter_name] = wos.count
                            test_result_dict[i] = worderdict
                            
                        else:
                            continue
                    if i == 0 :
                        return {"masssage" : "there is no stats for this work order"}
                    else:
                        return clean_nans(pd.DataFrame.from_dict(test_result_dict , orient="index").to_dict(orient="records"))
                else:
                    return{"massager" : "this city has no work order"}
            else:
                return{"massage" : "no such city"}
                        
        elif year and month:
            ct_q = con.query(cities).all()
            i = 0
            c=0
            worderdict = {}
            if ct_q:
                for ct in ct_q:
                    if len(ct.work_orders_r) != 0:
                        wo : work_orders
                        for wo in ct.work_orders_r: 
                            c+=1
                            if wo.period_r.year == year and wo.period_r.month == month:
                                wos : work_order_stats
                                if len(wo.work_order_stats_r) != 0:
                                    worderdict = {"نام شهر": ct.name, "شماره تست":wo.test_r.test_num , "سال" : wo.period_r.year , "ماه" : wo.period_r.month }
                                    for wos in wo.work_order_stats_r:
                                        worderdict[wos.parameter_r.parameter_name] = wos.count
                                    test_result_dict[i] = worderdict
                                    i+=1
                                else:
                                    continue
                            else:
                                continue
                    else:
                        continue
                if c == 0:
                    return{"massage":"there is no work order of this cities"}
                elif i == 0 :
                    return {"masssage" : "there is no stats for this work order"}
                else:
                    return clean_nans(pd.DataFrame.from_dict(test_result_dict , orient="index").to_dict(orient="records"))
            else:
                return{"massage" : "there is no city"}
        elif year:
            ct_q = con.query(cities).all()
            i = 0
            c = 0
            worderdict = {}
            if ct_q:
                for ct in ct_q:
                    if len(ct.work_orders_r) != 0:
                        c+=1
                        wo : work_orders
                        for wo in ct.work_orders_r: 
                            if wo.period_r.year == year:
                                wos : work_order_stats
                                if len(wo.work_order_stats_r) != 0:
                                    worderdict = {"نام شهر": ct.name, "شماره تست":wo.test_r.test_num , "سال" : wo.period_r.year , "ماه" : wo.period_r.month }
                                    for wos in wo.work_order_stats_r:
                                        worderdict[wos.parameter_r.parameter_name] = wos.count
                                    test_result_dict[i] = worderdict
                                    i+=1
                                else:
                                    continue
                            else:
                                continue
                    else:
                        continue
                if c==0:
                    return {"massage" : "there is no workorder for this cities"}
                elif i == 0 :
                    return {"masssage" : "there is no stats for this work order"}
                else:
                    return clean_nans(pd.DataFrame.from_dict(test_result_dict , orient="index").to_dict(orient="records"))
            else:
                return{"massage" : "there is no city"}

                
        elif test_num:
            ct_q = con.query(cities).all()
            i = 0
            c = 0
            worderdict = {}
            if ct_q:
                for ct in ct_q:
                    if len(ct.work_orders_r) != 0:
                        c+=1
                        wo : work_orders
                        for wo in ct.work_orders_r: 
                            majmo = 0
                            if wo.test_r.test_num == test_num:
                                wos : work_order_stats
                                if len(wo.work_order_stats_r) != 0:
                                    worderdict = {"نام شهر": ct.name, "شماره تست":wo.test_r.test_num , "سال" : wo.period_r.year , "ماه" : wo.period_r.month }
                                    for wos in wo.work_order_stats_r:
                                        worderdict[wos.parameter_r.parameter_name] = wos.count
                                        majmo += wos.count
                                    worderdict[wo.test_r.majmo_name] = majmo
                                    test_result_dict[i] = worderdict
                                    i+=1
                                else:
                                    continue
                            else:
                                continue
                    else:
                        continue
                if c == 0:
                    return{"massage":"cities hav no work orders"}
                elif i == 0 :
                    return {"masssage" : "there is no stats for this work order"}
                else:
                    return clean_nans(pd.DataFrame.from_dict(test_result_dict , orient="index").to_dict(orient="records"))
            else:
                return {"massage" : "there is no city"}
        elif month:
            ct_q = con.query(cities).all()
            i = 0
            c = 0
            worderdict = {}
            if ct_q:
                for ct in ct_q:
                    if len(ct.work_orders_r) != 0:
                        c+=1
                        wo : work_orders
                        for wo in ct.work_orders_r: 
                            if wo.period_r.month == month:
                                wos : work_order_stats
                                if len(wo.work_order_stats_r) != 0:
                                    worderdict = {"نام شهر": ct.name, "شماره تست":wo.test_r.test_num , "سال" : wo.period_r.year , "ماه" : wo.period_r.month }
                                    for wos in wo.work_order_stats_r:
                                        worderdict[wos.parameter_r.parameter_name] = wos.count
                                    test_result_dict[i] = worderdict
                                    i+=1
                                else:
                                    continue
                            else:
                                continue
                    else:
                        continue
                if c==0:
                    return{"massage" : "cities have no work order"}
                elif i == 0 :
                    return {"masssage" : "there is no stats for this work order"}
                else:
                    return clean_nans(pd.DataFrame.from_dict(test_result_dict , orient="index").to_dict(orient="records"))
            else:
                return{"massage":"there is no city"}
            
        else:
            ct_q = con.query(cities).all()
            i = 0
            c = 0
            worderdict = {}
            if ct_q:
                for ct in ct_q:
                    if len(ct.work_orders_r) != 0:
                        c+=1
                        wo : work_orders
                        for wo in ct.work_orders_r: 
                            worderdict = {"نام شهر": ct.name, "شماره تست":wo.test_r.test_num , "سال" : wo.period_r.year , "ماه" : wo.period_r.month }
                            wos : work_order_stats
                            if len(wo.work_order_stats_r) != 0:
                                for wos in wo.work_order_stats_r:
                                    worderdict[wos.parameter_r.parameter_name] = wos.count
                                test_result_dict[i] = worderdict
                                i+=1
                            else:
                                continue
                    else:
                        continue
                if c==0:
                    return{"massage" : "cities have no work order"}
                elif i == 0 :
                    return {"masssage" : "there is no stats for this work order"}
                else:
                    return clean_nans(pd.DataFrame.from_dict(test_result_dict , orient="index").to_dict(orient="records"))
            else:
                return{"massage":"there is no city"}


@router.post("/work_order_stats/add_work_order_stat")
def add_work_order_stat(city_name : str, test_num : int, year : int , month : int , par :str , count):
    with session.begin() as con:
        ct = con.query(cities).where(cities.name == city_name).first()
        tst = con.query(tests).where(tests.test_num == test_num).first()
        tp = con.query(time_priod).where(and_(time_priod.year == year , time_priod.month == month)).first()
        pr = con.query(parameters).where(parameters.parameter_name == par).first()
        if ct and tst and tp and pr:
            wo = con.query(work_orders).where(and_(work_orders.city_id == ct.city_id , work_orders.period_id == tp.period_id , work_orders.test_id == tst.test_id)).first()
            if wo:
                wos = con.query(work_order_stats).where(and_(work_order_stats.work_order_id == wo.work_order_id , work_order_stats.parameter_id == pr.parameter_id)).first()
                if not wos:
                    n_wos = work_order_stats(wo.work_order_id , pr.parameter_id , count)
                    con.add(n_wos)
                    return{"massage" : "work order stat successfully added"}
                else:
                    return {"massage" : "work order stat already exists"}
            else:
                return {"massage" : "work order does not exist"}
        else:
            return {"massage" : "inputs are wrong or they dont exist"}

@router.delete("/work_order_stats/delete_work_order_stat")
def delete_work_order_stat(city_name : str, test_num : int, year : int , month : int , par :str):
    with session.begin() as con:
        ct = con.query(cities).where(cities.name == city_name).first()
        tst = con.query(tests).where(tests.test_num == test_num).first()
        tp = con.query(time_priod).where(and_(time_priod.year == year , time_priod.month == month)).first()
        pr = con.query(parameters).where(parameters.parameter_name == par).first()
        if ct and tst and tp and pr:
            wo = con.query(work_orders).where(and_(work_orders.city_id == ct.city_id , work_orders.period_id == tp.period_id , work_orders.test_id == tst.test_id)).first()
            if wo:
                wos = con.query(work_order_stats).where(work_order_stats.work_order_id == wo.work_order_id , work_order_stats.parameter_id == pr.parameter_id).first()
                if wos:
                    con.delete(wos)
                    return{"massage" : "work order stat successfully deleted"}
            else:
                return {"massage" : "work order does not exist"}
        else:
            return {"massage" : "inputs are wrong or they dont exist"}


@router.post("/folders/add_folder")
def add_folder(city_name : str, test_num : int, year : int , month : int , par :str,  count : int):
    with session.begin() as con:
        ct = con.query(cities).where(cities.name == city_name).first()
        tst = con.query(tests).where(tests.test_num == test_num).first()
        tp = con.query(time_priod).where(and_(time_priod.year == year , time_priod.month == month)).first()
        if ct and tst and tp:
            wo = con.query(work_orders).where(and_(work_orders.city_id == ct.city_id , work_orders.test_id == tst.test_id , work_orders.period_id == tp.period_id)).first()
            if wo:
                parameter = con.query(parameters).where(parameters.parameter_name == par).first()
                if parameter:
                    wos = con.query(work_order_stats).where(and_(work_order_stats.parameter_id == parameter.parameter_id , work_order_stats.work_order_id == wo.work_order_id)).first()
                    if wos:
                        wos.count += count
                        return{"massage" : "folder/folders added successfully"}
                    else:
                        return{"massage" : "test does not have the parameter"}
                else:
                    return{"massage" : "parameter not found try adding it"}
            return{"massage" : "work order not found try adding it"}
        return{"massage" : "city not found try adding it"}


@router.delete("/folders/delete_folder")
def delete_folder(city_name : str, test_num : int, year : int , month : int , par :str,  count : int):
    with session.begin() as con:
        ct = con.query(cities).where(cities.name == city_name).first()
        tst = con.query(tests).where(tests.test_num == test_num).first()
        tp = con.query(time_priod).where(and_(time_priod.year == year , time_priod.month == month)).first()
        if ct and tst and tp:
            wo = con.query(work_orders).where(and_(work_orders.city_id == ct.city_id , work_orders.test_id == tst.test_id , work_orders.period_id == tp.period_id)).first()
            if wo:
                parameter = con.query(parameters).where(parameters.parameter_name == par).first()
                if parameter:
                    wos = con.query(work_order_stats).where(and_(work_order_stats.parameter_id == parameter.parameter_id , work_order_stats.work_order_id == wo.work_order_id)).first()
                    if wos:
                        if wos.count >= count:
                            wos.count -= count
                            con.commit()
                            return{"massage" , "folder/folders deleted successfully"}
                        else:
                            return{"massage" : f"no enouph folder/folders left to delete try less or equal to {wos.count}"}
                    else:
                        return{"massage" : "work order stat not found"}
                else:
                    return{"massage" : "parameter not found try adding it"}
            return{"massage" : "work order not found try adding it"}
        return{"massage" : "city not found try adding it"}


            


#برای انتقال یک یا چند پرونده از یک وضعیت به یک وضعیت دیگر
@router.put("/folders/move_folder")
def move_folder(city_name : str, test_num : int, year : int , month : int , from_par : str , to_par : str , count : int):
    with session.begin() as con:
        ct = con.query(cities).where(cities.name == city_name).first()
        tst = con.query(tests).where(tests.test_num == test_num).first()
        tp = con.query(time_priod).where(and_(time_priod.year == year , time_priod.month == month)).first()
        if ct:
            if tst:
                if tp:
                    wo = con.query(work_orders).where(and_(work_orders.city_id == ct.city_id , work_orders.test_id == tst.test_id , work_orders.period_id == tp.period_id)).first()
                    if wo:
                        wos : work_order_stats
                        pr : parameters
                        fr_pr = None
                        to_pr = None
                        for wos in wo.work_order_stats_r:
                            pr : parameters = wos.parameter_r
                            if pr.parameter_name == from_par:
                                if wos.count >= count :
                                    fr_pr = wos
                                else:
                                    return HTTPException(404 , "no enough folder to move")
                            elif pr.parameter_name == to_par:
                                to_pr = wos
                        if fr_pr:
                            if to_pr:
                                fr_pr.count -= count
                                to_pr.count +=count
                                return{"massage" : "transport done"}
                                con.commit()
                            else:
                                return HTTPException(404,"test does not have the to pamater")
                        else:
                            return HTTPException(404 , "test does not have the from parameter")
                    else:
                        return HTTPException(404 , "there is no workorder for this city,test and time period")
                else:
                    return HTTPException(404 , "time period not found")
            else:
                return HTTPException(404 , "test not founf")
        else:
            return HTTPException(404 , "city not found")
        

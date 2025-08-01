from fastapi import APIRouter, Query , HTTPException
import pandas as pd
from typing import Dict, List, Optional, Union
from sqlalchemy import create_engine, and_, func
from sqlalchemy.orm import sessionmaker
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
import traceback
import numpy as np
from pydantic import BaseModel

router = APIRouter()

@router.get("cities/get_cities")
def get_cities_df():
    with session.begin() as con:
        cities_query = con.query(cities).all()
        citiesdict = {}
        i = 0
        for ct in cities_query:
            citiesdict[i] = {"city_name": ct.name, "code_omor": ct.code_omor}
            i += 1
        df = pd.DataFrame.from_dict(citiesdict, orient="index")
        return df.to_json(orient="table")
    
@router.post("cities/creat_city")
def creat_city(name : str , code_omor : int):
    with session.begin() as con:
        try:
            n_city = cities(name=name , code_omor= code_omor)
            con.add(n_city)
            return {"massage" : "city added now you can add records for this city"}
        except:
            return {"massage" : "something went wrong"}
    
@router.delete("cities/delete_city")
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

@router.put("cities/edit_city")
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
    

@router.get("tests/get_tests")
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
        df = pd.DataFrame.from_dict(testsdict, orient="index")
        return df.to_json(orient="table")
    
@router.post("tests/add_test")
def add_test(test_name : str , test_num : int, test_majmo : str ):
    with session.begin() as con:
        try:
            n_test = tests(name= test_name , test_num= test_num , majmo_name=test_majmo)
            con.add(n_test)
            return {"massage": "test added successfully"}
        except:
            return HTTPException(400 , "something went wrong maybe test_num already exists of the test_name is duplicate")


@router.put("tests/edit_test")
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

@router.delete("tests/delete_test")
def delete_test(test_name : Optional[str] , test_num : Optional[int]):
    with session.begin() as con:
        try:
            if test_name and test_num:
                test = con.query(tests).where(and_(tests.test_name == test_name , tests.test_num == test_num).first())
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

    
    

@router.get("/get_parameters")
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
        df = pd.DataFrame.from_dict(parametersdict, orient="index")
        return df.to_json(orient="table")

@router.post("/add_parameter")
def add_parameter(parameter_name : str):
    with session.begin() as con:
        try:
            if_par = con.query(parameters).where(parameters.parameter_name == parameter_name)
            if not if_par:
                n_parameter = parameters(parameter_name=parameter_name)
                con.add(n_parameter)
                return{"massage" : "parameter added successfully"}
            else:
                return{"massage" : "parameter exists"}
        except:
            return HTTPException(400 , "something went wrong")

@router.put("/edit_parameter")
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

@router.delete("/delete_parameter")
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



@router.get("/get_time_perids")
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
        df = pd.DataFrame.from_dict(time_periodsdict, orient="index")
        return df.to_json(orient="table")

@router.post("/add_time_perids")
def add_time_periods(year : int, month : int):
    with session.begin() as con:
        try:
            time_period_query = con.query(time_priod).where(and_(time_priod.month == month , time_priod.year == year)).first()
            if not time_period_query:
                n_tp = time_priod(year = year , month= month)
                return{"massage":"time period added successfully"}
            else:
                return {"maasage" : "time period exists"}
        except:
            return HTTPException(400,"something went wrong")


@router.delete("/delete_time_perids")
def delete_time_periods(year : int, month : int , new_year :int, new_month:int):
    with session.begin() as con:
        try:
            time_period_query = con.query(time_priod).where(and_(time_priod.month == month , time_priod.year == year)).first()
            time_period_query1 = con.query(time_priod).where(and_(time_priod.month == new_month , time_priod.year == new_year)).first()
            if time_period_query :
                if not time_period_query1:
                    time_period_query.month = new_month
                    time_period_query.year = new_year
                    return{"massage":"time period editted successfully"}
                else:
                    return{"massage" : "the new time period exists try deleting the current"}
            else:
                return {"maasage" : "time period does not exist"}
        except:
            return HTTPException(400,"something went wrong")




@router.get(
    "/get_tests",
    description="get only one test on one specific time period for one or all of the cities",
)
def get_specific_test_data(
    city_name: Optional[str], test_num: int, year: int, month: int
):
    with session.begin() as con:
        final_dict: dict = {}
        cities_q = con.query(cities).all()
        test_q = con.query(tests).where(tests.test_num == test_num).first()
        tp_q = (
            con.query(time_priod)
            .where(and_(time_priod.year == year, time_priod.month == month))
            .first()
        )
        if not city_name:
            i = 0

            for city in cities_q:
                worder_stats = None
                worders: List[work_orders] = list(city.work_orders_r)
                if test_q and tp_q:
                    for wo in worders:
                        if (
                            wo.test_id == test_q.test_id
                            and wo.period_id == tp_q.period_id
                        ):
                            worder_stats = (
                                con.query(work_order_stats)
                                .where(
                                    work_order_stats.work_order_id == wo.work_order_id
                                )
                                .all()
                            )
                if worder_stats:
                    ndict: Dict[str, Union[int, str]] = {}
                    ndict["city_name"] = city.name
                    ndict["year"] = year
                    ndict["month"] = month
                    for worder in worder_stats:
                        parname = (
                            con.query(parameters)
                            .where(parameters.parameter_id == worder.parameter_id)
                            .first()
                        )
                        if parname:
                            ndict[parname.parameter_name] = worder.count
                    final_dict[i] = ndict
                    i += 1
            df = pd.DataFrame.from_dict(final_dict, orient="index")
            return df
        else:
            ct = con.query(cities).where(cities.name == city_name).first()
            if ct:
                i = 0
                worder_stats = None
                w1orders: List[work_orders] = list(ct.work_orders_r)
                if test_q and tp_q:
                    for wo in w1orders:
                        if (
                            wo.test_id == test_q.test_id
                            and wo.period_id == tp_q.period_id
                        ):
                            worder_stats = (
                                con.query(work_order_stats)
                                .where(
                                    work_order_stats.work_order_id == wo.work_order_id
                                )
                                .all()
                            )
                if worder_stats:
                    n1dict: Dict[str, Union[int, str]] = {}
                    n1dict["city_name"] = ct.name
                    n1dict["year"] = year
                    n1dict["month"] = month
                    for worder in worder_stats:
                        parname = (
                            con.query(parameters)
                            .where(parameters.parameter_id == worder.parameter_id)
                            .first()
                        )
                        if parname:
                            n1dict[parname.parameter_name] = worder.count
                    final_dict[i] = n1dict
                    i += 1
                df = pd.DataFrame.from_dict(final_dict, orient="index")
                return df.to_json(orient="table")

#برای انتقال یک یا چند پوشه از یک وضعیت به یک وضعیت دیگر
@router.post("/move_rec")
def move_rec(city_name : str, test_num : int, year : int , month : int , from_par : str , to_par : str , count : int):
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
        

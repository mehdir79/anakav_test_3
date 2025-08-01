from fastapi import APIRouter, Query
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


def get_cities_df():
    with session.begin() as con:
        cities_query = con.query(cities).all()
        citiesdict = {}
        i = 0
        for ct in cities_query:
            citiesdict[i] = {"city_name": ct.name, "code_omor": ct.code_omor}
            i += 1
        df = pd.DataFrame.from_dict(citiesdict, orient="index")
        return df


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
        return df


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
        return df


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
        return df


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
                return df


print(get_specific_test_data("شهر 2", 5, 1401, 7))

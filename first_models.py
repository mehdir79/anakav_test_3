from typing import Any, Optional, Dict
from sqlalchemy import (
    String,
    ForeignKey,
    event,
    select,
    func,
    create_engine,
    UniqueConstraint,
    JSON,
    null,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    sessionmaker,
    relationship,
)


class Base(DeclarativeBase):
    pass


engine = create_engine("sqlite:///database.db")
session = sessionmaker(bind=engine)


class cities(Base):
    __tablename__ = "cities"

    city_id: Mapped[int] = mapped_column(
        autoincrement=True, primary_key=True, unique=True
    )
    name: Mapped[str] = mapped_column(String(50), unique=True)
    code_omor: Mapped[int] = mapped_column(unique=True)

    def __init__(self, name, code_omor):
        self.name = name
        self.code_omor = code_omor

    work_orders_r = relationship("work_orders", back_populates="city_r")


class tests(Base):
    __tablename__ = "tests"

    test_id: Mapped[int] = mapped_column(
        autoincrement=True, primary_key=True, unique=True
    )
    test_name: Mapped[str] = mapped_column(String(50), unique=True)
    test_num: Mapped[int] = mapped_column(unique=True)
    majmo_name: Mapped[str] = mapped_column()

    def __init__(self, name, test_num, majmo_name):
        self.test_name = name
        self.test_num = test_num
        self.majmo_name = majmo_name

    test_parameter_r = relationship("test_parameter", back_populates="test_r")
    work_orders_r = relationship("work_orders", back_populates="test_r")


class parameters(Base):
    __tablename__ = "parameters"

    parameter_id: Mapped[int] = mapped_column(
        autoincrement=True, primary_key=True, unique=True
    )
    parameter_name: Mapped[str] = mapped_column(String(50), unique=True)

    def __init__(self, parameter_name):
        self.parameter_name = parameter_name

    test_parameter_r = relationship("test_parameter", back_populates="parameter_r")
    work_order_stats_r = relationship("work_order_stats", back_populates="parameter_r")


class test_parameter(Base):
    __tablename__ = "test_parameter"

    test_id: Mapped[int] = mapped_column(ForeignKey("tests.test_id"), primary_key=True)
    parameter_id: Mapped[int] = mapped_column(
        ForeignKey("parameters.parameter_id"), primary_key=True
    )

    def __init__(self, test_id: int, parameter_id: int):
        self.parameter_id = parameter_id
        self.test_id = test_id

    __table_args__ = (
        UniqueConstraint(
            "test_id", "parameter_id", name="uq_test_id_parameter_id_month"
        ),
    )

    test_r = relationship("tests", back_populates="test_parameter_r")
    parameter_r = relationship("parameters", back_populates="test_parameter_r")


class time_priod(Base):
    __tablename__ = "time_period"

    period_id: Mapped[int] = mapped_column(
        autoincrement=True, primary_key=True, unique=True
    )
    year: Mapped[int] = mapped_column()
    month: Mapped[int] = mapped_column()

    def __init__(self, year: int, month: int):
        self.year = year
        self.month = month

    __table_args__ = (UniqueConstraint("year", "month", name="uq_year_month"),)
    work_orders_r = relationship("work_orders", back_populates="period_r")


class work_orders(Base):
    __tablename__ = "work_orders"

    work_order_id: Mapped[int] = mapped_column(
        autoincrement=True, primary_key=True, unique=True
    )
    test_id: Mapped[int] = mapped_column(ForeignKey("tests.test_id"))
    city_id: Mapped[int] = mapped_column(ForeignKey("cities.city_id"))
    period_id: Mapped[int] = mapped_column(ForeignKey("time_period.period_id"))

    def __init__(self, test_id: int, city_id: int, period_id: int):
        self.test_id = test_id
        self.city_id = city_id
        self.period_id = period_id

    test_r = relationship("tests", back_populates="work_orders_r")
    city_r = relationship("cities", back_populates="work_orders_r")
    period_r = relationship("time_priod", back_populates="work_orders_r")
    work_order_stats_r = relationship("work_order_stats", back_populates="work_order_r")

    __table_args__ = (
        UniqueConstraint(
            "test_id", "city_id", "period_id", name="uq_city_id_test_id_period_id_month"
        ),
    )


class work_order_stats(Base):
    __tablename__ = "work_order_stats"

    work_order_id: Mapped[int] = mapped_column(
        ForeignKey("work_orders.work_order_id"), primary_key=True
    )
    parameter_id: Mapped[int] = mapped_column(
        ForeignKey("parameters.parameter_id"), primary_key=True
    )
    count: Mapped[int] = mapped_column(default=0, nullable=False)

    def __init__(self, work_order_id: int, parameter_id: int, count: int):
        self.work_order_id = work_order_id
        self.parameter_id = parameter_id
        self.count = count

    __table_args__ = (
        UniqueConstraint(
            "parameter_id", "work_order_id", name="uq_work_order_id_parameter_id_month"
        ),
    )

    work_order_r = relationship("work_orders", back_populates="work_order_stats_r")
    parameter_r = relationship("parameters", back_populates="work_order_stats_r")


Base.metadata.create_all(bind=engine)

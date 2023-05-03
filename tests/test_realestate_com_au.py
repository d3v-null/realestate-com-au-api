import os
import sys
import pytest
import json

from realestate_com_au import RealestateComAu
from realestate_com_au.objects.property import PropertyTrend, FullAddress
import pandas as pd
import tabulate


def test_constructor():
    api = RealestateComAu()
    assert api

def test_split_short_address_normal():
    assert FullAddress.split_short_address("2 The St") == ("2", "The Street", False)

def test_split_short_address_multi():
    assert FullAddress.split_short_address("2-4 The St") == ("2-4", "The Street", True)
    assert FullAddress.split_short_address("2 - 4 The St") == ("2-4", "The Street", True)
    assert FullAddress.split_short_address("2&4 The St") == ("2&4", "The Street", True)
    assert FullAddress.split_short_address("2 & 4 The St") == ("2&4", "The Street", True)

SUBPREM_PREFIXES = ["", "unit ", "u", "lot ", "proposed lot ", "p/l ", "lot"]

def test_split_short_address_sub_letter():
    assert FullAddress.split_short_address(f"2a The St") == ("2A", "The Street", False)
    # INV: assert FullAddress.split_short_address(f"2 a The St") == ("2A", "The Street", False)
    for sub in SUBPREM_PREFIXES:
        assert FullAddress.split_short_address(f"{sub}2a/62 The St") == ("2A/62", "The Street", False)
        assert FullAddress.split_short_address(f"{sub}2a, 62 The St") == ("2A/62", "The Street", False)
        assert FullAddress.split_short_address(f"{sub}1/21B The St") == ("1/21B", "The Street", False)
        assert FullAddress.split_short_address(f"{sub}1,55 Mars St") == ("1/55", "Mars Street", False)
        if sub == "": continue
        assert FullAddress.split_short_address(f"{sub}1 21 Burton St") == ("1/21", "Burton Street", False)

def test_split_short_address_sub_letter_multi():
    assert FullAddress.split_short_address("2&2a The St") == ("2&2A", "The Street", True)
    assert FullAddress.split_short_address("2 & 2a The St") == ("2&2A", "The Street", True)
    assert FullAddress.split_short_address("2 a&b The St") == ("2A&B", "The Street", True)
    assert FullAddress.split_short_address("2 a & b The St") == ("2A&B", "The Street", True)
    assert FullAddress.split_short_address("5a&5b The St") == ("5A&5B", "The Street", True)
    assert FullAddress.split_short_address("5a& 5b The St") == ("5A&5B", "The Street", True)

def test_split_short_address_sub_number():
    assert FullAddress.split_short_address(f"18/9 Petrea Place") == ("18/9", "Petrea Place", False)
    for sub in SUBPREM_PREFIXES:
        assert FullAddress.split_short_address(f"{sub}18/9 The St") == ("18/9", "The Street", False)
        assert FullAddress.split_short_address(f"{sub}18/9 Petrea Place") == ("18/9", "Petrea Place", False)
        assert FullAddress.split_short_address(f"{sub}19/ 2 The St") == ("19/2", "The Street", False)
        assert FullAddress.split_short_address(f"{sub}19 / 2 The St") == ("19/2", "The Street", False)

def test_split_short_address_sub_multi():
    for sub in SUBPREM_PREFIXES:
        assert FullAddress.split_short_address(f"{sub}19/2-4 The St") == ("19/2-4", "The Street", True)
        assert FullAddress.split_short_address(f"{sub}19/ 2-4 The St") == ("19/2-4", "The Street", True)
        assert FullAddress.split_short_address(f"{sub}19 / 2-4 The St") == ("19/2-4", "The Street", True)
        # assert FullAddress.split_short_address(f"{sub}19 / 2 - 4 The St") == ("19/2-4", "The Street", True)
        assert FullAddress.split_short_address(f"{sub}19/2&4 The St") == ("19/2&4", "The Street", True)
        assert FullAddress.split_short_address(f"{sub}19/ 2&4 The St") == ("19/2&4", "The Street", True)
        assert FullAddress.split_short_address(f"{sub}19 / 2&4 The St") == ("19/2&4", "The Street", True)
        assert FullAddress.split_short_address(f"{sub}19 / 2 & 4 The St") == ("19/2&4", "The Street", True)
        assert FullAddress.split_short_address(f"{sub}38-42/2 The Street") == ("38-42/2", "The Street", True)
        assert FullAddress.split_short_address(f"{sub}38-42 / 2 The Street") == ("38-42/2", "The Street", True)
        assert FullAddress.split_short_address(f"{sub}38 - 42 / 2 The Street") == ("38-42/2", "The Street", True)

def test_split_short_address_stupid_edge_cases():
    assert FullAddress.split_short_address("202&203&204 Melville Parade") == ("202&203&204", "Melville Parade", True)
    assert FullAddress.split_short_address("G01/2 The St") == ("G01/2", "The Street", False)
    assert FullAddress.split_short_address("A and B/149 Manning Road") == ("A&B/149", "Manning Road", True)
    assert FullAddress.split_short_address("SOLD1/77 Surrey Rd") == ("1/77", "Surrey Road", False)
    assert FullAddress.split_short_address("201 (Rear) Bishopsgate Street") == ("201 (REAR)", "Bishopsgate Street", False)
    assert FullAddress.split_short_address("FL 1 12/4-8 Queen Street") == ("1/12/4-8", "Queen Street", True)
    # assert FullAddress.split_short_address("5 11 15 Canterbury Terrace") == ("5 11 15", "Canterbury Terrace", True)
    # assert FullAddress.split_short_address("12A,B &C Farnham Street") == ("12A&B&C", "Farnham Street", True)
    # assert FullAddress.split_short_address("1707/Lot 100, 30 The Circus") == ("???", "The Circus", True)

def test_split_street_number():
    assert FullAddress.split_street_number("1") == (set(), {"1"})
    assert FullAddress.split_street_number("1A") == ({"A"}, {"1"})
    assert FullAddress.split_street_number("1&1A") == (set(), {"1", "1A"})
    # assert FullAddress.split_street_number("1A&B") == (None, {"1A", "1B"})
    assert FullAddress.split_street_number("1-3") == (set(), {"1", "3"})
    assert FullAddress.split_street_number("1A/2") == ({"1A"}, {"2"})
    assert FullAddress.split_street_number("1/2A") == ({"1"}, {"2A"})
    assert FullAddress.split_street_number("1A/2B") == ({"1A"}, {"2B"})
    assert FullAddress.split_street_number("1/2-4") == ({"1"}, {"2","4"})
    assert FullAddress.split_street_number("1/2&4") == ({"1"}, {"2","4"})
    assert FullAddress.split_street_number("1/2&4") == ({"1"}, {"2","4"})
    assert FullAddress.split_street_number("A&B/2&4") == ({"A", "B"}, {"2","4"})

def test_street_number_close():
    assert FullAddress.street_number_close("1", "1")
    assert FullAddress.street_number_close("1", "1-3")
    assert FullAddress.street_number_close("3", "1-3")
    assert not FullAddress.street_number_close("5", "1-3")
    assert FullAddress.street_number_close("1A", "1")
    assert FullAddress.street_number_close("1A", "1-3")

def test_property_trend_json():
    trend = PropertyTrend(id=None)
    print(trend.__dataclass_fields__.keys())
    trends = PropertyTrend.from_json("Bentley, WA 6102", {
        'annualGrowth': 0.06,
        'bedrooms': '4+',
        'medianRentalPrice': 480,
        'medianSoldPrice': 485000,
        'propertyType': 'unit',
        'rentDataIngestDateDisplay': '20 Jan 2023',
        'rentalProperties': 48,
        'soldDataIngestDateDisplay': '20 Jan 2023',
        'soldProperties': 13,
        'trends': {'medianRentalPrice': [{'bedrooms': '4+',
                                          'monthly': [{'count': 41,
                                                       'intervalEnd': '2022-07-31',
                                                       'intervalStart': '2021-08-01',
                                                       'value': 475},
                                                      {'count': 47,
                                                       'intervalEnd': '2022-02-28',
                                                       'intervalStart': '2021-03-01',
                                                       'value': 475}],
                                          'yearly': [{'count': 42,
                                                      'intervalEnd': '2021-12-31',
                                                      'intervalStart': '2021-01-01',
                                                      'value': 450},
                                                     {'count': 62,
                                                     'intervalEnd': '2016-12-31',
                                                      'intervalStart': '2016-01-01',
                                                      'value': 427}]}],
                    'medianSoldPrice': [{'bedrooms': 'ALL',
                                        'monthly': [{'count': 32,
                                                    'intervalEnd': '2017-07-31',
                                                     'intervalStart': '2016-08-01',
                                                     'value': 360000},
                                                    {'count': 79,
                                                    'intervalEnd': '2022-02-28',
                                                     'intervalStart': '2021-03-01',
                                                     'value': 368000}],
                                         'yearly': [{'count': 37,
                                                    'intervalEnd': '2016-12-31',
                                                     'intervalStart': '2016-01-01',
                                                     'value': 399000},
                                                    {'count': 52,
                                                    'intervalEnd': '2018-12-31',
                                                     'intervalStart': '2018-01-01',
                                                     'value': 335000}]}]}
    })
    columns = PropertyTrend.__dataclass_fields__.keys()
    df = pd.DataFrame( [trend.get_row_df(columns) for trend in trends], columns=columns )

    print(tabulate.tabulate(df, headers=df.columns)) # , tablefmt='psql')
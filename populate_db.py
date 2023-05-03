#!/usr/bin/env python

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, DataError
from sqlalchemy import create_engine
from argparse import ArgumentParser
from sys import stderr
from collections import Counter, defaultdict
from datetime import datetime, timedelta, date
import requests
import pandas as pd
from tabulate import tabulate

from realestate_com_au import RealestateComAu
from realestate_com_au.objects.listing import mapper_registry, Listing
from realestate_com_au.objects.property import Property, PropertyEvent, MediaItem, FullAddress


def eprint(*args, **kwargs):
    print(*args, file=stderr, **kwargs)


def get_parser():
    parser = ArgumentParser(
        description="populate db with data from realestate.com.au")
    parser.add_argument("--mode", type=str, default="listing",)
    parser.add_argument("--channel", type=str,
                        default="sold", help="buy, rent, sold")
    parser.add_argument("--surrounding_suburbs", default=False,
                        action="store_true", help="search surrounding suburbs")
    parser.add_argument("--locations", type=str, nargs="+",
                        help="locations to search")
    parser.add_argument("--property_types", type=str, nargs="+", default=[],
                        help="property types: house, unit apartment, townhouse, villa, land, acreage, retire, unitblock")
    parser.add_argument("--limit", type=int, default=100,
                        help="limit number of results")
    parser.add_argument("--sql_url", type=str,
                        help="url for sqlalchemy.create_engine")
    parser.add_argument("--debug", default=False,
                        action="store_true", help="debug sqlalchemy")
    parser.add_argument("--tabulate", default=False,
                        action="store_true", help="print a table of results")
    return parser



def populate_listing(engine, args):
    api = RealestateComAu()
    listings = api.search(
        limit=args.limit,
        property_types=args.property_types,
        locations=args.locations,
        channel=args.channel,
        surrounding_suburbs=args.surrounding_suburbs,
    )

    eprint(f"updating {len(listings)} listings")

    if args.tabulate:
        import pandas as pd
        import numpy as np
        from tabulate import tabulate
        data = [
            [
                l.id, l.badge, l.url, l.property.suburb, l.property.state, l.property.postcode, l.property.short_address, l.property.property_type,
                # bool(re.search(r"ensuite", l.description, re.IGNORECASE)),
                l.property.bedrooms, l.property.bathrooms, l.property.parking_spaces,
                # aircon_type(l.description), internet_type(l.description), property_type(l.property_type, l.bedrooms),
                # "", "", "", "",
                l.property.land_size, l.property.building_size,
                # display_area(l.land_size, l.land_size_unit), display_area(l.building_size, l.building_size_unit),
                # get_year(l.full_address) or '',
                l.price, l.sold_date
            ]
            for l in listings if l.address
        ]

        columns = [
            "ID", "badge", "url", "suburb", "state", "postcode", "short_address", "property_type",
            "bedrooms", "bathrooms", "parking_spaces",
            "land_size",  "building_size",
            # "year built",
            "price", "sold_date"]

        df = pd.DataFrame(data, columns=columns)
        # valid_mask = np.logical_and(df.land_size, df.price, df.sold_date > np.datetime64(0, 'D'))
        print(tabulate(df, headers=columns))

    # group listings by full_address
    by_address = defaultdict(list)
    for l in listings:
        if l.property and l.property.address:
            by_address[l.property.address].append(l)

    eprint(f"-> {len(by_address)} unique properties")

    def date_for_sort(l):
        if l.property.timeline:
            return l.property.timeline[0].date
        return datetime(1, 1, 1, 0, 0, 0).date()

    with Session(engine) as session:
        # print(session.dirty)

        for address, ls in by_address.items():
            eprint(f"{address} has {len(ls)} listings")
            if len(ls) > 1:
                ls = sorted(ls, key=date_for_sort)
                timeline = []
                # merge events into latest property
                for l in ls:
                    timeline.extend(l.property.timeline)
                ls[-1].property.timeline = timeline
            l_newest = ls[-1]

            p = session.query(Property).filter(
                Property.address == address
            ).one_or_none()

            if p:
                p.merge(l_newest.property)
                print(f"MERGING MATCHED PROPERTY!!!\n{p}\n")
            else:
                p = l_newest.property
                print(f"MERGING NEW PROPERTY!!!\n{p}\n")
            try:
                p = session.merge(p)
            except IntegrityError as exc:
                eprint(f"ERROR: {exc}")
                continue
            try:
                session.commit()
            except DataError as exc:
                eprint(f"ERROR committing {p} with {ls}: {exc}")
                raise exc
            session.refresh(p)
            for l in ls:
                l.property = p
                l.property_id = p.id
                session.merge(l)
            session.commit()

def populate_media(engine, args):
    with Session(engine) as session:
        for m in session.query(MediaItem).filter(
            MediaItem.date.is_(None)
        ).all():
            date = requests.head(m.url).headers.get('Last-Modified')
            # parse a date like Fri, 23 Apr 2021 16:10:29 GMT
            date = datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %Z")
            print(date)
            exit(0)

def address_fix(engine, args):
    with Session(engine) as session:
        fixes = []
        for p in session.query(Property).all():
            tokens = [*filter(None, [p.street_number, p.street_name])]
            street_number, street_name, _ = FullAddress.split_short_address(" ".join(tokens))
            if street_name != p.street_name or street_name != p.street_name:
                fixes.append([
                    p.id, p.actual_id,
                    repr(p.street_number), repr(p.street_name),
                    repr(street_number), repr(street_name)
                ])
                if p.actual_id < 0:
                    p.actual_id = None
                p.street_number = street_number
                p.street_name = street_name
                session.merge(p)
        session.commit()
        df = pd.DataFrame(fixes, columns=["id", "actual_id", "street_number", "street_name", "new_street_number", "new_street_name"])
        print(tabulate(df.sort_values(by="new_street_name"), headers=df.columns))

def merge_props(session, from_props, into_prop):
    for from_prop in from_props:
        print(f"from id {from_prop.id} | events: {from_prop.timeline}")
        if len(from_prop.timeline) > 0:
            print("bbb")
            exit(1)
        into_prop.merge(from_prop)
        print(f"merged events: {into_prop.timeline}")

        for listing in session.query(Listing).filter(Listing.property_id == from_prop.id).all():
            listing.property_id = into_prop.id
            session.merge(listing)
        session.delete(from_prop)

def address_duplicates(engine, args):
    with Session(engine) as session:
        display_columns = ["id", "actual_id", "url", "_street_number"]
        grouped_columns = ["_suburb", "_street_name", "_postcode"]
        columns = display_columns + grouped_columns
        df = pd.DataFrame(
            [prop.get_row_df(columns) for prop in session.query(Property).all()],
            columns=columns
        )
        for group, df in df.groupby(grouped_columns):
            if len(df) < 2:
                continue
            actual_ids = set([x for x in df.actual_id if x is not None and x > 0])
            if len(actual_ids) != 1:
                print(f"there is not exactly 1 actual id for {group}")
                print(tabulate(df[display_columns], headers=display_columns))
                continue
            actual_id = actual_ids.pop()
            with_actual_id = df[df.actual_id == actual_id]
            if len(with_actual_id) != 1:
                print(f"there is more than 1 property with actual id {actual_id}")
                print(tabulate(with_actual_id[display_columns], headers=display_columns))
                continue
            into_id =  [*with_actual_id["id"]][0]
            print(f"into id {into_id}")
            into_prop = session.get(Property, into_id)
            into_timeline = into_prop.timeline
            if len(into_timeline) > 0:
                print("aaa")
                exit(1)
            print(f"into events {into_timeline}")
            from_props = [
                session.get(Property, from_id)
                for from_id in df[df.actual_id != actual_id]["id"]
            ]
            # merge_props(session, from_props, into_prop)

            # session.merge(into_prop)

        # session.commit()

def main():
    parser = get_parser()
    args = parser.parse_args()

    Base = mapper_registry.generate_base()
    engine = create_engine(
        args.sql_url,
        echo=args.debug,
        future=True
    )
    Base.metadata.create_all(engine)

    if args.mode == "listing":
        populate_listing(engine, args)
    elif args.mode == "media":
        populate_media(engine, args)
    elif args.mode == "address_fix":
        address_fix(engine, args)
    elif args.mode == "address_duplicates":
        address_duplicates(engine, args)

if __name__ == "__main__":
    main()

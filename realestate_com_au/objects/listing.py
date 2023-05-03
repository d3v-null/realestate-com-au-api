from dataclasses import dataclass, field
from typing import List, Optional
import re
from realestate_com_au.utils import delete_nulls, strp_date_or_none, positive_float_or_none, positive_int_or_none
from realestate_com_au.objects.property import FullAddress, Property, PropertyEvent, mapper_registry, MediaItem

from sqlalchemy import Column, Text, Integer, ForeignKey, Table, VARCHAR
from sqlalchemy.dialects.mysql import JSON, TINYTEXT, MEDIUMINT
from sqlalchemy.orm import relationship, composite


@mapper_registry.mapped
@dataclass
class Listing:
    __table__ = Table(
        "realestate_listing",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("suburb", TINYTEXT),
        Column("state", VARCHAR(3)),
        Column("postcode", MEDIUMINT(unsigned=True)),
        Column("street_number", TINYTEXT),
        Column("street_name", TINYTEXT),
        Column("property_id", Integer, ForeignKey("realestate_property.id")),
        Column("badge", TINYTEXT),
        Column("url", TINYTEXT),
        Column("price", Integer),
        Column("price_text", TINYTEXT),
        Column("description", Text),
    )

    id: int
    # Captures Promotional text not held elsewhere, such as 'Under Contract'
    badge: Optional[str] = None
    url: Optional[str] = None
    price: Optional[int] = None
    address: Optional[FullAddress] = None
    # Captures the original text, such as a price range or comment. This is lost when converting to Integer
    price_text: Optional[str] = None
    description: Optional[str] = None
    property_id: Optional[int] = None
    property: Optional[Property] = None

    __mapper_args__ = {  # type: ignore
        "properties": {
            "property": relationship("Property", back_populates="listings"),
            "address": composite(
                FullAddress,
                __table__.c.street_number,
                __table__.c.street_name,
                __table__.c.suburb,
                __table__.c.state,
                __table__.c.postcode
            )
        }
    }


@ dataclass
class Lister:
    id: str
    name: str
    agent_id: str
    job_title: str
    url: str
    phone: str
    email: str


def parse_price_text(price_display_text):
    regex = r".*\$([0-9\,\.]+(?:k|K|m|M)*).*"
    price_groups = re.search(regex, price_display_text)
    price_text = (
        price_groups.groups()[
            0] if price_groups and price_groups.groups() else None
    )
    if price_text is None:
        return None

    price = None
    if price_text[-1] == "k" or price_text[-1] == "K":
        price = float(price_text[: -1].replace(",", ""))

        price *= 1000
    elif price_text[-1] == "m" or price_text[-1] == "M":
        price = float(price_text[: -1].replace(",", ""))
        price *= 1000000
    else:
        price = float(price_text.replace(",", "").split('.')[0])

    return int(price)


def parse_phone(phone):
    if not phone:
        return None
    return phone.replace(" ", "")


def parse_description(description):
    if not description:
        return None
    return description.replace("<br/>", "\n")
    # return description


def get_lister(lister):
    lister = delete_nulls(lister)
    lister_id = lister.get("id")
    name = lister.get("name")
    agent_id = lister.get("agentId")
    job_title = lister.get("jobTitle")
    url = lister.get("_links", {}).get("canonical", {}).get("href")
    phone = parse_phone(lister.get("preferredPhoneNumber"))
    email = lister.get("email")  # TODO untested, need to confirm
    return Lister(
        id=lister_id,
        name=name,
        agent_id=agent_id,
        job_title=job_title,
        url=url,
        phone=phone,
        email=email,
    )


def get_listing(listing):
    listing = delete_nulls(listing)
    # delete null keys for convenience

    listing_id = listing.get("id")
    badge = listing.get("badge", {}).get("label")
    url = listing.get("_links", {}).get("canonical", {}).get("href")
    address = listing.get("address", {})
    suburb = address.get("suburb")
    state = address.get("state")
    postcode = address.get("postcode")
    short_address = address.get("display", {}).get("shortAddress")
    # full_address = address.get("display", {}).get("fullAddress")
    property_type = listing.get("propertyType", {}).get("id")
    listing_company = listing.get("listingCompany", {})
    listing_company_id = listing_company.get("id")
    listing_company_name = listing_company.get("name")
    listing_company_phone = parse_phone(listing_company.get("businessPhone"))
    features = listing.get("generalFeatures", {})
    bedrooms = features.get("bedrooms", {}).get("value")
    bathrooms = features.get("bathrooms", {}).get("value")
    parking_spaces = features.get("parkingSpaces", {}).get("value")
    studies = features.get("studies", {}).get("value")
    property_sizes = listing.get("propertySizes", {})
    building_size = property_sizes.get("building", {}).get("displayValue")
    building_size_unit = property_sizes.get(
        "building", {}).get("sizeUnit", {}).get("displayValue")
    land_size = float(''.join(property_sizes.get(
        "land", {}).get("displayValue", '-1').split(',')))
    land_size_unit = property_sizes.get("land", {}).get(
        "sizeUnit", {}).get("displayValue")
    price_text = listing.get("price", {}).get("display", "")
    price = positive_int_or_none((parse_price_text(price_text)))
    if price and price > 1_000_000_000:
        price = None
    price_text = listing.get("price", {}).get("display")
    sold_date = strp_date_or_none(listing.get(
        "dateSold", {}).get("display"), '%d %b %Y')
    auction = listing.get("auction", {}) or {}
    # iso8601 python date format with +00:00 timezon
    fmt = "%Y-%m-%dT%H:%M:%S%z"
    auction_date = strp_date_or_none(auction.get(
        "dateTime", {}).get("value"), ['%d %b %Y', "%Y-%m-%dT%H:%M:%S%z"])
    description = parse_description(listing.get("description"))
    images = [MediaItem.from_templated(media.get('templatedUrl', {}), "photo")
              for media in listing.get("media", []).get('images', [])]
    images_floorplans = [MediaItem.from_templated(media.get('templatedUrl', {}), "floorplan")
                         for media in listing.get("media", []).get('floorplans', [])]
    listers = [get_lister(lister) for lister in listing.get("listers", [])]

    timeline = []
    if sold_date:
        timeline.append(PropertyEvent(
            id=None,
            property_id=None,
            event_type="sold_listing",
            date=sold_date,
            price=price,
        ))

    property = None
    print(address)
    address = None
    if short_address:
        street_num, street_name, multi = FullAddress.split_short_address(
            short_address)
        address = FullAddress(street_num, street_name, suburb, state, postcode)
        print(f"{short_address} => {address.street_number!r} {address.street_name!r}")
        if not multi:
            property = Property(
                # suburb=suburb,
                # state=state,
                # postcode=positive_int_or_none(postcode),
                # short_address=short_address,
                address=address,
                property_type=property_type,
                bedrooms=positive_int_or_none(bedrooms),
                bathrooms=positive_int_or_none(bathrooms),
                studies=positive_int_or_none(studies),
                parking_spaces=positive_int_or_none(parking_spaces),
                building_size=positive_float_or_none(building_size),
                # building_size_unit=building_size_unit,
                land_size=positive_float_or_none(land_size),
                # land_size_unit=land_size_unit,
                timeline=timeline,
                # media=[*images, *images_floorplans]
            )

    listing = Listing(
        id=listing_id,
        badge=badge,
        url=url,
        # listing_company_id=listing_company_id,
        # listing_company_name=listing_company_name,
        # listing_company_phone=listing_company_phone,
        price=price,
        price_text=price_text,
        # auction_date=auction_date,
        # sold_date=sold_date,
        property=property,
        description=description,
        address=address,
        # images=images,
        # images_floorplans=images_floorplans,
        # listers=listers,
    )

    return listing

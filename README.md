# realestate-com-au-api

🏠 Python wrapper for the realestate.com.au API

## Installation

Using **Python >= 3.6**:

```bash
pip install -e git+https://github.com/tomquirk/realestate-com-au-api.git#egg=realestate_com_au_api
```

### Example usage

```python
from realestate_com_au import RealestateComAu

api = RealestateComAu()

# Get property listings
listings = api.search(locations=["seventeen seventy, qld 4677"], channel="buy", keywords=["tenant"], exclude_keywords=["pool"])
```

## Data classes

#### [Listing](/realestate_com_au/objects/listing.py#L6)

Data class for a listing. See [listing.py](/realestate_com_au/objects/listing.py#L6) for reference.

## Legal

This code is in no way affiliated with, authorized, maintained, sponsored or endorsed by REA Group or any of its affiliates or subsidiaries. This is an independent and unofficial API. Use at your own risk.

## Dev

- set SQL_URL in .env
- generate RootCA
- start mitmproxy
- start wscapture.py
- autogui.sh or populate_db.sh

```python
from tabulate import tabulate

listings = api.search(locations=["Victoria Park, 3054"], channel="buy")

fieldnames = [
  'id',
  # 'auction_date',
  # 'badge',
  'bathrooms',
  'bedrooms',
  'building_size',
  'building_size_unit',
  # 'description',
  'full_address',
  # 'images',
  # 'images_floorplans',
  'land_size',
  'land_size_unit',
  # 'listers',
  # 'listing_company_id',
  # 'listing_company_name',
  # 'listing_company_phone',
  'parking_spaces',
  'postcode',
  'price',
  'price_text',
  'property_type',
  'short_address',
  'sold_date',
  'state',
  'suburb',
  'url'
]

table = [[l.__dict__[field] for field in fieldnames] for l in listings]

with open('listings.csv', 'w') as csvfile:
  writer = csv.writer(csvfile, fieldnames)
  writer.writerow(fieldnames)
  writer.writerows(table)

```

# get populate-db woring

```bash
xauth add $DISPLAY $(xauth list $DISPLAY | cut -d: -f2- | tail -1)
```

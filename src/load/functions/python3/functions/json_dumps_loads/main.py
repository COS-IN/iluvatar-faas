import json
from urllib.request import urlopen
from urllib.error import HTTPError
from time import time
import random
import traceback

# https://github.com/jdorfman/awesome-json-datasets
urls = ["https://www.govtrack.us/api/v2/role?current=true&role_type=senator",
"https://www.govtrack.us/api/v2/role?current=true&role_type=representative&limit=438",
"https://api.github.com/emojis",
"https://www.justice.gov/api/v1/blog_entries.json?amp%3Bpagesize=2",
"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
"https://www.justice.gov/api/v1/press_releases.json?pagesize=2",
"https://www.justice.gov/api/v1/speeches.json?pagesize=2",
"https://www.justice.gov/api/v1/vacancy_announcements.json?pagesize=2",
"http://vocab.nic.in/rest.php/states/json",
"https://data.gov.au/geoserver/abc-local-stations/wfs?request=GetFeature&typeName=ckan_d534c0e9_a9bf_487b_ac8f_b7877a09d162&outputFormat=json",
"https://data.nasa.gov/resource/2vr3-k9wn.json",
"https://data.nasa.gov/resource/y77d-th95.json",
"https://api.nobelprize.org/v1/prize.json",
"http://api.worldbank.org/v2/countries/USA/indicators/SP.POP.TOTL?per_page=5000&format=json",
"https://www.reddit.com/r/catholicmemes.json",
"https://pokeapi.co/api/v2/pokemon?limit=100",
"https://blockchain.info/latestblock",
"https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series/globe/land_ocean/ytd/12/1880-2016.json",
"https://data.cityofchicago.org/api/views/ijzp-q8t2",
"https://api.exchangerate-api.com/v4/latest/USD",
"https://mtgjson.com/api/v5/DMR.json",
"http://country.io/continent.json",
"https://api.hearthstonejson.com/v1/121569/all/cards.collectible.json",
"https://catfact.ninja/facts",
"https://catfact.ninja/breeds",
"https://api.urbandictionary.com/v0/define?term=pope",
"https://en.wikipedia.org/w/api.php?action=opensearch&search=venerable&limit=50&format=json",
]

cold = True

def main(args):
  random.seed()
  # return {"body":"testing\n\n"}
  error = []
  glob_start = time()
  global cold
  global urls
  was_cold = cold
  cold = False
  for i in range(10):
    try:
      link = random.choice(urls)

      try:
        f = urlopen(link)
      except HTTPError as e:
        # urls.remove(link)
        continue

      data = f.read().decode("utf-8")
      network = time() - glob_start

      start = time()
      json_data = json.loads(data)
      str_json = json.dumps(json_data, indent=4)

    except Exception as e:
      error.append(e)
      continue

    end = time()
    latency = end - start
    return {"body": {"network": network, "serialization": latency, "latency": end-glob_start, "cold":was_cold, "start":glob_start, "end":end, "url":link}}

  return {"body": { "cust_error":error, "cold":was_cold, "link":link }}
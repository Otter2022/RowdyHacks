from requests import get
from pprint import pprint

RideURL = get("https://queue-times.com/parks.json").json()
RideQueueURL = get("https://queue-times.com/parks/2/queue_times.json").json()

pprint(RideURL[1])

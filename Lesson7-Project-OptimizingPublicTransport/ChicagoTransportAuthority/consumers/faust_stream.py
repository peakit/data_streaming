"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record, validation=True):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record, validation=True):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream-v3", broker="kafka://localhost:9092", store="memory://")

# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
in_topic = app.topic("org.chicago.cta.v3.stations", value_type=Station)

# TODO: Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.station.table.v3",
                      partitions=1,
                      key_type=str,
                      value_type=TransformedStation
                     )

# TODO: Define a Faust Table
station_table = app.Table(
    "org.chicago.cta.station.table.v3",
    default=int,
    partitions=1,
    changelog_topic=out_topic
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(in_topic)
async def stations(stations):
    # stations.add_processor(transform_stations)
    
    async for st in stations:
        line_color = None
        if st.red:
            line_color = "red"
        elif st.green:
            line_color = "green"
        elif st.blue:
            line_color = "blue"

        tranStation = TransformedStation(
            station_id=st.station_id,
            station_name=st.station_name,
            order=st.order,
            line=line_color
        )
        station_table[st.station_id] = tranStation
        # await out_topic.send(key=str(trStation.station_id), value=trStation)

if __name__ == "__main__":
    app.main()

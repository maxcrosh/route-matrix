import pandas as pd
import geopandas as gpd
import flexpolyline as fp
from shapely.geometry import LineString
import aiohttp
import asyncio
import time

SERVICE_URL = "https://router.hereapi.com/v8/routes"
API_KEY = "YOUR_API_KEY"

data = pd.read_csv('FILE_WITH_DATA.csv')

error_log_file = f'error-log-{round(time.time())}.txt'

def generate_route(origin, destination, origin_lat, origin_lng, destination_lat, destination_lng, polyline, spans):
    for index, span in enumerate(spans):
        currentSpan = span['offset']
        try:   
            nextSpan = spans[index + 1]['offset'] + 1
        except IndexError:
            nextSpan = len(polyline)
        
        geometry = LineString(tuple(polyline[currentSpan:nextSpan]))
        # print(geometry)
        yield {
            'origin': origin,
            'destination': destination,
            'origin_lat': origin_lat,
            'origin_lng': origin_lng,
            'destination_lat': destination_lat,
            'destination_lng': destination_lng,
            'countryCode': span['countryCode'],
            'segmentRef': span['segmentRef'],
            'topologySegmentId': span['topologySegmentId'],
            'functionalClass': span['functionalClass'],
            # 'truckAttributes': span['truckAttributes'],
            # 'speedLimit': span['speedLimit'] if span['speedLimit'] else None,
            'geometry': geometry,
        }

async def calculate_truck_route(session, origin, destination, origin_lat, origin_lng, destination_lat, destination_lng):
    '''
    Function for route calculation.
    '''
    parameters = {
        "apikey": API_KEY,
        "origin": f"{origin_lat},{origin_lng}",
        "destination": f"{destination_lat},{destination_lng}",
        "transportMode": "truck",
        "return": "polyline",
        'spans': 'countryCode,segmentRef,segmentId,functionalClass,speedLimit'
    }
    
    async with session.get(SERVICE_URL, params=parameters) as res:
        response = await res.json()

        try:
            polyline = response['routes'][0]['sections'][0]['polyline']
            spans = response['routes'][0]['sections'][0]['spans']
            reshapedGeometry = [tuple(reversed(coords)) for coords in fp.decode(polyline)]
            
            route_gdf = gpd.GeoDataFrame(list(generate_route(origin, destination, origin_lat, origin_lng, destination_lat, destination_lng, reshapedGeometry, spans)))
            
            print(f"Success route: {origin}->{destination}")

            return route_gdf
        except IndexError:
            print(f"Error route: {origin}->{destination}")
            with open(error_log_file, 'a') as file:
                file.write(f"{origin},{destination},{origin_lat},{origin_lng},{destination_lat},{destination_lng}\n")
           


async def main():
    tasks = list()
    
    async with aiohttp.ClientSession() as session:
        for index, item in data.iterrows():
            tasks.append(calculate_truck_route(session, item['origin'], item['destination'], item['origin_lat'], item['origin_long'], item['destination_lat'], item['destination_long']))
            
        result = await asyncio.gather(*tasks)
        # print()
        
        # gdf = gpd.GeoDataFrame([item for item in result if item != None])
        gdf = gpd.GeoDataFrame(pd.concat(result))
        gdf.to_file("matrix.shp")
        # gdf.to_file("matrix.geojson", driver="GeoJSON")        


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

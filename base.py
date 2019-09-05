import pandas as pd
import os
import binascii
import struct
import fiona
import geopandas as gpd
import shapely
from descartes import PolygonPatch
from matplotlib.collections import PatchCollection
from pandas._libs.tslibs import resolution
from shapely.geometry import Point
from shapely.ops import transform
from pandas._libs.join import left_join_indexer
from pandas._libs.reshape import explode
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np

class Base:
    def __init__(self, root_folder='data'):
        self.root_folder = root_folder
        self.format = ""
        self.poc_name = ""

    def get_devices(self):
        pass

    def get_poc_data(self):
        return [f.name for f in os.scandir(self.root_folder) if f.is_dir()]

    def parse_format(self):
        pass


def transform(data):
    lat = data[0:8]
    lat = struct.unpack('!f', bytes.fromhex(lat))[0]
    lng = data[8:16]
    lng = struct.unpack('!f', bytes.fromhex(lng))[0]
    otro = data[16:24]

    otro_bin = bin(int(otro, 16))[2:].zfill(32)
    periodico = otro_bin[0:1]
    tipo_mensaje = int(otro_bin[1:6], 2)
    digitalInput = otro_bin[6]
    data1 = otro_bin[7:20]
    bateria = (4200 / 4095) * (int(data1, 2) - 4095) + 4200
    return [lat,
            lng,
            periodico,
            tipo_mensaje,
            digitalInput,
            bateria]


# Enable fiona driver
gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'

print(os.path.join('data', 'agl'))

root_path = os.path.join('data', 'agl')

data = {}
geofence = []

from zipfile import ZipFile
geofence_path = os.path.join(root_path, 'geofence')
geofence_temp_path = os.path.join(geofence_path, "temp")
for file in os.listdir(geofence_path):
    try:
        kmz = ZipFile(os.path.join(geofence_path, file), 'r')
        file_new = ".".join([file.split(".")[0], "kml"])
        kmz.extract("doc.kml", geofence_temp_path)
        # os.rename(os.path.join(geofence_temp_path, "doc.kml"), file_new)
        df = gpd.read_file(os.path.join(geofence_temp_path, "doc.kml"), driver='KML')
        df.geometry = df.geometry.map(lambda polygon: shapely.ops.transform(lambda x, y, z: (x, y), polygon))
        geofence.append(df)
    except Exception as e:
        print(e)
        continue

for file in os.listdir(os.path.join('data', 'agl')):
    if os.path.isdir(os.path.join(root_path, file)):
        continue
    data[file.split('-')[2]] = pd.read_csv(os.path.join(root_path, file), delimiter=';')

temp = {}
for device in data:
    rows = []
    temp[device] = data[device][['Timestamp', 'Data']]
    temp[device].insert(2, 'response', temp[device]['Data'].apply(transform))
    temp[device] = temp[device]['response']\
        .apply(pd.Series)\
        .merge(temp[device], left_index=True, right_index=True)\
        .drop(["response", "Data"], axis=1)
    temp[device].columns = ["lat", "lng", "periodico", "tipo_mensaje", "digitalInput", "bateria", "Timestamp"]
    temp[device]['Timestamp'] = pd.to_datetime(temp[device]['Timestamp'])
    temp[device] = temp[device].set_index('Timestamp')
    temp[device] = temp[device][(temp[device].lat != 0.0) & (temp[device].lng != 0.0)]
    # _ = data[device].apply(lambda row: [rows.append([row['Timestamp'], row['Data'], nn]) for nn in row.])
    # temp[device] = pd.DataFrame(temp[device]['Timestamp'], temp[device]['Data'].apply(transform))

device_df = temp

gdf_devices = {}
for device in device_df:
    geometry = [Point(xy) for xy in zip(device_df[device].lng, device_df[device].lat)]
    crs = {'init': 'epsg:4326'}
    gdf_devices[device] = gpd.GeoDataFrame(device_df[device],
                                           geometry=geometry,
                                           crs=crs)
    gdf_devices[device].plot(figsize=(10, 3))
    print(gdf_devices[device].head())

plt.show()

water = 'lightskyblue'
earth = 'cornsilk'

fig, ax1 = plt.subplots(figsize=(12, 10))
# mm = Basemap(width=600000,
#              height=400000,
#              resolution='i',
#              projection='aea',
#              ellps='WGS84',
#              lat_1=-1.2, lat_2=-1.4,
#              lat_0=-1.4, lon_0=-80)
mm = Basemap(width=300000,
             height=200000,
             resolution='i',
             projection='aea',
             ellps='WGS84',
             lat_1=-1.2, lat_2=-1.4,
             lat_0=-1.4, lon_0=-80)
coast = mm.drawcoastlines()
rivers = mm.drawrivers(color=water, linewidth=1.5)
continents = mm.fillcontinents(
    color=earth,
    lake_color=water)
bound= mm.drawmapboundary(fill_color=water)
countries = mm.drawcountries()
merid = mm.drawmeridians(
    np.arange(-180, 180, 2),
    labels=[False, False, False, True])
parall = mm.drawparallels(
    np.arange(0, 80),
    labels=[True, True, False, False])

patches = []
for fence in geofence:
    for poly in fence.geometry:
        if poly.geom_type == 'Polygon':
            mpoly = transform(mm, poly)
            patches.append(PolygonPatch(mpoly))
        elif poly.geom_type == 'MultiPolygon':
            for subpoly in poly:
                mpoly = transform(mm, subpoly)
                patches.append(PolygonPatch(mpoly))

zone = ax1.add_collection(PatchCollection(patches, match_original=True))

plt.show()

while True:
    pass
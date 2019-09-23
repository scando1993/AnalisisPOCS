import pandas as pd
import os, json
import struct
import fiona
import geopandas as gpd
import shapely
from descartes import PolygonPatch
from geopandas import GeoDataFrame
from matplotlib.collections import PatchCollection
from pandas._libs.tslibs import resolution
from shapely.geometry import Point, Polygon, box, MultiPolygon, GeometryCollection
from shapely.ops import transform
from pandas._libs.join import left_join_indexer
from pandas._libs.reshape import explode
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np
from openpyxl.workbook import Workbook
from tqdm import tqdm

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

#
# def distance_on_geoid(lat1, lon1, lat2, lon2):
#     lat1 = lat1 *
def fishnet(geometry, threshold):
    bounds = geometry.bounds
    xmin = int(bounds[0] // threshold)
    xmax = int(bounds[2] // threshold)
    ymin = int(bounds[1] // threshold)
    ymax = int(bounds[3] // threshold)
    ncols = int(xmax - xmin + 1)
    nrows = int(ymax - ymin + 1)
    cols = list(range(xmin, xmax + 1, 1))
    rows = list(range(ymin, ymax + 1, 1))
    result = []
    # for i in range(xmin, xmax+1):
    #     for j in range(ymin, ymax+1):

    for i in tqdm(cols):
        for j in rows:
            b = box(i * threshold, j * threshold, (i + 1) * threshold, (j + 1) * threshold)
            g = geometry.intersection(b)
            if g.is_empty:
                continue
            result.append(g)
    return result


def katana(geometry, threshold, count=0):
    """Split a Polygon into two parts across it's shortest dimension"""
    bounds = geometry.bounds
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    if max(width, height) <= threshold or count == 250:
        # either the polygon is smaller than the threshold, or the maximum
        # number of recursions has been reached
        return [geometry]
    if height >= width:
        # split left to right
        a = box(bounds[0], bounds[1], bounds[2], bounds[1] + height / 2)

        b = box(bounds[0], bounds[1] + height / 2, bounds[2], bounds[3])
    else:
        # split top to bottom
        a = box(bounds[0], bounds[1], bounds[0] + width / 2, bounds[3])
        b = box(bounds[0] + width / 2, bounds[1], bounds[2], bounds[3])
    result = []
    for d in (a, b,):
        c = geometry.intersection(d)
        if not isinstance(c, GeometryCollection):
            c = [c]
        for e in c:
            if isinstance(e, Polygon) or isinstance(e, MultiPolygon):
                result.extend(katana(e, threshold, count+1))
    if count > 0:
        return result
    # convert multipart into singlepart
    final_result = []
    for g in result:
        if isinstance(g, MultiPolygon):
            final_result.extend(g)
        else:
            final_result.append(g)
    return final_result


def transform_sigfox(data):
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
geofence_dataset = {}

from zipfile import ZipFile
geofence_path = os.path.join(root_path, 'geofence')
geofence_temp_path = os.path.join(geofence_path, "temp")
for file in os.listdir(geofence_path):
    try:
        kmz = ZipFile(os.path.join(geofence_path, file), 'r')
        file_new = ".".join([file.split(".")[0], "kml"])
        kmz.extract("doc.kml", geofence_temp_path)
        df = gpd.read_file(os.path.join(geofence_temp_path, "doc.kml"), driver='KML')
        df.geometry = df.geometry.map(lambda polygon: shapely.ops.transform(lambda x, y, z: (x, y), polygon))
        if file_new in ["TECA FRANCHESCO TABACCHI.kml"]:
            g = [i for i in df.geometry]
            all_coords = list(shapely.geometry.mapping(g[0])["coordinates"][0])
            polys = Polygon(all_coords)
            # _temp = fishnet(polys, 0.0010)
            _temp = fishnet(polys, 0.0005)
            i = 1
            for fence in _temp:
                df = GeoDataFrame({'name': file.split(".")[0] + '_' + str(i),
                                   'geometry': [fence]})
                i += 1
                geofence.append({'name': file.split(".")[0] + "_" + str(i),
                                   'df': df})

        else:
            geofence.append({'name': file.split(".")[0], 'df': df})
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
    temp[device].insert(2, 'response', temp[device]['Data'].apply(transform_sigfox))
    temp[device] = temp[device]['response']\
        .apply(pd.Series)\
        .merge(temp[device], left_index=True, right_index=True)\
        .drop(["response", "Data"], axis=1)
    temp[device].columns = ["lat", "lng", "periodico", "tipo_mensaje", "digitalInput", "bateria", "Timestamp"]
    temp[device]['Timestamp'] = pd.to_datetime(temp[device]['Timestamp'])
    temp[device] = temp[device].set_index('Timestamp')
    temp[device] = temp[device][(temp[device].lat != 0.0) & (temp[device].lng != 0.0)]
    temp[device] = temp[device][(temp[device].lat < -1.25) & (temp[device].lng < -79.0)]
    temp[device] = temp[device][(temp[device].lat > -1.40) & (temp[device].lng > -82.0)]

device_df = temp

gdf_devices = {}
for device in device_df:
    geometry = [Point(xy) for xy in zip(device_df[device].lng, device_df[device].lat)]
    crs = {'init': 'epsg:4326'}
    gdf_devices[device] = gpd.GeoDataFrame(geometry=geometry,
                                           crs=crs)

polygons = {}
patches = []
for fence in geofence:
    for poly in fence['df'].geometry:
        if poly.geom_type == 'Polygon':
            polygons[fence['name']] = poly
        elif poly.geom_type == 'MultiPolygon':
            for subpoly in poly:
                # polygons.append(subpoly)
                polygons[fence['name']] = subpoly

zones = gpd.GeoSeries(polygons)
zones_gdf = gpd.GeoDataFrame({'name': [key for key, _ in polygons.items()],
                              'geometry': [geom for _, geom in polygons.items()]},
                             geometry=[geom for _, geom in polygons.items()])

zones_gdf.to_excel('test.xlsx')
print([key for key in zones_gdf.keys()])
print(" ")
print(zones.head())
print(" ")
print(zones_gdf.head())
zones.plot(figsize=(15, 10), color='#0B2380', cmap='tab20b')
string = zones.to_json()
with open('polygons.js', "w") as f:
    f.write("var polygons = " + string)

hacienda = gpd.read_file("polygons.json")
print(hacienda.head())

hacienda.plot(figsize=(15, 10), color='#0B2380', cmap='tab20b')

stats = {}
for device in gdf_devices:
    gdf_devices[device] = gdf_devices[device].assign(**{key: gdf_devices[device].within(geom) for key, geom in zones.items()})
    _stats = {'name': [key for key, _ in zones.items()], 'occurrences': [gdf_devices[device][key].values.sum() for key, geom in zones.items()]}
    stats[device] = pd.DataFrame.from_dict(data=_stats)
    print(stats[device])
    merged = zones_gdf.set_index('name').join(stats[device].set_index('name'), on='name', how='left')
    print(merged.head())
    variable = 'occurrences'
    vmin, vmax = 2, 346

    fig, ax = plt.subplots(1, figsize=(15, 10))
    merged.plot(ax=ax, column=variable, cmap='Reds', linewidth=0.8, edgecolor='0.8')
    ax.axis('off')
    ax.set_title('Title', fontdict={'fontsize':'25', 'fontweight':'3'})
    # ax.annotate('')
    # Create colorbar as a legend
    sm = plt.cm.ScalarMappable(cmap='Reds', norm=plt.Normalize(vmin=vmin, vmax=vmax))
    # empty array for the data range
    sm._A = []
    # add the colorbar to the figure
    cbar = fig.colorbar(sm)

    # base = hacienda.plot(ax=ax, color='#8BE898', cmap='tab20b')

    # gdf_devices[device].plot(ax=base, marker=".")
    plt.savefig(device + ".png")

plt.show()

print(gdf_devices)
print(stats)
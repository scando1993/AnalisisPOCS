import pandas as pd
import os
import binascii
import struct
import fiona
import geopandas as gpd
import shapely
from pandas._libs.tslibs import resolution
from shapely.geometry import Point
from pandas._libs.join import left_join_indexer
from pandas._libs.reshape import explode
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np


water = 'lightskyblue'
earth = 'cornsilk'

fig, ax1 = plt.subplots(figsize=(12, 10))
mm = Basemap(width=300000,
             height=200000,
             resolution='i',
             projection='lcc',
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
mm.etopo(ax=ax1)

plt.show()

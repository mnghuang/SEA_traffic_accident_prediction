from matplotlib import pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.colors import BoundaryNorm
from matplotlib.cm import ScalarMappable
import numpy as np
import pandas as pd
from mpl_toolkits.basemap import Basemap
from shapely.geometry import Point, Polygon, MultiPoint, MultiPolygon
from shapely.prepared import prep
import fiona
from descartes import PolygonPatch


class PlotSeattleChoropleth(object):

    def __init__(self, shapefile_name, section_name):
        self.shapefile_name = shapefile_name
        self.section_name = section_name
        self._set_basemap()
        self._set_mapdata

    def fit_data(self, lon, lat, values):
        mapped_points = [Point(self.map(x, y)) for x, y in zip(lon, lat)]
        all_points = MultiPoint(mapped_points)
        city_points = filter(self.hood_polygons.contains, all_points)
        self.df_map['values'] = values

    def plot_map(self, title_name=None, cmap='Blues', figwidth=14
                     , breaks=[0., 0.2, 0.4, 0.6, 0.8, 1.]):
        df_map['bins'] = self.df_map.hood_hours.apply(self.get_breaks,
                                                      args=(breaks,))
        bins_labels = ["{0}%".format(100 * x) for x in breaks]
        fig = plt.figure(figsize=(figwidth, figwidth * h / w))
        ax = fig.add_subplot(111, axisbg = 'w', frame_on = False)
        func = lambda x: PolygonPatch(x, ec='#111111', lw=.8, alpha=1.,
                                      zorder=4)
        self.df_map['patches'] = self.df_map['poly'].map(func)
        pc = PatchCollection(self.df_map['patches'], match_original=True)
        cmap_rng = (self.df_map.bins.values - self.df_map.bins.values.min())/ \
                   (self.df_map.bins.values.max() - self.df_map.bins.values.min())
        cmap_list = [cmap(val) for val in cmap_rng]
        pc.set_facecolor(cmap_list)
        ax.add_collection(pc)
        self.map.drawmapscale(self.coords[0] + 0.08,
                              self.coords[1] + -0.01,
                              self.coords[0],
                              self.coords[1],
                              10.,
                              fontsize=16,
                              barstyle='fancy',
                              labelstyle='simple',
                              fillcolor1='w',
                              fillcolor2='#555555',
                              fontcolor='#555555',
                              zorder=5,
                              ax=ax)
        cbar = self._create_colorbar(cmap, ncolors = len(bins_labels) + 1,
                               labels = bins_labels, shrink=0.5)
        cbar.ax.tick_params(labelsize=16)
        fig.suptitle(title_name, fontdict={'size':24, 'fontweight':'bold'},
                     y=0.92)


    def _set_basemap(self):
        shp = fiona.open('{0}.shp'.format(self.shapefile_name))
        self.coords = shp.bounds
        shp.close()
        w, h = self.coords[2] - self.coords[0], self.coords[3] - self.coords[1]
        extra = 0.01
        self.map = Basemap(projection = 'tmerc', ellps = 'WGS84',
                           lon_0 = np.mean([coords[0], coords[2]]),
                           lat_0 = np.mean([coords[1], coords[3]]),
                           llcrnrlon = coords[0] - extra * w,
                           llcrnrlat = coords[1] - (extra * h), 
                           urcrnrlon = coords[2] + extra * w,
                           urcrnrlat = coords[3] + (extra * h),
                           resolution = 'i',
                           suppress_ticks = True)
        self.out = self.map.readshapefile(shapefilename, name='seattle',
                                          drawbounds=False, color='none',
                                          zorder=2)

    def _set_mapdata(self):
        self.df_map = pd.DataFrame({
            'poly': [Polygon(points) for points in self.map.seattle],
            'name': [hood[self.section_name] for hood in self.map.seattle_info]
        })

        self.hood_polygons = prep(MultiPolygon(list(df_map['poly'].values)))

    def get_breaks(self, value, breaks):
        for i in xrange(len(breaks) - 1):
            if value > breaks[i] and value <= breaks[i + 1]:
                return i
        return -1

    def _create_colorbar(cmap, ncolors, labels, **kwargs):    
        norm = BoundaryNorm(range(0, ncolors), cmap.N)
        mappable = ScalarMappable(cmap=cmap, norm=norm)
        mappable.set_array([])
        mappable.set_clim(-0.5, ncolors+0.5)
        colorbar = plt.colorbar(mappable, **kwargs)
        colorbar.set_ticks(np.linspace(0, ncolors, ncolors+1)+0.5)
        colorbar.set_ticklabels(range(0, ncolors))
        colorbar.set_ticklabels(labels)
        return colorbar
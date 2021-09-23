import geopandas as gpd
import pandas as pd
import logging
import requests
from shapely.geometry import Polygon
from tqdm import tqdm
import os

logger = logging.getLogger(__name__)

def get_taz_geoms(region, taz_id_col_in='objectid', zone_id_col_out = 'zone_id'):

    if region == 'sfbay':
        url = (
            'https://opendata.arcgis.com/datasets/'
            '94e6e7107f0745b5b2aabd651340b739_0.geojson')
    
    gdf = gpd.read_file(url, crs="EPSG:4326")
    gdf.rename(columns={taz_id_col_in: zone_id_col_out}, inplace=True)

    # zone_id col must be str
    gdf[zone_id_col_out] = gdf[zone_id_col_out].astype(str)

    return gdf


def get_county_block_geoms(state_fips, county_fips, zone_type = 'blocks', result_size=10000):

    if zone_type == 'blocks':
        base_url = (
            'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/'
#             'Tracts_Blocks/MapServer/12/query?where=STATE%3D{0}+and+COUNTY%3D{1}' #2020 census
            'tigerWMS_Census2010/MapServer/18/query?where=STATE%3D{0}+and+COUNTY%3D{1}'#2010 census
            '&resultRecordCount={2}&resultOffset={3}&orderBy=GEOID'
            '&outFields=GEOID%2CSTATE%2CCOUNTY%2CTRACT%2CBLKGRP%2CBLOCK%2CCENTLAT'
            '%2CCENTLON&outSR=%7B"wkid"+%3A+4326%7D&f=pjson')

    elif zone_type == 'block_groups':
        base_url = (
            'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/'
#             'Tracts_Blocks/MapServer/11/query?where=STATE%3D{0}+and+COUNTY%3D{1}' #2020 census
            'tigerWMS_Census2010/MapServer/16/query?where=STATE%3D{0}+and+COUNTY%3D{1}'#2010 census
            '&resultRecordCount={2}&resultOffset={3}&orderBy=GEOID'
            '&outFields=GEOID%2CSTATE%2CCOUNTY%2CTRACT%2CBLKGRP%2CCENTLAT'
            '%2CCENTLON&outSR=%7B"wkid"+%3A+4326%7D&f=pjson')
        
    blocks_remaining = True
    all_features = []
    page = 0
    while blocks_remaining:
        offset = page * result_size
        url = base_url.format(state_fips, county_fips, result_size, offset)
        result = requests.get(url)
        try:
            features = result.json()['features']
        except KeyError:
            logger.error("No features returned. Try a smaller result size.")
        all_features += features
        if 'exceededTransferLimit' in result.json().keys():
            if result.json()['exceededTransferLimit']:
                page += 1
            else:
                blocks_remaining = False
        else:
            if len(features) == 0:
                blocks_remaining = False
            else:
                page += 1

    df = pd.DataFrame()
    for feature in all_features:
        tmp = pd.DataFrame([feature['attributes']])
        tmp['geometry'] = Polygon(
            feature['geometry']['rings'][0],
            feature['geometry']['rings'][1:])
        df = pd.concat((df, tmp))
    gdf = gpd.GeoDataFrame(df, crs="EPSG:4326")
    return gdf


def get_block_geoms(settings, data_dir='./tmp/'):
    
    region = settings['region']
    FIPS = settings['FIPS'][region]
    state_fips = FIPS['state']
    county_codes = FIPS['counties']
    zone_type = settings['zone_type']
    
    all_block_geoms = []

    if os.path.exists(os.path.join(data_dir, zone_type + ".shp")):
        logger.info("Loading block geoms from disk!")
        blocks_gdf = gpd.read_file(os.path.join(data_dir, zone_type + ".shp"))

    else:
        logger.info("Downloading {zone_type} geoms from Census TIGERweb API!".format())

        # get block geoms from census tigerweb API
        for county in tqdm(
                county_codes, total=len(county_codes),
                desc='Getting block geoms for {0} counties'.format(
                    len(county_codes))):
            county_gdf = get_county_block_geoms(state_fips, county, zone_type)
            all_block_geoms.append(county_gdf)

        blocks_gdf = gpd.GeoDataFrame(
            pd.concat(all_block_geoms, ignore_index=True), crs="EPSG:4326")

        # save to disk
        logger.info(
            "Got {0} block geometries. Saving to disk.".format(
                len(all_block_geoms)))
        blocks_gdf.to_file(os.path.join(data_dir, zone_type + ".shp"))

    return blocks_gdf


def get_taz_from_block_geoms(blocks_gdf, zones_gdf, local_crs, zone_col_name):

    logger.info("Assigning blocks to TAZs!")

    # df to store GEOID to TAZ results
    block_to_taz_results = pd.DataFrame()

    # ignore empty geoms
    zones_gdf = zones_gdf[~zones_gdf['geometry'].is_empty]

    # convert to meter-based proj
    zones_gdf = zones_gdf.to_crs(local_crs)
    blocks_gdf = blocks_gdf.to_crs(local_crs)

    zones_gdf['zone_area'] = zones_gdf.geometry.area

    # assign zone ID's to blocks based on max area of intersection
    intx = gpd.overlay(blocks_gdf, zones_gdf.reset_index(), how='intersection')
    intx['intx_area'] = intx['geometry'].area
    intx = intx.sort_values(['GEOID', 'intx_area'], ascending=False)
    intx = intx.drop_duplicates('GEOID', keep='first')

    # add to results df
    block_to_taz_results = pd.concat((
        block_to_taz_results, intx[['GEOID', zone_col_name]]))

    # assign zone ID's to remaining blocks based on shortest
    # distance between block and zone centroids
    unassigned_mask = ~blocks_gdf['GEOID'].isin(block_to_taz_results['GEOID'])

    if any(unassigned_mask):

        blocks_gdf['geometry'] = blocks_gdf['geometry'].centroid
        zones_gdf['geometry'] = zones_gdf['geometry'].centroid

        all_dists = blocks_gdf.loc[unassigned_mask, 'geometry'].apply(
            lambda x: zones_gdf['geometry'].distance(x))

        nearest = all_dists.idxmin(axis=1).reset_index()
        nearest.columns = ['blocks_idx', zone_col_name]
        nearest.set_index('blocks_idx', inplace=True)
        nearest['GEOID'] = blocks_gdf.reindex(nearest.index)['GEOID']

        block_to_taz_results = pd.concat((
            block_to_taz_results, nearest[['GEOID', zone_col_name]]))

    return block_to_taz_results.set_index('GEOID')[zone_col_name]


def map_block_to_taz(
        settings, region, zones_gdf=None, zone_id_col='zone_id',
        reference_taz_id_col='objectid', data_dir='./tmp/'):
    """
    Returns:
        A series named 'zone_id' with 'GEOID' as index name
    """
    region = settings['region']
    FIPS = settings['FIPS'][region]
    state_fips = FIPS['state']
    county_codes = FIPS['counties']
    local_crs = settings['local_crs'][region]
    
    if zones_gdf is None:
        zones_gdf = get_taz_geoms(region, reference_taz_id_col, zone_id_col)
    blocks_gdf = get_block_geoms(state_fips, county_codes, data_dir)
    blocks_gdf.crs = 'EPSG:4326'
    blocks_to_taz = get_taz_from_block_geoms(
        blocks_gdf, zones_gdf, local_crs, zone_id_col)

    return blocks_to_taz


def get_zone_from_points(df, zones_gdf, zone_id_col, local_crs):
    '''
    Assigns the gdf index (zone_id) for each index in df
    Parameters:
    -----------
    - df columns names x, and y. The index is the ID of the point feature.
    - zones_gdf: GeoPandas GeoDataFrame with zone_id as index, geometry, area.

    Returns:
    -----------
        A series with df index and corresponding gdf id
    '''

    logger.info("Assigning zone IDs to {0}".format(df.index.name))
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.x, df.y), crs="EPSG:4326")
    
    zones_gdf.geometry.crs = "EPSG:4326"

    # convert to meters-based local crs
    gdf = gdf.to_crs(local_crs)
    zones_gdf = zones_gdf.to_crs(local_crs)

    # Spatial join
    intx = gpd.sjoin(
        gdf, zones_gdf.reset_index(),
        how='left', op='intersects')

    return intx[zone_id_col]

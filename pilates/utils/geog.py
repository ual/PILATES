import geopandas as gpd
import pandas as pd
import logging
import requests
from shapely.geometry import Polygon
from tqdm import tqdm
import os

from pilates.utils.io import read_datastore

logger = logging.getLogger(__name__)

def get_taz_geoms(settings, taz_id_col_in='taz1454', zone_id_col_out='zone_id', 
                  data_dir='./tmp/' ):
    
    region = settings['region']
    zone_type = settings['skims_zone_type']

    file_name =  '{0}_{1}.shp'.format(zone_type, region)
    taz_geoms_fpath = os.path.join(data_dir, file_name)
    
    if os.path.exists(taz_geoms_fpath):
        logger.info("Loading taz geoms from disk!")
        gdf = gpd.read_file(taz_geoms_fpath)
        
    else:
        logger.info("Downloading {} geoms".format(zone_type))

        if region == 'sfbay':
            url = (
                'https://opendata.arcgis.com/datasets/'
                '94e6e7107f0745b5b2aabd651340b739_0.geojson')

        elif region == 'austin':
            url = (
                'https://beam-outputs.s3.amazonaws.com/pilates-outputs/geometries/block_groups_austin.geojson')
            
        ## FIX ME: other regions taz should be here - only sfbay for now
        gdf = gpd.read_file(url, crs="EPSG:4326")
        gdf.rename(columns={taz_id_col_in: zone_id_col_out}, inplace=True)

        # zone_id col must be str
        gdf[zone_id_col_out] = gdf[zone_id_col_out].astype(str)
        gdf.to_file(taz_geoms_fpath)
    
    return gdf


def get_county_block_geoms(
        state_fips, county_fips, zone_type='block', result_size=10000):

    if (zone_type == 'block') or (zone_type == 'taz'): #to map blocks to taz. 
        base_url = (
            'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/'
#             'Tracts_Blocks/MapServer/12/query?where=STATE%3D{0}+and+COUNTY%3D{1}' #2020 census
            'tigerWMS_Census2010/MapServer/18/query?where=STATE%3D{0}+and+COUNTY%3D{1}'#2010 census
            '&resultRecordCount={2}&resultOffset={3}&orderBy=GEOID'
            '&outFields=GEOID%2CSTATE%2CCOUNTY%2CTRACT%2CBLKGRP%2CBLOCK%2CCENTLAT'
            '%2CCENTLON&outSR=%7B"wkid"+%3A+4326%7D&f=json')

    elif zone_type == 'block_group':
        base_url = (
            'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/'
#             'Tracts_Blocks/MapServer/11/query?where=STATE%3D{0}+and+COUNTY%3D{1}' #2020 census
            'tigerWMS_Census2010/MapServer/16/query?where=STATE%3D{0}+and+COUNTY%3D{1}'#2010 census
            '&resultRecordCount={2}&resultOffset={3}&orderBy=GEOID'
            '&outFields=GEOID%2CSTATE%2CCOUNTY%2CTRACT%2CBLKGRP%2CCENTLAT'
            '%2CCENTLON&outSR=%7B"wkid"+%3A+4326%7D&f=json')

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

    region = settings['region'] or 'beam'
    FIPS = settings['FIPS'][region]
    state_fips = FIPS['state']
    county_codes = FIPS['counties']
    
    zone_type = settings['skims_zone_type']
    if zone_type == 'taz':
        zone_type_v1 = 'block' #triger block geometries
    else:
        zone_type_v1 = zone_type

    all_block_geoms = []
    file_name = '{0}_{1}.shp'.format(zone_type_v1, region)

    if os.path.exists(os.path.join(data_dir, file_name)):
        logger.info("Loading block geoms from disk!")
        blocks_gdf = gpd.read_file(os.path.join(data_dir, file_name))

    else:
        logger.info("Downloading {} geoms from Census TIGERweb API!".format(zone_type))

        # get block geoms from census tigerweb API
        for county in tqdm(
                county_codes, total=len(county_codes),
                desc='Getting block geoms for {0} counties'.format(
                    len(county_codes))):
            county_gdf = get_county_block_geoms(state_fips, county, zone_type)
            all_block_geoms.append(county_gdf)
  
        blocks_gdf = gpd.GeoDataFrame(
            pd.concat(all_block_geoms, ignore_index=True), crs="EPSG:4326")
        
        # make sure geometries match with geometries in blocks table
        if zone_type in ['block','block_group']:
            geoids = list(geoid_to_zone_map(settings, year=None).keys())
            blocks_gdf = blocks_gdf[blocks_gdf.GEOID.isin(geoids)]

        # save to disk
        logger.info(
            "Got {0} block geometries. Saving to disk.".format(
                len(all_block_geoms)))
        blocks_gdf.to_file(os.path.join(data_dir, file_name))

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
        reference_taz_id_col='taz1454', data_dir='./tmp/'):
    """
    Returns:
        A series named 'zone_id' with 'GEOID' as index name
    """
    region = settings['region']
    local_crs = settings['local_crs'][region]

    if zones_gdf is None:
        zones_gdf = get_taz_geoms(settings, reference_taz_id_col, zone_id_col)
    blocks_gdf = get_block_geoms(settings, data_dir)
    blocks_gdf.crs = 'EPSG:4326'
    blocks_to_taz = get_taz_from_block_geoms(
        blocks_gdf, zones_gdf, local_crs, zone_id_col)
    return blocks_to_taz.astype(str)


def get_zone_from_points(df, zones_gdf, local_crs):
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
    zone_id_col = zones_gdf.index.name
    
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
    
    assert len(intx) == len(gdf)

    return intx[zone_id_col]


def geoid_to_zone_map(settings, year=None):
    """"
    Maps the GEOID to a unique zone_id. 

    Returns
    --------
    Returns a dictionary. Keys are GEOIDs and values are the 
    corresponding zone_id where the GEOID belongs to. 
   """
    region = settings['region']
    zone_type = settings['skims_zone_type']
    travel_model = settings.get('travel_model', 'beam')
    zone_id_col = 'zone_id'

    geoid_to_zone_fpath = \
        "pilates/utils/data/{0}/{1}/{2}_geoid_to_zone.csv".format(region, travel_model,zone_type)

    if os.path.isfile(geoid_to_zone_fpath):
        logger.info("Reading GEOID to zone mapping.")
        geoid_to_zone = pd.read_csv(
            geoid_to_zone_fpath, dtype={'GEOID': str, zone_id_col: str})

        num_zones = geoid_to_zone[zone_id_col].nunique()

        if zone_type != 'taz':
            assert geoid_to_zone[zone_id_col].astype(int).min() == 1
            assert geoid_to_zone[zone_id_col].astype(int).max() == num_zones

        mapping = geoid_to_zone.set_index('GEOID')[zone_id_col].to_dict()

    else:
        logger.info("Zone mapping not found. Creating it on the fly.")

        if zone_type == 'taz':
            logger.info("Mapping block IDs to TAZ")
            geoid_to_zone = map_block_to_taz(
                settings, region, zone_id_col=zone_id_col,
                reference_taz_id_col='taz1454')
            mapping = geoid_to_zone.to_dict()

        elif zone_type == 'block_group':
            store, table_prefix_yr = read_datastore(settings, year)
            blocks = store[os.path.join(table_prefix_yr, 'blocks')]
            order = blocks.index.str[:12].unique()
            store.close()

        elif zone_type == 'block':
            store, table_prefix_year = read_datastore(settings, year)
            blocks = store[os.path.join(table_prefix_year, 'blocks')]
            order = blocks.index.unique()
            store.close()

        else:
            logger.info(
                "Define a valid zone type value. Options "
                "['taz', 'block_group', 'block']")

        # zone IDs are generated on-the-fly for GEOID/FIPS-based skims
        if zone_type in ['block', 'block_group']:

            geoid_to_zone = pd.DataFrame(
                {'GEOID': order, 'zone_id': range(1, len(order) + 1)},
                dtype=str)

            geoid_to_zone = geoid_to_zone.set_index('GEOID')[zone_id_col]
            mapping = geoid_to_zone.to_dict()

        # Save file to disk
        geoid_to_zone.to_csv(geoid_to_zone_fpath)

    return mapping

import sys
import os
# import math
# import tables
import pandas as pd
import sqlite3
import random
# from operator import itemgetter
# from collections import OrderedDict
import logging
from sortedcontainers import SortedDict

logger = logging.getLogger(__name__)

# ======================= Settings for the run =======================
# usim_output = 'model_data_2011.h5'
# db_demand = 'campo-Demand.sqlite'
# db_supply = 'campo-Supply.sqlite'
# block_loc_file = 'campo_block_to_loc.csv'
# population_scale_factor = 0.25
# ====================================================================


class Household:
	def __init__(self, id, hh, from_usim=True):
		self.id = id
		if from_usim:
			self.hhold = hh['serialno']
			self.block_id = hh['block_id']
			self.location = -1
			self.zone = -1
			self.vehicles = hh['cars']
			self.persons = hh['persons']
			self.income = hh['income']
			self.workers = hh['workers']
			# 1= own_1p_<54, 2=own_1p_55+, 3=own_2p_<54, 4=own_2p_55+, 5= rent_1p_<54, 6=rent_1p_55+, 7=rent_2p_<54, 8=rent_2p_55+
			hht_mapping = {1: 4, 2: 4, 3: 1, 4: 1, 5: 9, 6: 9, 7: 6, 8: 6} 
			self.hhtype = hht_mapping[hh['hh_type']] 
			hu_type_map = {'yes': 1, 'no': 3}
			self.housing_unit_type = hu_type_map[hh['sf_detached']]				
			self.person_list = {}
			self.vehicle_list = {}
			self.ecom = 0
			self.dispose_veh = 0
			if 'time_in_home' in hh:
				self.time_in_home = per['time_in_home']
				if pd.isnull(per['time_in_home']):
					self.time_in_home = -1
			else:
				self.time_in_home = -1
			self.usim_record = hh
			self.data = hh
		else:
			self.block_id = None
			self.zone = None
			self.person_list = {}
			self.vehicle_list ={}
			self.location = hh['location']
			
			self.usim_record = None
			# store the full record from the sqlite query as a data element, in case specification changes
			self.data = hh

			
	
	def push_to_db(self, dbCon):
		try:
			if self.usim_record is not None:
				query = 'insert into Household (household,hhold,location,persons,workers,vehicles,type,income,housing_unit_type, ecom, dispose_veh, time_in_home) values (?,?,?,?,?,?,?,?,?,?,?,?);'
				dbCon.execute(query, [self.id, self.hhold, self.location, self.persons, self.workers, self.vehicles, self.hhtype, self.income, self.housing_unit_type, self.ecom, self.dispose_veh, self.time_in_home])
			else:
				fields_list = ','.join(self.data.keys())
				values_list = ','.join(['?']*len(self.data.keys()))
				query ='insert into Household (household, {}) values (?, {});'.format(fields_list, values_list)
				dbCon.execute(query, [self.id, *list(self.data.values())])
		except sqlite3.IntegrityError:
			print(self.data)
		
	def set_marital_status_for_members(self):
		married = False
		ref_per = None
		spouse = None
		for p in self.person_list.values():
			if p.relate == 0:
				ref_per = p
			if p.relate == 1:
				spouse = p
				married = True
		if married:
			ref_per.marital_status = 1
			spouse.marital_status = 1


class Person:
	def __init__(self, id, per, from_usim=True):
		self.id = id
		if from_usim:
			self.per_id = per['member_id']-1
			self.household = per['household_id']
			self.age = per['age']
			self.worker_class = per['worker']
			self.education = per['edu']
			self.industry = 0
			self.employment = per['worker']
			self.gender = per['sex']
			self.income = per['earning']
			self.relate = per['relate']
			self.marital_status = 5  # never married in Polaris enum
			self.race = per['race_id']
			enrollment_map = {0: 0, 1: 2}
			self.school_enrollment = enrollment_map[per['student']]
			self.school_grade_level = max(min(per['student']*(self. age-3), 15), 0)  # use age to guess at grade level for those enrolled in school
			self.work_hours = per['hours']
			self.telecommute_level = per['work_at_home']*4
			# create and initialze the school and work zones if they don't exist - otherwise read them. Note - called 'zone' in the urbansim data, but in polaris refers to the activity location id
			if 'work_zone_id' in per:
				self.work_zone_id = per['work_zone_id']
				if pd.isnull(per['work_zone_id']):
					self.work_zone_id = -1
			else:
				self.work_zone_id = -1
			if 'school_zone_id' in per:
				self.school_zone_id = per['school_zone_id']
				if pd.isnull(per['school_zone_id']): # if value exists but is null in urbansim data - replace with -1 due to not null constraint in polaris sqlite db
					self.school_zone_id = -1
			else:
				self.school_zone_id = -1
			if 'time_in_job' in per:
				self.time_in_job = per['time_in_job']
				if pd.isnull(per['time_in_job']):
					self.time_in_job = -1
			else:
				self.time_in_job = -1
			self.transit_pass = 0
			self.usim_record = per
		else:
			self.household = per['household']
			self.data = per
			self.usim_record = None
			
	def push_to_db(self, dbCon):
		if self.usim_record is not None:
			try:
				if not self.school_zone_id:
					self.school_zone_id = -1
				#query = 'insert into Person (person,household,id,age,worker_class,education,industry,employment,gender,income, marital_status, race,school_enrollment, school_grade_level,work_hours,telecommute_level, transit_pass, work_location_id, school_location_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'
				query = 'insert into Person (household,id,age,worker_class,education,industry,employment,gender,income, marital_status, race,school_enrollment, school_grade_level,work_hours,telecommute_level, transit_pass, work_location_id, school_location_id, time_in_job) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'
				dbCon.execute(query, [self.household, self.per_id, self.age, self.worker_class, self.education, self.industry, self.employment, self.gender, self.income, self.marital_status, self.race, self.school_enrollment, self.school_grade_level, self.work_hours, self.telecommute_level, self.transit_pass, self.work_zone_id, self.school_zone_id, self.time_in_job])
			except sqlite3.IntegrityError:
				print('SQLITE3 integrity error: ')
				print(self.usim_record)
		else:
			try:
				fields_list = ','.join(self.data.keys())
				values_list = ','.join(['?']*len(self.data.keys()))
				query ='insert into Person ({}) values ({});'.format(fields_list, values_list)
				dbCon.execute(query, list(self.data.values()))
			except sqlite3.IntegrityError:
				print('SQLITE3 integrity error: ')
				print(self.data)
	
	
class Vehicle:
	def __init__(self, veh):	
		self.id = veh['vehicle_id']
		self.household = veh['hhold']
		self.data = veh

		
	def push_to_db(self, dbCon):
		try:
			fields_list = ','.join(self.data.keys())
			values_list = ','.join(['?']*len(self.data.keys()))
			query ='insert into Vehicle ({}) values ({});'.format(fields_list, values_list)
			dbCon.execute(query, list(self.data.values()))
		except sqlite3.IntegrityError:
			print('SQLITE3 integrity error: ')
			print(self.veh)
	

class Job:
	def __init__(self, job):
		self.block_id = job['block_id']
		self.sector = job['sector_id']
		job_mapping = {'11': 3, '21': 3, '22': 3, '23': 3, '42': 0, '51': 2, '52': 2, '53': 2, '54': 2, '55': 2, '56': 5, '61': 1, '62': 5, '71': 5, '72': 2, '81': 2, '92': 1, '31-33': 4, '44-45': 0, '48-49': 3}
		self.sector_agg = job_mapping[self.sector]
		self.zone = None
		self.usim_record = job
		

class Block:
	def __init__(self, id, block):
		self.id = id
		self.county_id = block['county_id']
		self.tract_id = block['tract_id']
		self.zone = None


class Zone:
	def __init__(self, id):
		self.id = id
		self.households = 0
		self.persons = 0
		self.employment_total = 0
		self.employment_retail = 0
		self.employment_government = 0
		self.employment_manufacturing = 0
		self.employment_services = 0
		self.employment_industrial = 0
		self.employment_other = 0
		self.percent_white = 0
		self.percent_black = 0
		self.hh_inc_avg = 0
	
	def Add_HH(self, HH):
		self.households += 1
		self.hh_inc_avg += HH.income
		for p in HH.person_list.values():
			self.Add_Person(p)
	
	def Add_Person(self, P):
		self.persons += 1
		if P.race == 1:
			self.percent_white += 1
		if P.race == 1:
			self.percent_black += 1
	
	def Add_Job(self, J):
		self.employment_total += 1
		if J.sector_agg == 0:
			self.employment_retail += 1
		if J.sector_agg == 1:
			self.employment_government += 1
		if J.sector_agg == 2:
			self.employment_services += 1
		if J.sector_agg == 3:
			self.employment_industrial += 1
		if J.sector_agg == 4:
			self.employment_manufacturing += 1
		if J.sector_agg == 5:
			self.employment_other += 1
			
	def Normalize(self):
		if self.persons > 0:
			self.percent_white = self.percent_white / self.persons
			self.percent_black = self.percent_black / self.persons
		if self.households > 0:
			self.hh_inc_avg = self.hh_inc_avg / self.households
		
	def Update_Zone_in_DB(self, DbCon):
		Q = 'UPDATE zone SET pop_households = ?, pop_persons = ?, employment_total = ?, employment_retail = ?, employment_retail = ?, employment_retail = ?, employment_retail = ?, employment_retail = ?, employment_retail = ?, percent_white = ?, percent_black = ?, hh_inc_avg = ? WHERE zone = ?'
		DbCon.execute(Q, [self.households,  self.persons,  self.employment_total,  self.employment_retail,  self.employment_government,  self.employment_manufacturing,  self.employment_services,  self.employment_industrial,  self.employment_other,  self.percent_white,  self.percent_black,  self.hh_inc_avg, self.id])
		

def clean_db(DbCon, clear_agents=True):
	logger.info("Clean up the input database for writing...")

	DbCon.execute('pragma foreign_keys=off;')
	
	if clear_agents:
		DbCon.execute('delete from Household;')
		DbCon.execute('delete from Person;')
		DbCon.execute('delete from Vehicle;')
	
	DbCon.execute('delete from Activity;')
	DbCon.execute('delete from EV_Charging;')
	DbCon.execute('delete from Path;')
	DbCon.execute('delete from Path_Multimodal;')
	DbCon.execute('delete from Path_Multimodal_links;')
	DbCon.execute('delete from Path_links;')	
	DbCon.execute('delete from Person_Gaps;')
	DbCon.execute('delete from Plan;')
	DbCon.execute('delete from TNC_Trip;')
	DbCon.execute('delete from Transit_Vehicle;')
	DbCon.execute('delete from Transit_Vehicle_links;')
	DbCon.execute('delete from Traveler;')
	DbCon.execute('delete from Trip where person is not null;')
	
	DbCon.execute('drop table if exists act_wait_count;')
	DbCon.execute('drop table if exists activity_Start_Distribution;')
	DbCon.execute('drop table if exists activity_distribution;')
	DbCon.execute('drop table if exists activity_rate_distribution;')
	DbCon.execute('drop table if exists artificial_count;')
	DbCon.execute('drop table if exists avg_wait_and_total_time;')
	DbCon.execute('drop table if exists avg_wait_and_total_time_cta;')
	DbCon.execute('drop table if exists boardings_by_agency_and_trip_mode;')
	DbCon.execute('drop table if exists boardings_by_agency_mode;')
	DbCon.execute('drop table if exists boardings_by_agency_mode_area_type;')
	DbCon.execute('drop table if exists boardings_by_agency_mode_time;')
	DbCon.execute('drop table if exists cta_IVTT_Wait_path_id_trip_id;')
	DbCon.execute('drop table if exists cta_IVTT_Wait_path_id_trips_aggregated;')
	DbCon.execute('drop table if exists executed_activity_dist_by_area;')
	DbCon.execute('drop table if exists executed_activity_dist_by_area_city;')
	DbCon.execute('drop table if exists executed_activity_mode_share;')
	DbCon.execute('drop table if exists executed_activity_mode_share_by_area;')
	DbCon.execute('drop table if exists executed_activity_mode_share_fails;')
	DbCon.execute('drop table if exists gap_bins;')
	DbCon.execute('drop table if exists gap_breakdown;')
	DbCon.execute('drop table if exists gap_breakdown_binned;')
	DbCon.execute('drop table if exists gap_calculations;')
	DbCon.execute('drop table if exists gap_calculations_binned;')
	DbCon.execute('drop table if exists greater_routed_time;')
	DbCon.execute('drop table if exists mode_Distribution_ADULT;')
	DbCon.execute('drop table if exists mode_count;')
	DbCon.execute('drop table if exists person_distribution;')
	DbCon.execute('drop table if exists planned_activity_mode_share;')
	DbCon.execute('drop table if exists planned_activity_mode_share_by_area;')
	DbCon.execute('drop table if exists there_is_path;')
	DbCon.execute('drop table if exists transit_occupancy;')
	DbCon.execute('drop table if exists transit_transfers;')
	DbCon.execute('drop table if exists transit_transfers_with_gen_cost;')
	DbCon.execute('drop table if exists transit_trips_with_boarding_count;')
	DbCon.execute('drop table if exists transit_trips_wo_boarding_count;')
	DbCon.execute('drop table if exists trips_in_network_city;')
	DbCon.execute('drop table if exists ttime_By_ACT_Average;')
	DbCon.execute('drop table if exists ttime_By_ACT_Average_w_skims;')
	DbCon.execute('drop table if exists ttime_By_ACT_Average_w_skims_hway;')
	DbCon.execute('drop table if exists vmt_vht_by_mode;')
	DbCon.execute('drop table if exists vmt_vht_by_mode_city;')
	DbCon.execute('pragma foreign_keys=on;')
	DbCon.commit()


class Usim_Data:
	def __init__(self, forecast_year, usim_output):
		self.block_hh_count = {}
		self.hh_data = None
		self.per_data = None
		self.job_data = None
		self.block_data = None

		self.hh_dict = {}
		self.per_dict = {}
		self.job_dict = {}
		self.block_dict = {}
		
		# verify filepaths
		#usim_output = "{0}/model_data_{1}.h5".format(usim_output_dir, forecast_year)
		if not os.path.exists(usim_output):
			logger.critical("Error: input urbansim data file path not found: " + usim_output)
			sys.exit()
			
		# Connect to urbansim output and import data
		if forecast_year:
			self.households_data_lbl = '{0}/households'.format(forecast_year)
			self.persons_data_lbl = '{0}/persons'.format(forecast_year)
			self.jobs_data_lbl = '{0}/jobs'.format(forecast_year)
			self.blocks_data_lbl = '{0}/blocks'.format(forecast_year)
		else:
			self.households_data_lbl = 'households'
			self.persons_data_lbl = 'persons'
			self.jobs_data_lbl = 'jobs'
			self.blocks_data_lbl = 'blocks'
		
		self.usim_data = pd.HDFStore(usim_output)
		
		self.hh_data = self.usim_data[self.households_data_lbl]
		self.hh_idx = self.hh_data.index
		self.per_data = self.usim_data[self.persons_data_lbl]
		self.per_idx = self.per_data.index
		self.job_data = self.usim_data[self.jobs_data_lbl]
		self.block_data = self.usim_data[self.blocks_data_lbl]
		
	def Fill_From_Usim_Output(self):
		
		#self.Get_Usim_Datastores()

		# Iterate through households in the datastore and construct HH objects, as well as the household-block distribution pdf
		logger.info('Reading households from Urbansim output...')
		for idx, data in self.hh_data.iterrows():
			hh = Household(idx, data)
			self.hh_dict[hh.id] = hh		

		# Iterate through persons in the datastore and construct PER objects
		logger.info('Reading persons from Urbansim output...')
		for idx, data in self.per_data.iterrows():
			p = Person(idx, data)
			
			# check that person is in a valid household
			if p.household in self.hh_dict:
				self.hh_dict[p.household].person_list[p.id] = p  # put person into the appropriate household member dictionary

		for h in self.hh_dict.values():
			h.set_marital_status_for_members()
		
		# Iterate through blocks in the datastore and construct Block objects		
		logger.info('Reading blocks from Urbansim output...')
		for id, data in self.block_data.iterrows():
			b = Block(id, data)
			self.block_dict[id] = b
			
		logger.info('Reading jobs from Urbansim output...')
		for id, data in self.job_data.iterrows():
			j = Job(data)
			self.job_dict[id] = j

	def Close(self):
		self.usim_data[self.persons_data_lbl] = self.per_data 
		self.usim_data.close()
		logger.info("Closing urbansim hdf5 file...")


class Polaris_Data:
	def __init__(self, demand_db):
		self.hh_dict = {}
		self.per_dict = {}
		self.veh_dict = {}
		self.Fill_From_Polaris_Data(demand_db)
		
	def Fill_From_Polaris_Data(self, demand_db):
		demand_db.row_factory = sqlite3.Row
		cur = demand_db.cursor()
	
		for row in cur.execute('Select * from Household').fetchall():
			data = dict(row)
			id = data['household']
			hh = Household(id, data, False)
			self.hh_dict[hh.id] = hh
		for row in cur.execute('Select * from Person').fetchall():
			data = dict(row)
			id = data['person']
			p = Person(id, data, False)
			if p.household in self.hh_dict:
				self.hh_dict[p.household].person_list[p.id] = p  # put person into the appropriate household member dictionary
		for row in cur.execute('Select * from Vehicle').fetchall():
			data = dict(row)
			v = Vehicle(data)
			if v.data['hhold'] in self.hh_dict:
				self.hh_dict[v.household].vehicle_list[v.id] = v
				
		# clear the source demand db after reading
		clean_db(demand_db)


def preprocess_usim_for_polaris(forecast_year, usim_output_dir, block_loc_file, db_supply, db_demand, population_scale_factor, usim_settings):
	
	logger.info('Starting polaris preprocessor for forecast year {0}'.format(forecast_year))
	
	random.seed()
	
	# verify filepaths
	delete_agents = True # flag to indicate whether all hh, person, vehicles should be deleted and reimported from UrbanSim (True) or only updated from UrbanSim (False)
	
	if forecast_year:
		usim_output = "{0}/model_data_{1}.h5".format(usim_output_dir, forecast_year)
		delete_agents = False
	else:
		# no forecast year so read the input urbansim model from settings
		region = usim_settings['region']
		region_id = usim_settings['region_to_region_id'][region]
		usim_base_fname = usim_settings['usim_formattable_input_file_name']
		usim_base = usim_base_fname.format(region_id=region_id)
		usim_output = "{0}/{1}".format(usim_output_dir, usim_base)
	if not os.path.exists(db_demand):
		logger.critical("Error: input demand db file path not found: " + db_demand)
		sys.exit()
	if not os.path.exists(db_supply):
		logger.critical("Error: input supply db file path not found" + db_supply)
		sys.exit()
	if not os.path.exists(usim_output):
		logger.critical("Error: input urbansim data file path not found: " + usim_output)
		sys.exit()
	if not os.path.exists(block_loc_file):
		logger.critical("Error: census block to Polaris location mapping file path not found")
		sys.exit()

	# Connect to POLARIS DBs to modify
	DbCon = sqlite3.connect(db_demand)
	DbSupply = sqlite3.connect(db_supply)

	# Geographic correspondence structures
	block_to_loc = {}
	loc_to_zone = {}
	block_to_zone = {}
	
	# Read in block to location correspondence
	blk = pd.read_csv(block_loc_file, dtype='str')
	for id, data in blk.iterrows():
		block_to_loc[str(data['InputID'])] = int(data['TargetID'])

	# Read in location to zone correspondence from Supply
	for row in DbSupply.execute('Select location, zone from location'):
		loc_to_zone[row[0]] = row[1]
	
	# construct block to zone correspondence
	for b, l in block_to_loc.items():
		block_to_zone[b] = loc_to_zone[l]


	# ==================== MODIFY THE INPUT DEMAND POPULATION in DEMAND.SQLITE =======================================
	clean_db(DbCon, delete_agents)
	
	# Read the current demand database to find existing households/persons (will be empty if delete_agents=True)
	polaris_data = Polaris_Data(DbCon)

	logger.info('Reading urbansim data from {0}'.format(usim_output))
	usim_data = Usim_Data(forecast_year, usim_output)
	usim_data.Fill_From_Usim_Output()
	
	# Connect to urbansim output and import data
	hh_unplaced = []
	block_hh_count = {}
	block_pdf = SortedDict()

	# update block dictionary and initialize the zone structure
	zone_data = {}
	for id, b in usim_data.block_dict.items():
		b.zone = block_to_zone[id]
		if b.zone not in zone_data:
			zone_data[b.zone] = Zone(b.zone)

	# Iterate through households in the datastore and construct HH objects, as well as the household-block distribution pdf
	num_valid_HH = 0.0
	for id, hh in usim_data.hh_dict.items():
		# check for valid home location
		if hh.block_id in block_to_loc:
			hh.location = block_to_loc[hh.block_id]
			hh.zone = block_to_zone[hh.block_id]
			# update hh pdf precursors...
			num_valid_HH += 1.0
			if hh.block_id in block_hh_count:
				block_hh_count[hh.block_id] += 1.0
			else:
				block_hh_count[hh.block_id] = 1.0
		else:
			hh_unplaced.append(hh)
	
	# create the block pdf
	c_prob = 0.0
	for id, count in block_hh_count.items():
		c_prob += count/num_valid_HH
		block_pdf[c_prob] = id
	block_pdf[1.0] = block_pdf.values()[-1]
	

	# place the unplaced households
	for hh in hh_unplaced:
		r = random.random()
		key_block_idx = block_pdf.bisect_left(r)
		block_key = block_pdf.keys()[key_block_idx]
		hh.block_id = block_pdf[block_key]
		hh.location = block_to_loc[hh.block_id]
		hh.zone = block_to_zone[hh.block_id]


	# Sample the households according to sample %
	random.seed()
	hh_dict_sampled = []
	for h in usim_data.hh_dict.values():
		if random.random() < population_scale_factor:
			# if this was an existing household in the polaris model, just update the fields that Usim modifies
			if h.id in polaris_data.hh_dict.keys():
				hh = polaris_data.hh_dict[h.id]
				hh.location = h.location
				hh.zone = h.zone
				hh_dict_sampled.append(hh)
			# otherwise, create the whole household
			else:
				hh_dict_sampled.append(h)

	logger.info('Pushing households and persons to POLARIS DB...')
	for h in hh_dict_sampled:
		h.push_to_db(DbCon)
		for p in h.person_list.values():
			p.push_to_db(DbCon)
		for v in h.vehicle_list.values():
			v.push_to_db(DbCon)
	DbCon.commit()

	# ==================== MODIFY THE ZONES in SUPPLY.SQLITE =======================================
	logger.info('Reading jobs from Urbansim output...')
	for id, j in usim_data.job_dict.items():
		j.zone = block_to_zone[j.block_id]

	for id, hh in usim_data.hh_dict.items():
		hzone = zone_data[hh.zone]
		hzone.Add_HH(hh)

	for id, j in usim_data.job_dict.items():
		jzone = zone_data[j.zone]
		jzone.Add_Job(j)

	logger.info('Pushing zone data from Urbansim output...')
	for id, z in zone_data.items():
		z.Normalize()
		z.Update_Zone_in_DB(DbSupply)

	usim_data.Close()
	DbSupply.commit()


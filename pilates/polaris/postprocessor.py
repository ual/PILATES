import os
# import sys
import sqlite3
import argparse
import pilates.polaris.skim_file_reader as skim_reader
import logging

logger = logging.getLogger(__name__)

def archive_polaris_output(forecast_year, output_dir, data_dir):


def archive_usim_skims():
	logger.info('Archiving UrbanSim skims')


def postprocess_polaris_for_usim(database_name, NetworkDbPath, DemandDbPath, ResultDbPath, auto_skim_path, transit_skim_path, vot_level):

	skims = skim_reader.Skim_Results()
	skim_reader.Main(skims,auto_skim_path, transit_skim_path)

	#******************************************************************************************************************************************************************
	#standard db entry - do not modify
	#------------------------------------------------------------------------------------------------------------------------------------------------------------------
	DbCon = sqlite3.connect ( DemandDbPath )
	DbCon.execute ( "pragma foreign_keys = on;" )
	ProcessDir = os.getcwd ()

	DbCon.execute ( "attach database '" + NetworkDbPath + "' as a;" )
	DbCon.execute ( "attach database '" + ResultDbPath + "' as b;" )


	class OD_record():
		def __init__(self,O_id, D_id, mode, avtype, count):
			self.noAV = 0
			self.l3 = 0
			self.l5 = 0
			self.tnc = 0
			self.total = 0
			self.highway = 0
			self.oid = O_id
			self.did = D_id
			self.gttime_factor = []
			self.congestion_level_by_time = []
			self.add_record(mode,avtype,count)

		def add_record(self, mode, av_type, count):
			if mode == 0 and av_type == 0:
				self.noAV += count
			elif mode == 0 and av_type == 1:
				self.l3 += count
			elif mode == 0 and av_type == 2:
				self.l5 += count
			elif mode == 9:
				self.tnc += count
			self.total += count

		def set_congestion_and_highway_flags(self, skim):
			o_idx = skim.zone_id_to_index_map[self.oid]
			d_idx = skim.zone_id_to_index_map[self.did]
			fft = skim.auto_ttime_skims[skim.intervals[0]][o_idx,d_idx]/60
			dst = skim.auto_distance_skims[skim.intervals[0]][o_idx,d_idx]/1000
			if fft == 0:
				self.highway = 0
			elif dst/fft > 80.0:
				self.highway = 1

			# get the congestion level for each timeperiod
			for i in range(0,len(skim.intervals)):
				tt = skim.auto_ttime_skims[skim.intervals[i]][o_idx,d_idx]/60
				if fft == 0:
					self.congestion_level_by_time.append(0.0)
				elif tt/fft > 1.5:
					self.congestion_level_by_time.append(1.0)
				else:
					self.congestion_level_by_time.append(0.0)

		# replicate VOTT table from Scenario documents
		def get_factor(self, vott_lvl, cong_lvl, lvl3, lvl5, hwy):
			if lvl3 + lvl5 < 1:
				return 1.0

			if vott_lvl == 0:
				if cong_lvl == 0:
					if lvl3 == 1:
						if hwy == 1:
							return 0.8
						else:
							return 1.0
					else:
						if hwy == 1:
							return 0.6
						else:
							return 0.6
				else:
					if lvl3 == 1:
						if hwy == 1:
							return 0.6
						else:
							return 1.0
					else:
						if hwy == 1:
							return 0.425
						else:
							return 0.425
			else:
				if cong_lvl == 0:
					if lvl3 == 1:
						if hwy == 1:
							return 0.95
						else:
							return 1.0
					else:
						if hwy == 1:
							return 0.8
						else:
							return 0.8
				else:
					if lvl3 == 1:
						if hwy == 1:
							return 0.85
						else:
							return 1.0
					else:
						if hwy == 1:
							return 0.6
						else:
							return 0.6

		def calculate_gttime_factor(self, skim, wait_times):
			if self.total > 0:
				self.tnc = self.tnc/self.total
				self.noAV = self.noAV/self.total
				self.l3 = self.l3/self.total
				self.l5 = self.l5/self.total
				wait = wait_times[self.oid]
			else:
				self.tnc = 0.0
				self.noAV = 1.0
				self.l3 = 0.0
				self.l5 = 0.0
				wait = wait_times[self.oid]

			o_idx = skim.zone_id_to_index_map[self.oid]
			d_idx = skim.zone_id_to_index_map[self.did]

			# determine if the OD pair is mostly highway or arterial
			self.set_congestion_and_highway_flags(skim)

			# for each skim_time period, calculate the gttime factor
			for i in range(0,len(skim.intervals)):
				tt = skim.auto_ttime_skims[skim.intervals[i]][o_idx,d_idx]
				l3_factor = self.get_factor(vot_level, self.congestion_level_by_time[i], 1,0,self.highway)
				l5_factor = self.get_factor(vot_level, self.congestion_level_by_time[i], 0,1,self.highway)
				tnc_factor = 1.0
				if tt > 0: tnc_factor = (wait + tt) / tt
				#print ('*** ', tt, l3_factor, l5_factor, tnc_factor, wait)
				self.gttime_factor.append(self.noAV + self.l3*l3_factor + self.l5*l5_factor + self.tnc*tnc_factor)

		def print(self):
			res_string = ''
			res_string += str(self.oid) + ', '
			res_string += str(self.did) + ', '
			res_string += str(self.tnc) + ', '
			res_string += str(self.noAV) + ', '
			res_string += str(self.l3) + ', '
			res_string += str(self.l5) + ', '
			for f in self.gttime_factor:
				res_string += str(f) + ', '

			print(res_string)

	class O_record(OD_record):
		def __init__(self,O_id, D_id, mode, av_type, count):
			OD_record.__init__(self,O_id, D_id, 0, 0, 0)
			self.OD_records = {}
			self.add_OD_record(O_id, D_id, mode, av_type, count)

		def add_OD_record(self, O_id, D_id, mode, av_type, count):
			if D_id in self.OD_records:
				self.OD_records[D_id].add_record(mode, avtype, count)
			else:
				self.OD_records[D_id] = OD_record(O_id, D_id, mode, avtype, count)
			# also update the origin zone level averages to fill in missing OD pairs...
			OD_record.add_record(self, mode, av_type, count)

		# do inverse distance weighting of all the destination factors which are defined for this origin to use for missing OD-pairs
		def calculate_avg_gttime_factor(self, skim):
			pass

	# skip the VOT refactoring if using baseline VOT values...
	if vot_level != 2:

		#******************************************************************************************************************************************************************
		# Get the TNC wait times by origin zone from the result database
		#------------------------------------------------------------------------------------------------------------------------------------------------------------------
		print ('Getting zonal TNC wait times...')
		query2 =  "select zone, avg(avg_wait_minutes) "
		query2 += "from b.ZoneWaitTimes "
		query2 += "group by zone order by zone;"
		ZoneRows = DbCon.execute ( query2 )

		ZoneWaitTimes = {}
		for zone, avg_wait in ZoneRows:
			ZoneWaitTimes[zone] = avg_wait
		print ('Done.')


		#******************************************************************************************************************************************************************
		# Loop over each trip record
		#------------------------------------------------------------------------------------------------------------------------------------------------------------------
		print ('Starting trip query...')
		query =  "select o.zone as o_id, d.zone as d_id, mode, t.automation_type as av_type, count(*) "
		query += "from Trip, a.location as o, a.location as d, vehicle as v, vehicle_type as t "
		query += "where (mode = 0 or mode = 9) and o.location = origin and d.location = destination and trip.vehicle = v.vehicle_id and v.type = t.type_id "
		query += "group by o_id, d_id, mode, av_type;"
		TripRows = DbCon.execute ( query )
		print ('Done.')


		#******************************************************************************************************************************************************************
		# Read the combined trip info by OD into the map
		print ('Constructing OD structure...')
		OD_info = {}
		for oid, did, mode, avtype, count in TripRows:
			if oid in OD_info:
				OD_info[oid].add_OD_record(oid,did,mode,avtype,count)
			else:
				OD_info[oid] = O_record(oid,did,mode,avtype,count)
		print ('Done.')

		#******************************************************************************************************************************************************************
		# Add the missing OD pairs using the zonal average share values
		for oid in skims.zone_id_to_index_map:
			if oid in OD_info:
				o_info = OD_info[oid]
				for did in skims.zone_id_to_index_map:
					if did not in o_info.OD_records:
						o_info.add_OD_record(oid, did, 0,0,0)
						od_rec = o_info.OD_records[did]
						od_rec.noAV = o_info.noAV
						od_rec.l3 = o_info.l3
						od_rec.l5 = o_info.l5
						od_rec.tnc = o_info.tnc
						od_rec.total = o_info.total



		print ('Processing skim travel times...')
		for oid,o in OD_info.items():
			#o.calculate_gttime_factor(skims)
			for did, d in o.OD_records.items():
				#print(oid, did, d.noAV, d.l3, d.l5, d.tnc, d.total)
				o_idx = skims.zone_id_to_index_map[oid]
				d_idx = skims.zone_id_to_index_map[did]
				# get the new factors
				d.calculate_gttime_factor(skims, ZoneWaitTimes)
				# update the skim values
				for i in range(0,len(skims.intervals)):
					tt = skims.auto_ttime_skims[skims.intervals[i]][o_idx,d_idx]
					skims.auto_ttime_skims[skims.intervals[i]][o_idx,d_idx] = tt * d.gttime_factor[i]
		print ('Done.')

	#******************************************************************************************************************************************************************
	# write the final updated skims to hdf5 and close...
	#------------------------------------------------------------------------------------------------------------------------------------------------------------------
	print ('Writing HDF5 database...')
	skim_reader.WriteSkimsHDF5(database_name + '_urbansim_skims.hdf5', skims, False)
	print ('Done.')

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Process the skim data for UrbanSim')
	parser.add_argument('-auto_skim_file', default='', help='An input auto mode skim file to read, in polaris .bin V0 or V1 format')
	parser.add_argument('-transit_skim_file', default='', help='An input transit mode skim file to read, in polaris .bin V0 or V1 format')
	parser.add_argument('-database_name', default='', help='Database name/filepath with no extension or schema indicator, i.e. "chicago", not "chicago-Supply" or chicago-Supply.sqlite')
	parser.add_argument('-vot_level', type=int, default=2, help='use 0 for VOTT low (from Scenario settings file - i.e. long run) and 1 for VOTT high (short run/less impact), 2 = no change')
	args = parser.parse_args()

	# ******************************************************************************************************************************************************************
	# SETTINGS
	# ------------------------------------------------------------------------------------------------------------------------------------------------------------------
	NetworkDbPath = args.database_name + '-Supply.sqlite'
	DemandDbPath = args.database_name + '-Demand.sqlite'
	ResultDbPath = args.database_name + '-Result.sqlite'
	auto_skim_path = args.auto_skim_file
	transit_skim_path = args.transit_skim_file
	vot_level = args.vot_level if args.vot_level != 0 else 0  # use 0 for VOTT low (from Scenario settings file - i.e. long run) and 1 for VOTT high (short run/less impact)

	postprocess_polaris_for_usim(args.database_name, NetworkDbPath, DemandDbPath, ResultDbPath, auto_skim_path, transit_skim_path, vot_level)

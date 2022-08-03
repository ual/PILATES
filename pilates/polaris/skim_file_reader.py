import sys
import os
import math
import numpy
import struct
import argparse
import csv
import h5py as h5
from operator import itemgetter
from collections import OrderedDict


######################################################################################
# Place code to work with Skims in this funcxtion
######################################################################################	
def Main(skims, highway_skim_file='', transit_skim_file='', write_bin=False, write_csv=False, write_tab=False, write_HDF5=False, origin_list=None, dest_list=None, limit_modes_list=None, limit_values_list=None):

    do_highway = GetHighwaySkims(highway_skim_file, skims)

    if not skims.silent:
        skims.print_header_info()

    do_transit = GetTransitSkims(transit_skim_file, do_highway, skims)

    if origin_list is not None: skims = ReduceTransitSkims(skims,origin_list)

    if do_highway and write_bin: WriteHighwaySkimsV1(highway_skim_file, skims)
    if do_transit and write_bin: WriteTransitSkimsV1(transit_skim_file, skims)

    if do_highway and write_csv: WriteHighwaySkimsV1_CSV(highway_skim_file, skims, origin_list, dest_list, limit_values_list)
    if do_transit and write_csv: WriteTransitSkimsV1_CSV(transit_skim_file, skims, origin_list, dest_list, limit_modes_list, limit_values_list)

    if do_highway and write_tab: WriteHighwaySkimsV1_TEXT(highway_skim_file, skims, origin_list, dest_list)
    #if do_transit and write_tab: WriteTransitSkimsV1_TEXT(ransit_skim_file, skims, origin_list, dest_list)

    if write_HDF5: WriteSkimsHDF5('test.hdf5',skims, do_transit)

def MEP(skims, highway_skim_file, transit_skim_file,):

    do_highway = GetHighwaySkims(highway_skim_file, skims)

    skims.print_header_info()

    do_transit = GetTransitSkims(transit_skim_file, do_highway, skims)


    t = skims.intervals[skims.get_interval_idx(540)]
    a_cost = skims.auto_cost_skims[t]
    t_cost = skims.transit_fare['BUS'][t]
    t_ivtt = skims.transit_ttime['BUS'][t]
    dist = skims.auto_distance_skims[t]

    mep_file = open('mep_cost_file.csv', 'w')

    # loop through all zones
    mep_file.write('taz,auto_costpermile,tran_costpermile\n')
    for kvp in skims.zone_id_to_index_map.items():
        i = kvp[0]
        idx = kvp[1]

        acost = 0.0
        adist = 0.0

        tcost = 0.0
        tdist = 0.0

        for kvp2 in skims.zone_id_to_index_map.items():
            j = kvp2[0]
            jdx = kvp2[1]

            acost += a_cost[idx,jdx]
            adist += dist[idx,jdx]

            if t_ivtt[idx,jdx] < 86400 and t_ivtt[idx,jdx] > 0 and idx != jdx:
                tcost += t_cost[idx,jdx]
                tdist += dist[idx,jdx]


        #print(tcost, tdist)
        if tdist == 0:
            tcost = 99999.0
            tdist = 1.0

        mep_file.write(str(i) + "," + str(acost/adist) + "," + str(tcost/tdist) + '\n')



######################################################################################
# Skim Functions - do not modify anything below this section
######################################################################################
def GetHighwaySkims(highway_skim_file, skims):

    if highway_skim_file == '':
        return False

    infile = open(highway_skim_file, 'rb')

    version1 = True;
    version2 = True;
    version3 = True;

    # check version tag
    if not Check_Tag(infile,"SKIM:V01"):
        version1 = False
        infile.seek(-8,1)
    else:
        skims.version = 1

    if not Check_Tag(infile,"SKIM:V02"):
        version2 = False
        infile.seek(-8,1)
    else:
        version1 = True
        version2 = True
        skims.version = 2

    if not Check_Tag(infile,"SKIM:V03"):
        version3 = False
        infile.seek(-8,1)
    else:
        version1 = True
        version2 = True
        version3 = True
        skims.version = 3

    # check for the new MODES count tag.
    if version3:
        if not Check_Tag(infile,"MODE"):
            infile.seek(-4,1)
        else:
            version3 = True
            tmodes = struct.unpack("i",infile.read(4))[0]
            skims.version = 3


    # check to see if <BZON> tag is in the file, if not read old version header information (modes,num_zones)
    if not Check_Tag(infile,"BZON"):
        infile.seek(-4,1)
        # get zones
        modes, zones = struct.unpack("ii",infile.read(8))
    # if zone tag is there, read in numzones, then the zone id-index pairs
    else:
        zones = struct.unpack("i",infile.read(4))[0]
        for i in range(zones):
            id, index = struct.unpack("ii",infile.read(8))
            if id not in skims.zone_id_to_index_map.keys():
                skims.zone_id_to_index_map[id] = index
                skims.zone_index_to_id_map[index] = id
            else:
                print("Error, zone id was found more than once in zone map.")
        Check_Tag(infile,"EZON",exit=True)

    # read intervals
    if version1:
        Check_Tag(infile,"BINT",True)
        num_intervals = struct.unpack("i",infile.read(4))[0]
        for i in range(num_intervals):
            skims.intervals.append(struct.unpack("i",infile.read(4))[0])
        Check_Tag(infile,"EINT",True)
    else:
        increment = struct.unpack("i",infile.read(4))[0] / 60
        interval = increment
        num_intervals = 0
        while interval < 1441:
            skims.intervals.append(interval)
            interval = interval + increment
            num_intervals = num_intervals + 1
    size = zones*zones
    skims.num_zones=zones

    # for each interval, check tags and read in matrix
    for i in range(num_intervals):
        if version1: Check_Tag(infile,"BMAT",True)
        data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = size))
        if data.size < size:
            print("Error: matrix not read properly for interval " + str(i) + ", copying from previous")
            skims.auto_ttime_skims[skims.intervals[i]] = skims.auto_ttime_skims[skims.intervals[i-1]]
        else:
            skims.auto_ttime_skims[skims.intervals[i]] = data.reshape(zones,zones)
        if version1: Check_Tag(infile,"EMAT",True)

        # if reading a version 2 skim, also read in the distance and cost matrices for each interval
        if version2:
            Check_Tag(infile,"BMAT",True)
            data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = size))
            if data.size < size:
                print("Error: matrix not read properly for interval " + str(i) + ", copying from previous")
                skims.auto_distance_skims[skims.intervals[i]] = skims.auto_distance_skims[skims.intervals[i-1]]
            else:
                skims.auto_distance_skims[skims.intervals[i]] = data.reshape(zones,zones)
            Check_Tag(infile,"EMAT",True)
            Check_Tag(infile,"BMAT",True)
            data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = size))
            if data.size < size:
                print( "Error: matrix not read properly for interval " + str(i) + ", copying from previous")
                skims.auto_cost_skims[skims.intervals[i]] = skims.auto_cost_skims[skims.intervals[i-1]]
            else:
                skims.auto_cost_skims[skims.intervals[i]] = data.reshape(zones,zones)
            Check_Tag(infile,"EMAT",True)

    return True

def GetTransitSkims(transit_skim_file, validate_against_highway, skims, zone_list=None):
    if transit_skim_file == '':
        return False

    infile = open(transit_skim_file, 'rb')

    version1 = True;
    version2 = True;
    version3 = False;

    # check version tag
    if not Check_Tag(infile,"SKIM:V01"):
        version1 = False
        infile.seek(-8,1)
    else:
        skims.version = 1

    if not Check_Tag(infile,"SKIM:V02"):
        version2 = False
        infile.seek(-8,1)
    else:
        version1 = True
        version2 = True
        skims.version = 2

    if not Check_Tag(infile,"SKIM:V03"):
        version3 = False
        infile.seek(-8,1)
    else:
        version1 = True
        version2 = True
        version3 = True
        skims.version = 3

    # check for the new MODES count tag.
    if version3:
        if not Check_Tag(infile,"MODE"):
            infile.seek(-4,1)
        else:
            version3 = True
            tmodes = struct.unpack("i",infile.read(4))[0]
            skims.version = 3
            if tmodes == len(skims.transit_modes)+2:
                skims.transit_modes.append('UNPNR')
            else:
                print("SOMETHING IS WRONG HERE...")

    # check to see if <BZON> tag is in the file, if not read old version header information (modes,num_zones)
    if not Check_Tag(infile,"BZON"):
        infile.seek(-4,1)
        # get zones
        tzones = struct.unpack("i",infile.read(4))[0]
    # if zone tag is there, read in numzones, then the zone id-index pairs
    else:
        tzones = struct.unpack("i",infile.read(4))[0]
        if not skims.silent:
            print( "Number of zones = " + str(tzones))
        for i in range(tzones):
            id, index = struct.unpack("ii",infile.read(8))
            if validate_against_highway:
                if id not in skims.zone_id_to_index_map.keys():
                    print( "Error: zone id " + str(id) + " found in transit file that does not exist in highway file")
                else:
                    if skims.zone_id_to_index_map[id] != index:
                        print( "Error: zone id has a different index in transit file from that found in highway file")
            else:
                if id not in skims.zone_id_to_index_map.keys():
                    skims.zone_id_to_index_map[id] = index
                    skims.zone_index_to_id_map[index] = id
                else:
                    print( "Error, zone id was found more than once in zone map.")
        #tag = struct.unpack("<4s",infile.read(4))[0]
        Check_Tag(infile,"EZON",exit=True)

    # read intervals
    if version2:
        if not skims.silent:
            print( "version 1 is true")
        Check_Tag(infile,"BINT",True)
        num_intervals = struct.unpack("i",infile.read(4))[0]
        if not skims.silent:
            print (num_intervals, '************************************************')
        for i in range(num_intervals):
            skims.transit_intervals.append(struct.unpack("i",infile.read(4))[0])
        Check_Tag(infile,"EINT",True)

    tsize = tzones*tzones
    skims.num_tzones=tzones

    if not skims.silent: print( "Reading information for " + str(tzones) + " zones. Version 1=" + str(version1) + "....")

    # for each interval, check tags and read in matrix
    for i in range(num_intervals):
        for mode in skims.transit_modes:
            if version1: Check_Tag(infile, "BMAT",True)
            data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
            if (mode == 'BUS' and skims.bus_only) or not skims.bus_only:
                if data.size < tsize: print( "Error: transit ttime matrix not read properly: " + str(data.size) + ", " + str(i) + ", " + mode)
                else: skims.transit_ttime[mode][skims.transit_intervals[i]] = data.reshape(tzones,tzones)
            if version1: Check_Tag(infile, "EMAT",True)

            if version1: Check_Tag(infile, "BMAT",True)
            data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
            if (mode == 'BUS' and skims.bus_only) or not skims.bus_only:
                if data.size < tsize: print( "Error: transit walk time matrix not read properly")
                else: skims.transit_walk_access_time[mode][skims.transit_intervals[i]] = data.reshape(tzones,tzones)
            if version1: Check_Tag(infile, "EMAT",True)

            if version1: Check_Tag(infile, "BMAT",True)
            data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
            if (mode == 'BUS' and skims.bus_only) or not skims.bus_only:
                if data.size < tsize: print( "Error: transit auto access time matrix not read properly")
                else: skims.transit_auto_access_time[mode][skims.transit_intervals[i]] = data.reshape(tzones,tzones)
            if version1: Check_Tag(infile, "EMAT",True)

            if version1: Check_Tag(infile, "BMAT",True)
            data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
            if (mode == 'BUS' and skims.bus_only) or not skims.bus_only:
                if data.size < tsize: print( "Error: transit_wait_time matrix not read properly")
                else: skims.transit_wait_time[mode][skims.transit_intervals[i]] = data.reshape(tzones,tzones)
            if version1: Check_Tag(infile, "EMAT",True)

            if version1: Check_Tag(infile, "BMAT",True)
            data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
            if (mode == 'BUS' and skims.bus_only) or not skims.bus_only:
                if data.size < tsize: print( "Error: transit_transfers matrix not read properly")
                else: skims.transit_transfers[mode][skims.transit_intervals[i]] = data.reshape(tzones,tzones)
            if version1: Check_Tag(infile, "EMAT",True)

            if version1: Check_Tag(infile, "BMAT",True)
            data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
            if (mode == 'BUS' and skims.bus_only) or not skims.bus_only:
                if data.size < tsize: print( "Error: transit_fare matrix not read properly")
                else: skims.transit_fare[mode][skims.transit_intervals[i]] = data.reshape(tzones,tzones)
            if version1: Check_Tag(infile, "EMAT",True)

    if not skims.silent: print( "Done.")
    return True

def ReduceTransitSkims(skim, zone_list):
    new_skim = Skim_Results()

    index = 0
    for zoneid in zone_list:
        if zoneid in skim.zone_id_to_index_map:
            new_skim.zone_id_to_index_map[zoneid] = index
            index += 1
        else: print( "ERROR: " + str(zoneid) + " from reduced zone list is not in original list of zones.")

    new_skim.num_tzones = len(zone_list)
    new_skim.resize_arrays(len(zone_list))

    for ozoneid in zone_list:
        for dzoneid in zone_list:
            if ozoneid not in skim.zone_id_to_index_map or dzoneid not in skim.zone_id_to_index_map or ozoneid not in new_skim.zone_id_to_index_map or dzoneid not in new_skim.zone_id_to_index_map:
                print( "ERROR: " + str(zoneid) + " from reduced zone list is not in original list of zones.")
                sys.exit()

            o_idx_orig = skim.zone_id_to_index_map[ozoneid]
            d_idx_orig = skim.zone_id_to_index_map[dzoneid]
            o_idx = new_skim.zone_id_to_index_map[ozoneid]
            d_idx = new_skim.zone_id_to_index_map[dzoneid]

            new_skim.transit_ttime[o_idx,d_idx] = skim.transit_ttime[o_idx_orig,d_idx_orig]
            new_skim.transit_walk_access_time[o_idx,d_idx] = skim.transit_walk_access_time[o_idx_orig,d_idx_orig]
            new_skim.transit_wait_time[o_idx,d_idx] = skim.transit_wait_time[o_idx_orig,d_idx_orig]
            new_skim.transit_fare[o_idx,d_idx] = skim.transit_fare[o_idx_orig,d_idx_orig]
            new_skim.auto_distance[o_idx,d_idx] = skim.auto_distance[o_idx_orig,d_idx_orig]

    return new_skim

def Check_Tag(file, check_val, exit=False):
    size = len(check_val)
    read_val = file.read(size)

    # check if at end of file
    if len(read_val) != size: return False

    # if not, read in tag
    tag = struct.unpack("<" + str(size) + "s",read_val)[0]

    # check against expected value and exit if requested
    if tag.decode("utf-8") != check_val:
        if exit:
            print( "Error: " + check_val + " tag missing. Read as: " + tag.decode("utf-8"))
            sys.exit()
        else:
            print( "Warning: tag '" + check_val + " was missing. Read as: " + tag.decode("utf-8"))
            return False
    else:
        return True

def WriteHighwaySkimsV1(highway_skim_file, skims):
    outfile = open(highway_skim_file, 'wb')

    # Write version info
    outfile.write(struct.pack("<8s","SKIM:V01"))

    # Write zone identification info if available, othwerise write old-style zone info
    if len(skims.zone_id_to_index_map) > 0:
        outfile.write(struct.pack("<4s","BZON"))
        outfile.write(struct.pack("i",skims.num_zones))
        for kvp in skims.zone_id_to_index_map.items():
            outfile.write(struct.pack("ii",kvp[0], kvp[1]))
        outfile.write(struct.pack("<4s","EZON"))
    else:
        outfile.write(struct.pack("ii",1, skims.num_zones))

    # Write intervals
    outfile.write(struct.pack("<4s","BINT"))
    outfile.write(struct.pack("i",len(skims.auto_ttime_skims)))
    for interval in sorted(skims.auto_ttime_skims.keys()):
        outfile.write(struct.pack("i",interval))
    outfile.write(struct.pack("<4s","EINT"))

    # Write skim matrix for each interval
    for interval in sorted(skims.auto_ttime_skims.keys()):
        outfile.write(struct.pack("<4s","BMAT"))
        skims.auto_ttime_skims[interval].tofile(outfile)
        outfile.write(struct.pack("<4s","EMAT"))

def WriteTransitSkimsV1(transit_skim_file, skims):
    outfile = open(transit_skim_file, 'wb')

    vtag = struct.pack("<8s","SKIM:V01")
    outfile.write(vtag)

    # Write zone identification info if available, othwerise write old-style zone info
    if len(skims.zone_id_to_index_map) > 0:
        outfile.write(struct.pack("<4s","BZON"))
        outfile.write(struct.pack("i",skims.num_tzones))
        if not skims.silent: print( "Transit zones = " + str(skims.num_tzones))
        for kvp in skims.zone_id_to_index_map.items():
            outfile.write(struct.pack("ii",kvp[0], kvp[1]))
        outfile.write(struct.pack("<4s","EZON"))
    else:
        outfile.write(struct.pack("ii",1, skims.num_tzones))

    btag = struct.pack("<4s","BMAT")
    etag = struct.pack("<4s","EMAT")

    outfile.write(btag)
    skims.transit_ttime.tofile(outfile)
    outfile.write(etag)

    outfile.write(btag)
    skims.transit_walk_access_time.tofile(outfile)
    outfile.write(etag)

    outfile.write(btag)
    skims.auto_distance.tofile(outfile)
    outfile.write(etag)

    outfile.write(btag)
    skims.transit_wait_time.tofile(outfile)
    outfile.write(etag)

    outfile.write(btag)
    skims.transit_fare.tofile(outfile)
    outfile.write(etag)

def WriteHighwaySkimsV1_CSV(highway_skim_file, skims, origin_list=None, dest_list=None, limit_values_list=None):
    with open(highway_skim_file + '.csv', 'w') as outfile:

        # Update the origin and destination lists
        if origin_list is None: origin_list = sorted(skims.zone_id_to_index_map.keys())
        if dest_list is None: dest_list = sorted(skims.zone_id_to_index_map.keys())

        # Write intervals
        for interval in sorted(skims.auto_ttime_skims.keys()):
            outfile.write("Auto TTime for interval" + str(interval) + '\n')
            # Write skim matrix for each interval
            outfile.write(',')

            # write destination zone headers (if in dest list)
            for i in dest_list:
                outfile.write(str(i) + ',')
            outfile.write('\n')

            for i in origin_list:
                outfile.write(str(i) + ',')
                for j in dest_list:
                    i_id = skims.zone_id_to_index_map[i]
                    j_id = skims.zone_id_to_index_map[j]
                    outfile.write(str(skims.auto_ttime_skims[interval][i_id,j_id]) + ',')
                outfile.write('\n')
            outfile.write('\n')

            # output the distance and cost if it is version 2...
            if skims.version == 2:
                outfile.write("Auto Distance (m) for interval" + str(interval) + '\n')
                outfile.write(',')
                # write destination zone headers (if in dest list)
                for i in dest_list:
                    outfile.write(str(i) + ',')
                outfile.write('\n')

                for i in origin_list:
                    outfile.write(str(i) + ',')
                    for j in dest_list:
                        i_id = skims.zone_id_to_index_map[i]
                        j_id = skims.zone_id_to_index_map[j]
                        outfile.write(str(skims.auto_distance_skims[interval][i_id,j_id]) + ',')
                    outfile.write('\n')
                outfile.write('\n')

                outfile.write("Auto Cost ($) for interval" + str(interval) + '\n')
                outfile.write(',')
                # write destination zone headers (if in dest list)
                for i in dest_list:
                    outfile.write(str(i) + ',')
                outfile.write('\n')

                for i in origin_list:
                    outfile.write(str(i) + ',')
                    for j in dest_list:
                        i_id = skims.zone_id_to_index_map[i]
                        j_id = skims.zone_id_to_index_map[j]
                        outfile.write(str(skims.auto_cost_skims[interval][i_id,j_id]) + ',')
                    outfile.write('\n')
                outfile.write('\n')
        outfile.write('\n')

def WriteHighwaySkimsV1_TEXT(highway_skim_file, skims, origin_list=None, dest_list=None):
    with open(highway_skim_file + '.csv', 'w') as outfile:

        # Update the origin and destination lists
        if origin_list is None: origin_list = sorted(skims.zone_id_to_index_map.keys())
        if dest_list is None: dest_list = sorted(skims.zone_id_to_index_map.keys())

        # Write intervals
        outfile.write('O,D,')
        for interval in sorted(skims.auto_ttime_skims.keys()):
            outfile.write(str(interval) + ',')
        # Write skim matrix for each interval
        outfile.write('\n')


        for i in origin_list:
            for j in dest_list:
                i_id = skims.zone_id_to_index_map[i]
                j_id = skims.zone_id_to_index_map[j]
                outfile.write(str(i) + ',' + str(j) + ',')
                for interval in sorted(skims.auto_ttime_skims.keys()):
                     outfile.write(str(skims.auto_ttime_skims[interval][i_id,j_id]) + ',')
                outfile.write('\n')

def ConvertTransitToV1(transit_skim_file, skims,zone_id_to_index):
    if transit_skim_file == '':
        return False

    # get the zone map
    with open(zone_id_to_index, 'r') as infile:
        cr = csv.reader(infile,delimiter =',')
        for row in cr:
            skims.zone_id_to_index_map[int(row[0])] = int(row[1])
            skims.zone_index_to_id_map[int(row[1])] = int(row[0])

    infile = open(transit_skim_file, 'rb')

    tzones = struct.unpack("i",infile.read(4))[0]

    tsize = tzones*tzones
    skims.num_tzones=tzones

    if not skims.silent: print( "Reading information for " + str(tzones) + " zones....")

    data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
    if data.size < tsize: print( "Error: transit ttime matrix not read properly")
    else: skims.transit_ttime = data.reshape(tzones,tzones)

    data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
    if data.size < tsize: print( "Error: transit walk time matrix not read properly")
    else: skims.transit_walk_access_time = data.reshape(tzones,tzones)

    data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
    if data.size < tsize: print( "Error: auto distance matrix not read properly")
    else: skims.auto_distance = data.reshape(tzones,tzones)

    data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
    if data.size < tsize: print( "Error: transit_wait_time matrix not read properly")
    else: skims.transit_wait_time = data.reshape(tzones,tzones)

    data = numpy.matrix(numpy.fromfile(infile, dtype='f',count = tsize))
    if data.size < tsize: print( "Error: transit_fare matrix not read properly")
    else: skims.transit_fare = data.reshape(tzones,tzones)

    if not skims.silent: print( "Done.")
    return True

def WriteTransitSkimsV1_CSV(transit_skim_file, skims, origin_list=None, dest_list=None, limit_modes_list=None, limit_values_list=None):
    outfile = open(transit_skim_file + '.csv', 'w')

    # Update the origin and destination lists
    if origin_list is None: origin_list = sorted(skims.zone_id_to_index_map.keys())
    if dest_list is None: dest_list = sorted(skims.zone_id_to_index_map.keys())

    # Write intervals
    for interval in sorted(skims.transit_intervals):
        #outfile.write("Skim for interval" + str(interval) + '\n')

        for mode in skims.transit_modes:
            if limit_modes_list and mode not in limit_modes_list : continue

            # Write TTime
            if not limit_values_list or 'IVTT' in limit_values_list:
                outfile.write(mode)
                outfile.write(" Transit IVTTime Skim @")
                outfile.write(str(interval) + '\n')
                outfile.write(',')
                for i in dest_list:
                    outfile.write(str(i) + ',')
                outfile.write('\n')
                for i in origin_list:
                    outfile.write(str(i) + ',')
                    for j in dest_list:
                        i_id = skims.zone_id_to_index_map[i]
                        j_id = skims.zone_id_to_index_map[j]
                        outfile.write(str(skims.transit_ttime[mode][interval][i_id,j_id]) + ',')
                    outfile.write('\n')
                outfile.write('\n')

            # Write walk time
            if not limit_values_list or 'WALK_OVTT' in limit_values_list:
                outfile.write(mode + " Transit Walk Access Time Skim @" + str(interval) + '\n')
                outfile.write(',')
                for i in dest_list:
                    outfile.write(str(i) + ',')
                outfile.write('\n')
                for i in origin_list:
                    outfile.write(str(i) + ',')
                    for j in dest_list:
                        i_id = skims.zone_id_to_index_map[i]
                        j_id = skims.zone_id_to_index_map[j]
                        outfile.write(str(skims.transit_walk_access_time[mode][interval][i_id,j_id]) + ',')
                    outfile.write('\n')
                outfile.write('\n')

            # Write auto access time
            if not limit_values_list or 'AUTO_OVTT' in limit_values_list:
                outfile.write(mode + " Transit Auto Access Time Skim @" + str(interval) + '\n')
                outfile.write(',')
                for i in dest_list:
                    outfile.write(str(i) + ',')
                outfile.write('\n')
                for i in origin_list:
                    outfile.write(str(i) + ',')
                    for j in dest_list:
                        i_id = skims.zone_id_to_index_map[i]
                        j_id = skims.zone_id_to_index_map[j]
                        outfile.write(str(skims.transit_auto_access_time[mode][interval][i_id,j_id]) + ',')
                    outfile.write('\n')
                outfile.write('\n')

            # Write wait time
            if not limit_values_list or 'WAIT' in limit_values_list:
                outfile.write(mode + " Transit Wait Time Skim @" + str(interval) + '\n')
                outfile.write(',')
                for i in dest_list:
                    outfile.write(str(i) + ',')
                outfile.write('\n')
                for i in origin_list:
                    outfile.write(str(i) + ',')
                    for j in dest_list:
                        i_id = skims.zone_id_to_index_map[i]
                        j_id = skims.zone_id_to_index_map[j]
                        outfile.write(str(skims.transit_wait_time[mode][interval][i_id,j_id]) + ',')
                    outfile.write('\n')
                outfile.write('\n')

            # Write transfers
            if not limit_values_list or 'TRANSFERS' in limit_values_list:
                outfile.write(mode + " Transit Transfers Skim @" + str(interval) + '\n')
                outfile.write(',')
                for i in dest_list:
                    outfile.write(str(i) + ',')
                outfile.write('\n')
                for i in origin_list:
                    outfile.write(str(i) + ',')
                    for j in dest_list:
                        i_id = skims.zone_id_to_index_map[i]
                        j_id = skims.zone_id_to_index_map[j]
                        outfile.write(str(skims.transit_transfers[mode][interval][i_id,j_id]) + ',')
                    outfile.write('\n')
                outfile.write('\n')

            # Write Fare
            if not limit_values_list or 'COST' in limit_values_list:
                outfile.write(mode + " Transit Fare Skim @" + str(interval) + '\n')
                outfile.write(',')
                for i in dest_list:
                    outfile.write(str(i) + ',')
                outfile.write('\n')
                for i in origin_list:
                    outfile.write(str(i) + ',')
                    for j in dest_list:
                        i_id = skims.zone_id_to_index_map[i]
                        j_id = skims.zone_id_to_index_map[j]
                        outfile.write(str(skims.transit_fare[mode][interval][i_id,j_id]) + ',')
                    outfile.write('\n')
                outfile.write('\n')

def WriteSkimsHDF5(filename, skims, do_transit):
    f = h5.File(filename, 'w')

    # get zone ids in order and convert to numpy array
    ids = numpy.array([])
    for i in range(0,skims.num_zones-1):
        ids = numpy.append(ids, skims.zone_index_to_id_map[i])
    zones = f.create_dataset("zone_ids", data = ids)

    # get interval start times and convert to numpy array
    ints = numpy.array([])
    for i in sorted(skims.intervals):
        ints = numpy.append(ints, i)
    intervals = f.create_dataset("interval_end_minutes", data = ints)

    # create the auto and transit groups
    auto_group = f.create_group('auto_skims')
    transit_groups = {}
    if do_transit:
        for s in skims.transit_modes:
            transit_groups[s] = f.create_group(s.lower() + '_skims')

    j = 0
    for i in sorted(skims.intervals):
        ai = auto_group.create_group('t' + str(j))
        aii = ai.create_dataset('ivtt',data=skims.auto_ttime_skims[i])
        aic = ai.create_dataset('cost',data=skims.auto_cost_skims[i])
        aid = ai.create_dataset('distance',data=skims.auto_distance_skims[i])

        if do_transit:
            for m in skims.transit_modes:
                ti = transit_groups[m].create_group('t' + str(j))
                tii = ti.create_dataset('ivtt',data=skims.transit_ttime[m][i])
                tio = ti.create_dataset('walk_ovtt',data=skims.transit_walk_access_time[m][i])
                tia = ti.create_dataset('auto_ovtt',data=skims.transit_auto_access_time[m][i])
                tiw = ti.create_dataset('wait',data=skims.transit_wait_time[m][i])
                tit = ti.create_dataset('transfers',data=skims.transit_transfers[m][i])
                tif = ti.create_dataset('fare',data=skims.transit_fare[m][i])

        j += 1



#note transit_walk_files and transit_wait_files are lists - as there may be multiple wait/walk input files which are aggregated to one value
def ReadTransitSkims_CSV(transit_ttime_file, transit_walk_files, transit_wait_files, transit_fare_file, auto_distance_file, zone_id_to_index, skims):
    # get the zone map
    with open(zone_id_to_index, 'r') as infile:
        cr = csv.reader(infile,delimiter =',')
        for row in cr:
            skims.zone_id_to_index_map[int(row[0])] = int(row[1])
            skims.zone_index_to_id_map[int(row[1])] = int(row[0])

    nzone = len(skims.zone_id_to_index_map)
    skims.num_tzones = nzone

    # resize the matrices according to number of zones
    skims.resize_arrays(nzone)

    ignored_zones = []

    # read in each individual matrix and store_const
    delim = ','
    if '.txt' in transit_ttime_file: delim = '\t'
    else: delim = ','

    with open(transit_ttime_file, 'r') as infile:
        cr = csv.reader(infile,delimiter =delim)
        try:
            for row in cr:
                if len(row) > 2:
                    if int(row[0]) not in skims.zone_id_to_index_map:
                        if row[0] not in ignored_zones: ignored_zones.append(row[0])
                    elif int(row[1]) not in skims.zone_id_to_index_map:
                        if row[1] not in ignored_zones: ignored_zones.append(row[1])
                    else:
                        o = skims.zone_id_to_index_map[int(row[0])]
                        d = skims.zone_id_to_index_map[int(row[1])]
                        skims.transit_ttime[o,d] = float(row[2])
        except ValueError:
            print( row[2])

    for transit_walk_file in transit_walk_files:
        if '.txt' in transit_walk_file: delim = '\t'
        else: delim = ','
        with open(transit_walk_file, 'r') as infile:
            cr = csv.reader(infile,delimiter =delim)
            try:
                for row in cr:
                    if len(row) > 2:
                        if int(row[0]) not in skims.zone_id_to_index_map:
                            if row[0] not in ignored_zones: ignored_zones.append(row[0])
                        elif int(row[1]) not in skims.zone_id_to_index_map:
                            if row[1] not in ignored_zones: ignored_zones.append(row[1])
                        else:
                            o = skims.zone_id_to_index_map[int(row[0])]
                            d = skims.zone_id_to_index_map[int(row[1])]
                            skims.transit_walk_access_time[o,d] += float(row[2])
            except ValueError:
                    print( row[2])

    for transit_wait_file in transit_wait_files:
        if '.txt' in transit_wait_file: delim = '\t'
        else: delim = ','
        with open(transit_wait_file, 'r') as infile:
            try:
                cr = csv.reader(infile,delimiter =delim)
                for row in cr:
                    if len(row) > 2:
                        if int(row[0]) not in skims.zone_id_to_index_map:
                            if row[0] not in ignored_zones: ignored_zones.append(row[0])
                        elif int(row[1]) not in skims.zone_id_to_index_map:
                            if row[1] not in ignored_zones: ignored_zones.append(row[1])
                        else:
                            o = skims.zone_id_to_index_map[int(row[0])]
                            d = skims.zone_id_to_index_map[int(row[1])]
                            skims.transit_wait_time[o,d] += float(row[2])
            except ValueError:
                print( row[2])

    if '.txt' in transit_wait_file: delim = '\t'
    else: delim = ','
    with open(transit_fare_file, 'r') as infile:
        cr = csv.reader(infile,delimiter =delim)
        try:
            for row in cr:
                if len(row) > 2:
                    if int(row[0]) not in skims.zone_id_to_index_map:
                        if row[0] not in ignored_zones: ignored_zones.append(row[0])
                    elif int(row[1]) not in skims.zone_id_to_index_map:
                        if row[1] not in ignored_zones: ignored_zones.append(row[1])
                    else:
                        o = skims.zone_id_to_index_map[int(row[0])]
                        d = skims.zone_id_to_index_map[int(row[1])]
                        skims.transit_fare[o,d] = float(row[2])
        except ValueError:
                print( row[2])

    if '.txt' in transit_wait_file: delim = '\t'
    else: delim = ','
    with open(auto_distance_file, 'r') as infile:
        cr = csv.reader(infile,delimiter =delim)
        try:
            for row in cr:
                if len(row) > 2:
                    if int(row[0]) not in skims.zone_id_to_index_map:
                        if row[0] not in ignored_zones: ignored_zones.append(row[0])
                    elif int(row[1]) not in skims.zone_id_to_index_map:
                        if row[1] not in ignored_zones: ignored_zones.append(row[1])
                    else:
                        o = skims.zone_id_to_index_map[int(row[0])]
                        d = skims.zone_id_to_index_map[int(row[1])]
                        skims.auto_distance[o,d] = float(row[2])
        except ValueError:
                print( row[2])

    print( "Warning, the following tazs from skim .csv input were ignored as they are not in the skim_id_to_index_file:")
    for id in ignored_zones: print( str(id))

#note highway travel time files and intervals are lists
def ReadHighwaySkims_CSV(highway_ttime_files, intervals, zone_id_to_index, skims):
    print( "Reading highway skims from csv files...")

    # get the zone map
    with open(zone_id_to_index, 'r') as infile:
        cr = csv.reader(infile,delimiter =',')
        for row in cr:
            skims.zone_id_to_index_map[int(row[0])] = int(row[1])
            skims.zone_index_to_id_map[int(row[1])] = int(row[0])

    nzone = len(skims.zone_id_to_index_map)
    skims.num_zones = nzone
    skims.num_tzones = nzone

    # get the intervals
    for interval in intervals: skims.intervals.append(interval)
    print( "Time intervals: " + str(intervals))

    # resize the matrices according to number of zones and add for each interval
    print( "Resizing matrice...")
    skims.resize_arrays(nzone)

    ignored_zones = []

    interval_count = 0
    for highway_ttime_file in highway_ttime_files:
        interval = skims.intervals[interval_count]
        read_count = 0
        print( "Reading data for interval ending in " + str(interval))
        with open(highway_ttime_file, 'r') as infile:
            cr = csv.reader(infile,delimiter =',')
            try:
                for row in cr:
                    if len(row) > 2:
                        if int(row[0]) not in skims.zone_id_to_index_map:
                            if row[0] not in ignored_zones: ignored_zones.append(row[0])
                        elif int(row[1]) not in skims.zone_id_to_index_map:
                            if row[1] not in ignored_zones: ignored_zones.append(row[1])
                        else:
                            o = skims.zone_id_to_index_map[int(row[0])]
                            d = skims.zone_id_to_index_map[int(row[1])]
                            skims.auto_ttime_skims[interval][o,d] += float(row[2])
                        read_count = read_count + 1
                    if read_count % (nzone*50) == 0:
                        print( str("Reading for interval {} is {:.2%} complete.").format(interval, float(read_count) / float(nzone) / float(nzone) ))
            except ValueError:
                    print( row[2])
            interval_count = interval_count + 1
    print( "Warning, the following tazs from skim .csv input were ignored as they are not in the skim_id_to_index_file:")
    for id in ignored_zones: print( str(id))

class Skim_Results:
    def __init__(self, silent=False):
        self.version = 0
        self.auto_ttime_skims = {}
        self.auto_cost_skims = {}
        self.auto_distance_skims = {}
        self.zone_id_to_index_map = {}
        self.zone_index_to_id_map = {}
        self.transit_ttime = {}
        self.transit_walk_access_time = {}
        self.transit_auto_access_time = {}
        self.transit_wait_time = {}
        self.transit_transfers = {}
        self.transit_fare = {}
        self.num_zones=0
        self.num_tzones=0
        self.transit_modes = ['BUS', 'RAIL', 'PNR', 'PNRAIL', 'UNPNR']
        self.bus_only=False
        self.intervals=[]
        self.transit_intervals=[]
        self.silent = silent
        for mode in self.transit_modes:
            self.transit_ttime[mode]={}
            self.transit_walk_access_time[mode] = {}
            self.transit_auto_access_time[mode] = {}
            self.transit_wait_time[mode] = {}
            self.transit_transfers[mode] = {}
            self.transit_fare[mode] = {}
    def get_interval_idx(self, time):
        idx = 0
        for i in self.intervals:
            if time < i: return idx
            else: idx += 1
        return len(self.intervals) - 1
    def get_transit_interval_idx(self, time):
        idx = 0
        for i in self.transit_intervals:
            if time < i: return idx
            else: idx += 1
        return len(self.transit_intervals) - 1
    def print_header_info(self):
        if self.silent:
            return
        print( "Zone info (id, index):")
        for kvp in self.zone_id_to_index_map.items():
            print( kvp[0], kvp[1])
        print( "\r\nInterval info:")
        print( "Number of intervals = " + str(len(self.auto_ttime_skims.keys())) + ":")
        for interval in sorted(self.auto_ttime_skims.keys()):
            print( interval)
        print( "\r\n")
    def print_OD_info(self, o_idx, d_idx, time, do_auto, do_transit, header=True):
        i = self.get_interval_idx(time)
        j = self.get_transit_interval_idx(time)

        #print( i, o_idx, d_idx)
        if o_idx < len(self.zone_id_to_index_map) and d_idx < len(self.zone_id_to_index_map):
            if header: s = "Time, Mode, IVTT, Walk_OVTT, Auto_OVTT, Wait_Time, Transfer, Fare\r\n"
            else: s=""
            if do_auto: s += str(time) + ", Auto," + str(self.auto_ttime_skims[self.intervals[i]][o_idx,d_idx]) + ","
            else: s += "Auto, NA,"
            if do_transit:
                interval = self.transit_intervals[j]
                for mode in self.transit_modes:
                    if (mode == 'BUS' and self.bus_only) or not self.bus_only:
                        s += (mode + "," + str(self.transit_ttime[mode][interval][o_idx,d_idx]) +
                        "," + str(self.transit_walk_access_time[mode][interval][o_idx,d_idx]) +
                        "," + str(self.transit_auto_access_time[mode][interval][o_idx,d_idx]) +
                        "," + str(self.transit_wait_time[mode][interval][o_idx,d_idx]) +
                        "," + str(self.transit_transfers[mode][interval][o_idx,d_idx]) +
                        "," + str(self.transit_fare[mode][interval][o_idx,d_idx]) + ",")
            else: s += "TRANSIT,NA,NA,NA,NA,NA,NA"
            return s
    def print_skims(self):
        for interval in sorted(self.auto_ttime_skims.keys()):
            print( 'Skim end=' + str(interval))
            if len(self.auto_ttime_skims[interval])>0: print( self.auto_ttime_skims[interval])
            print( '')

        print( 'transit_ttime')
        if self.transit_ttime.size>0: print( self.transit_ttime)
        print( '')
        print( 'transit_walk_access_time')
        if self.transit_walk_access_time.size>0: print( self.transit_walk_access_time)
        print( '')
        print( 'auto_distance')
        if self.auto_distance.size>0: print( self.auto_distance)
        print( '')
        print( 'transit_wait_time')
        if self.transit_wait_time.size>0: print( self.transit_wait_time)
        print( '')
        print( 'transit_fare')
        if self.transit_fare.size>0: print( self.transit_fare)
    def resize_arrays(self, nzone):
        self.transit_ttime = numpy.resize(self.transit_ttime,(nzone,nzone))
        self.transit_walk_access_time = numpy.resize(self.transit_walk_access_time,(nzone,nzone))
        self.transit_wait_time = numpy.resize(self.transit_wait_time,(nzone,nzone))
        self.transit_fare = numpy.resize(self.transit_fare,(nzone,nzone))
        self.auto_distance = numpy.resize(self.auto_distance,(nzone,nzone))

        for interval in self.intervals:
            m = numpy.matrix(0,dtype='f')
            m = numpy.resize(m,(nzone,nzone))
            self.auto_ttime_skims[interval] = m


if __name__ == "__main__":

    skims = Skim_Results()

    # parse the command line args
    parser = argparse.ArgumentParser(description='Process the skim data')
    parser.add_argument('-auto_skim_file', default='', help='An input auto mode skim file to read, in polaris .bin V0 or V1 format')
    parser.add_argument('-transit_skim_file', default='', help='An input transit mode skim file to read, in polaris .bin V0 or V1 format')
    parser.add_argument('-csv', action='store_const', const=1, help='Write CSV output flag')
    parser.add_argument('-tab', action='store_const', const=1, help='Write tab-delimited output flag')
    parser.add_argument('-bin', action='store_const', const=1, help='Write binary output flag')
    parser.add_argument('-hdf5', action='store_const', const=1, help='Write HDF5 output flag')
    parser.add_argument('-origin_list', type=int, nargs='*', help='A list of origin zone IDs used to generate a sub-skim file for only those zones')
    parser.add_argument('-destination_list', type=int, nargs='*', help='A list of destination zone IDs used to generate a sub-skim file for only those zones')
    parser.add_argument('-read_from_csv', action='store_true', help='Flag to indicate that skims will be read and created from  CSV input files, rather than from Polaris format. Automatically sets -bin and -csv to true. Requires -auto_skim_file and/or transit_skim_file to be provided as output')
    parser.add_argument('-convert_to_v1', action='store_true', help='Flag to indicate input skims are in V0 and need to convert to V1')
    parser.add_argument('-interactive', action='store_true', help='Flag to start interactive mode allowing for travel time requests between OD pairs')
    parser.add_argument('-batch', action='store_true', help='Flag to start batch mode allowing for travel time requests between multiple OD pairs, defined in trip_file')
    parser.add_argument('-trip_file', default='', help='CSV file of O,D,departure_times for use in batch mode.')
    parser.add_argument('-i1_transit_ttime_file', default = '', help='Input transit skim csv file. One file describing the transit in-vehicle time for trip from O-D.')
    parser.add_argument('-i2_transit_walk_files', nargs='*', help='Input transit skim csv file. One or more files describing the walking time components for trip from O-D.')
    parser.add_argument('-i3_transit_wait_files', nargs='*', help='Input transit skim csv file. One or more files describing the waiting time components for trip from O-D.')
    parser.add_argument('-i4_transit_fare_file', default = '', help='Input transit skim csv file. One ofile describing the fare for trip from O-D.')
    parser.add_argument('-i5_auto_distance_file',default='', help='Input transit skim csv file. One file describing the auto distance for trip from O-D.')
    parser.add_argument('-zone_id_to_index_file', default = '', help='Map of zone ids to zone indexes. Required for skim file creation from csv files.')
    parser.add_argument('-i6_highway_ttime_files',nargs='*', help='Input csv file, required if read_from_csv to be true and no i*_transit files are specified.  One csv file for each skim time interval, with each row in "O,D,ttime" format.')
    parser.add_argument('-i7_highway_intervals',nargs='*',type=int, help='Defines the end times of the skim intervals for which the -i6_highway_ttime_files were created, one interval per file required.')
    parser.add_argument('-define_intervals',action='store_true', help='Flag to indicate that the intervals defined in auto_skim_fil will be altered to those defined in i7_highway_intervals.')
    parser.add_argument('-bus_only',action='store_true', help='flag to read only the bus+rail skims in order to save memory...')
    parser.add_argument('-limit_modes',nargs='*', help='Limit output to the specified modes - list of RAIL, BUS, PNR, PNRAIL')
    parser.add_argument('-limit_values',nargs='*', help='Limit output to the specified values - list of IVTT, WALK_OVTT, AUTO_OVTT, COST, TRANSFERS, WAIT, DISTANCE')
    parser.add_argument('-mep', action='store_true', help='Dump MEP information.')


    args = parser.parse_args()

    write_csv=False
    if args.csv==1: write_csv=True
    write_hdf5=False
    if args.hdf5==1: write_hdf5=True
    write_bin=False
    if args.bin==1: write_bin=True
    write_tab=False
    if args.tab==1: write_tab=True
    if write_csv==False and write_tab==False and write_hdf5==False: write_bin=True
    skims.bus_only = args.bus_only

    if (args.read_from_csv):
        write_bin = True
        write_csv = True
        if args.zone_id_to_index_file == '': raise NameError("Error: missing zone_id_to_index file")

        if args.i6_highway_ttime_files is not None:
            ReadHighwaySkims_CSV(args.i6_highway_ttime_files,args.i7_highway_intervals,args.zone_id_to_index_file,skims)
            WriteHighwaySkimsV1(args.auto_skim_file,skims)
            WriteHighwaySkimsV1_CSV(args.auto_skim_file,skims)

        if args.i1_transit_ttime_file != '':
            ReadTransitSkims_CSV(args.i1_transit_ttime_file,args.i2_transit_walk_files,args.i3_transit_wait_files,args.i4_transit_fare_file,args.i5_auto_distance_file, args.zone_id_to_index_file,skims)
            WriteTransitSkimsV1(args.transit_skim_file,skims)
            WriteTransitSkimsV1_CSV(args.transit_skim_file,skims)

    elif (args.convert_to_v1):
        write_bin = True
        write_csv = True
        if args.zone_id_to_index_file == '': raise NameError("Error: missing zone_id_to_index file")
        if args.auto_skim_file != '': raise NameError("Error: v0 to v1 converter not implemented for auto skims")
        if args.transit_skim_file != '':
            do_transit = ConvertTransitToV1(args.transit_skim_file,skims, args.zone_id_to_index_file)
            if do_transit: WriteTransitSkimsV1(args.transit_skim_file+"_v1", skims)
            if do_transit: WriteTransitSkimsV1_CSV(args.transit_skim_file+"_v1", skims)

    elif (args.interactive):

        Main(skims, args.auto_skim_file, args.transit_skim_file, False, False, False, False, None, None)

        do_auto = args.auto_skim_file != ''
        do_transit = args.transit_skim_file != ''

        s = ''
        while (s != 'q'):
            multiple_times = False
            s = input('Enter an O,D,time tuple (or q to exit): ')
            OD_s = s.split(',')
            if len(OD_s) == 2:
                try:
                    O = int(OD_s[0])
                    D = int(OD_s[1])
                    multiple_times = True
                except ValueError:
                    print( "Error: enter a valid OD pair seperated by a comma")
                    continue
            elif len(OD_s) == 3:
                try:
                    O = int(OD_s[0])
                    D = int(OD_s[1])
                    T = int(OD_s[2])
                except ValueError:
                    print( "Error: enter a valid OD pair seperated by a comma")
                    continue
            else:
                print( "Error: enter an OD pair and time seperated by a comma")
                continue
            if O not in skims.zone_id_to_index_map:
                print( "Origin ID '" + str(O) + "' not found.")
                continue
            if D not in skims.zone_id_to_index_map:
                print( "Destination ID '" + str(D) + "' not found.")
                continue
            O_idx = skims.zone_id_to_index_map[int(O)]
            D_idx = skims.zone_id_to_index_map[int(D)]
            if O_idx is None: raise NameError("Error: Origin zone id '" + O + "' not found.")
            if D_idx is None: raise NameError("Error: Destination zone id '" + D + "' not found.")

            if multiple_times:
                for t in skims.intervals:
                    print( skims.print_OD_info(O_idx, D_idx, t, do_auto, do_transit,False))
            else:
                print( skims.print_OD_info(O_idx, D_idx, T, do_auto, do_transit))

    elif (args.batch):
        Main(skims, args.auto_skim_file, args.transit_skim_file, False, False, False, False, None, None)
        do_auto = args.auto_skim_file != ''
        do_transit = args.transit_skim_file != ''

        with open(args.trip_file[:-4] + '_out.csv', 'wb') as outfile:
            outfile.write('ID,O,D,Time,AutoTime,TransitTime,walk_time,wait_time,fare,dist\r\n')
            with open(args.trip_file, 'r') as infile:
                    cr = csv.reader(infile,delimiter =',')
                    cr.next()
                    try:
                        for row in cr:
                            if len(row) == 3:
                                id_offset = 0
                                ID = '1'
                            if len(row) != 4:
                                print( "Error: input must be in 'ID,O,D,departure_time' format.")
                                continue
                            else:
                                id_offset = 1
                                ID = row[0]
                            try:
                                O = int(row[0+id_offset])
                                D = int(row[1+id_offset])
                                T = int(row[2+id_offset])

                                if O not in skims.zone_id_to_index_map:
                                    print( "Origin ID '" + str(O) + "' not found.")
                                    continue
                                if D not in skims.zone_id_to_index_map:
                                    print( "Destination ID '" + str(D) + "' not found.")
                                    continue
                                O_idx = skims.zone_id_to_index_map[int(O)]
                                D_idx = skims.zone_id_to_index_map[int(D)]
                                if O_idx is None: raise NameError("Error: Origin zone id '" + O + "' not found.")
                                if D_idx is None: raise NameError("Error: Destination zone id '" + D + "' not found.")
                                outfile.write(ID + ',' + row[0+id_offset] + ',' + row[1+id_offset] + ',' + row[2+id_offset] + ',' + skims.print_OD_info(O_idx, D_idx, T, do_auto, do_transit,False) + '\r\n' )
                            except ValueError:
                                print( "Error: enter a valid OD pair seperated by a comma: " + str(row))
                                continue

                    except ValueError:
                            print( row)

    elif (args.mep):
        MEP(skims, args.auto_skim_file, args.transit_skim_file)

    else:
        Main(skims, args.auto_skim_file, args.transit_skim_file, write_bin, write_csv, write_tab, write_hdf5, args.origin_list, args.destination_list, args.limit_modes, args.limit_values)


import sys
import fnmatch
import os
import re


def update_scenario_file(base_scenario, forecast_year):
	s = base_scenario.replace('.json', '_' + str(forecast_year) + '.json')
	return s

def modify_scenario(scenario_file, parameter, value):
	f = open(scenario_file,'r')
	filedata = f.read()
	datalist = []
	f.close()

	for d in re.split(',|\n',filedata): 
		datalist.append(d.strip())

	find_str = '"' + parameter + '"*'
	find_match_list = fnmatch.filter(datalist,find_str)

	if len(find_match_list)==0:
		print('Could not find parameter: ' + find_str + ' in scenario file: ' + scenario_file)
		print(datalist)
		sys.exit()
		
	find_match = find_match_list[len(find_match_list)-1]

	newstr = '"' + parameter + '" : ' + str(value)
	newdata = filedata.replace(find_match,newstr)

	f = open(scenario_file,'w')
	f.write(newdata)
	f.close()

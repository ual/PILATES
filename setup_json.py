import copy
import json
from pathlib import Path

# Study Directory
# \\vms-fs.es.anl.gov\VMS_Studies\FY23\2212 - ACC-CACC Land Use Study

# Getting data onto BEBOP
# scp "/mnt/q/FY23/2212 - ACC-CACC Land Use Study/2 - POLARIS setup"/general_v3/* \
#     bebop:/lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/jamie_setup/no_cacc_ref/austin
# scp "/mnt/q/FY23/2212 - ACC-CACC Land Use Study/1 - Inputs"/vehicle_distribution_campo_20??.txt \
#     bebop:/lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/jamie_setup/no_cacc_ref/austin
# scp "/mnt/q/FY23/2212 - ACC-CACC Land Use Study/1 - Inputs"/vehicle_distribution_campo_fleet_20??.txt \
#     bebop:/lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/jamie_setup/no_cacc_ref/austin
# scp "/mnt/q/FY23/2212 - ACC-CACC Land Use Study/1 - Inputs"/vehicle_operating_cost_20??.sql \
#     bebop:/lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/jamie_setup/no_cacc_ref/austin

json_dir = Path("/lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/jamie_setup/no_cacc_ref/austin")

# capacity should increase ~ 19% total over 25 years (in 5 yr increments)
five_year_growth = 1.19 ** (1.0/5.0)

##### Note - if CACC VOT is to be turned on, make sure to change the VOT_LEVEL in polaris_settings.yaml

def main():
    json_files = [("scenario_abm.template.json",      "scenario_abm_{}.json"),
                  ("scenario_abm_init.template.json", "scenario_abm_init_{}.json")]
    for in_template, out_template in json_files:
        original_json = read_json(json_dir / in_template )
        growth_relative_to_base = 1.0

        for year in [2010, 2015, 2020, 2025, 2030, 2035]:
            year_json = copy.copy(original_json)

            # l3 automation - change this depending on if we are running with CACC/ACC or not
            #l3_auto = { 2010: 99999, 2015 : 99999, 2020 : 13000, 2025 : 6000, 2030 : 2000, 2035 : 1 }
            l3_auto = { 2010: 99999, 2015 : 99999, 2020 : 99999, 2025 : 99999, 2030 : 99999, 2035 : 99999 }
            year_json['Scenario controls']['L3_automation_cost'] = l3_auto[year]
            year_json['Scenario controls']['simulate_cacc'] = False

            # expressway base = 1600, arterials base cap = 1400, local no change
            year_json['Network simulation controls']['capacity_expressway'] = 1600 * growth_relative_to_base
            year_json['Network simulation controls']['capacity_arterial'] = 1400 * growth_relative_to_base
            growth_relative_to_base *= five_year_growth

            write_json(json_dir / out_template.format(year), year_json)

            # We are going to start from 2020, apparently that means no _year suffix
            #if year == 2020:
            #    write_json(json_dir / in_template.replace('.template', ''), year_json)


def read_json(filename):
    with open(filename, 'r') as f:
        return json.loads(f.read())

def write_json(filename, data):
    print(f"writing to {filename}")
    with open(filename, 'w') as f:
        return f.write(json.dumps(data, indent=2))


main()
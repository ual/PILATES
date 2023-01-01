import json
import yaml
from pathlib import Path
import sys
import fnmatch
import os
import re
from typing import Dict, Optional, Tuple


def update_scenario_file(base_scenario, forecast_year):
	s = base_scenario.replace('.json', '_' + str(forecast_year) + '.json')
	return s



def apply_modification(input_filename: os.PathLike, modifications: Dict, output_filename: Optional[os.PathLike] = None):
    input_filename = Path(input_filename)

    if modifications is None:
        return input_filename

    extension = input_filename.suffix
    output_filename = output_filename or input_filename.with_suffix(f".modified{extension}")
    json_tree = load_dict_from_file(input_filename)

    for key, value in modifications.items():
        dic, key = find_containing_dic(json_tree, key)
        dic[key] = value

    save_dict_to_file(output_filename, json_tree)

    return output_filename

def load_dict_from_file(filename: Path):
    filename = Path(filename)
    with open(filename, "r") as f:
        if filename.suffix == ".json":
            return json.load(f)
        if filename.suffix == ".yaml":
            return yaml.load(f, Loader=yaml.FullLoader)


def save_dict_to_file(filename, dic):
    filename = Path(filename)
    with open(filename, "w") as f:
        if filename.suffix == ".json":
            json.dump(dic, f, indent=4)
        if filename.suffix == ".yaml":
            yaml.dump(dic, f)

def split_key(key):
    tokens = key.split(".")
    if len(tokens) < 2:
        return None, key
    return tokens[0:-1], tokens[-1]


def find_containing_dic(dictio: dict, key: str) -> Tuple[dict, str]:
    """Finds the dictionary that contains the given key and returns a reference to it
    so that modifications can be made to it.
    """
    if key in dictio:
        return dictio, key  # it is a top level key

    # Check if the user specified it as a nested key syntax (eg. a.b.c)
    outer, inner = split_key(key)
    if outer is None:
        # If not, see if any of the sub-dictionaries have a key with the given name
        sub_dict = find_recursively_dic_with_key(dictio, key)
        # If so, return that, otherwise just return the original
        return (sub_dict or dictio, key)

    # If the user specified the exact key - the containing dict has to exist
    for i in outer:
        if i not in dictio:
            raise RuntimeError(f"Can't find key {key} in dictionary")
        dictio = dictio[i]
    return dictio, inner


def find_recursively_dic_with_key(dic, key_to_find):
    for key, value in dic.items():
        if key == key_to_find:
            return dic
        elif isinstance(value, dict):
            ret_val = find_recursively_dic_with_key(value, key_to_find)
            if ret_val is not None:
                return ret_val



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

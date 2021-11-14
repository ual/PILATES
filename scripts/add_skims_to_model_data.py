import yaml
import os
os.chdir("..")
from pilates.urbansim import preprocessor as usim_pre

if __name__ == '__main__':


    with open('settings.yaml') as f:
        settings = yaml.load(f, Loader=yaml.FullLoader)
    usim_pre.add_skims_to_model_data(settings)

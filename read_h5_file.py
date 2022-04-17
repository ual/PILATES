import h5py
filename = "pilates/urbansim/data/model_data_2015.h5"

with h5py.File(filename, "r") as f:
    #
    # # List all groups
    # print("Keys: %s" % f.keys())
    # a_group_key = list(f.keys())[0]
    #
    # # Get the data
    # data = list(f[a_group_key])
    # Get the HDF5 group
    for key1 in f.keys():
        print(key1)  # Names of the groups in HDF5 file.

        group = f[key1]

        # Checkout what keys are inside that group.
        for key in group.keys():
            print('\t{0}'.format(key))

            # data = group[key][()]
            # Do whatever you want with data

    # After you are done
    f.close()
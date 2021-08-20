#!/bin/bash
echo $PWD

rm ./pilates/polaris/data/*
rm ./pilates/urbansim/data/*

cp ./pilates/backup/campo_skims.hdf5 pilates/polaris/data/
cp ./pilates/backup/custom_mpo_*.h5 pilates/urbansim/data/

cp /mnt/e/polaris/UrbanSim_files/*.json /mnt/e/polaris/Repos/campoInputs
cp /mnt/e/polaris/UrbanSim_files/*.csv /mnt/e/polaris/Repos/campoInputs
cp /mnt/e/polaris/campoInputs-orig/*.sqlite /mnt/e/polaris/Repos/campoInputs
cp /mnt/e/polaris/campoInputs-orig/*.bin /mnt/e/polaris/Repos/campoInputs

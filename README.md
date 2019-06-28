# PILATES
**P**latform for \
**I**ntegrated \
**L**anduse \
**A**nd \
**T**ransportation \
**E**xperiments and \
**S**imulation

PILATES is designed to facilitate the automated coordination of intra-simulation integration between UrbanSim, ActivitySynth, and a user-specified travel demand model.

This repository is comprised of the following:
1. **pilates.sh** -- a single executable bash script responisble for orchestrating the execution of all simulations and intermediate data-transformation scripts managing command-line arguments and environment variables
2. Two intermediate data-transformation scripts located in the **scripts/** directory, responsible for packaging and unpackaging data inputs and outputs to meet the specifications required by individual simulation platforms utilized within PILATES.
3. A **Dockerfile** containing the instructions used to build a PILATES container image. To build a PILATES image yourself, simply navigate to the directory containing the Dockerfile and execute the following command: `docker build -t <give your image a tag (name) here> `. Or simply find and download the container image from dockerhub.

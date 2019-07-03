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
3. A **Dockerfile** containing the instructions used to build a PILATES container image. The easiest way to get PILATES running to just pull the image down from dockerhub (`docker pull mxndrwgrdnr/pilates`) but the Dockerfile allows you to build an image yourself (`docker build -t <name> .`) and make whatever changes you'd like at build-time.



### Running PILATES
Once the PILATES container image is installed you can run it by executing `docker run -it mxndrwgrdnr/pilates` + the following positional arguments:
- `[start year]`, *required*, e.g. 2010, the first simulation year corresponding to the vintage of your input data.
- `[end year]`, *required*, e.g. 2040, the final simulation year and **the year for which synthetic activity plans will be generated.**
- `[intra-simulation frequency]`, *required*, e.g. 5, the interval (in simulation years) at which the simulation models are run, should be a whole number divisor of the simulation duration (end year - start year).
- `[scenario]`, *required*, e.g. base, has no operational effect on simulations but helps specify the filepaths for reading inputs from and writing outputs to.
- `[skims filename]`, *required*, e.g. skims-baseline.csv.gz, name of the compressed .csv containing travel model skims located on s3 in the designated skims bucket (see "Setting up AWS S3 accesss below").
- `[in-year outputs on]`, *optional*, e.g. on, appending "on" as the optional 6th positional argument will cause PILATES to generate activity plans for the start year, which otherwise it would not. 


### Example use cases
Use 2010 input data to generate base scenario synthetic activity plans for the year 2040 using 5 year intervals:
- `docker run -it mxndrwgrdnr/pilates 2010 2040 5 base skims-baseline.csv.gz`

Use 2010 input data to generate hi-tech scenario synthetic activity plans for the year 2010 AND 2025 using 5 year intervals:
- `docker run -it mxndrwgrdnr/pilates 2010 2025 5 base skims-baseline.csv.gz on`

Use the 2025 output data to generate hi-tech scenario synthetic activity plans for the year 2040 using 5 year intervals:
- `docker run -it mxndrwgrdnr/pilates 2025 2040 5 base skims-baseline.csv.gz`


### Setting up AWS S3 access
The Dockerfile defines three environment variables corresponding to the names of three s3 buckets that PILATES will use for reading and writing data: `$BAUS_INPUT_BUCKET` (default: urbansim-inputs), `$BAUS_OUTPUT_BUCKET` (default: urbansim-outputs), and `$SKIMS_BUCKET` (default: urbansim-beam). The defaults can be replaced via build-arguments if building the image from the Dockerfile. Once built however, PILATES must have access to whichever buckets have been specified. The easiest way to ensure access is to deploy the PILATES image on an AWS EC2 instance created with an IAM role that has access to these buckets, which case the PILATES image will simply inherit the credentials of its host machine. If you are running PILATES locally, however, or for some reason the IAM-based inheritance doesn't work, you can simply pass your AWS credentials to the PILATES image as additional environment variables using the `-e` flag in `docker run` like so:
```
docker run -it -e $AWS_ACCESS_KEY_ID -e $AWS_SECRET_ACCESS_KEY mxndrwgrdnr/pilates 2010 2040 5 base skims-baseline.csv.gz
```
provided you have first defined these environment variables on your host machine.

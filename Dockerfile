FROM continuumio/miniconda3

ENV HOME /home/ubuntu

# env vars
ENV CONDA_DIR /opt/conda
ENV PATH $CONDA_DIR/bin:$PATH
ENV CONDA_ENV_BAUS_ORCA_1_4 BAUS_orca_1_4
ENV CONDA_ENV_BAUS_ORCA_1_5 BAUS_orca_1_5
ENV CONDA_ENV_ASYNTH activitysynth
ENV PATH /opt/conda/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin:$PATH
ENV PATH /opt/conda/envs/$CONDA_ENV_BAUS_ORCA_1_5/bin:$PATH
ENV PATH /opt/conda/envs/$CONDA_ENV_ASYNTH/bin:$PATH

ENV PILATES_PATH $HOME/PILATES
ENV LOG_PATH $PILATES_PATH/logs

ENV BAUS_PATH $HOME/bayarea_urbansim
ENV BAUS_DATA_STORE_PATH $BAUS_PATH/data
ENV BAUS_DATA_OUTPUT_PATH $BAUS_PATH/output
ENV BAUS_DATA_OUTPUT_FILE model_data_output.h5
ENV BAUS_DATA_OUTPUT_FILEPATH $BAUS_DATA_OUTPUT_PATH/$BAUS_DATA_OUTPUT_FILE

ARG BAUS_INPUT_BUCKET=urbansim-inputs
ENV BAUS_INPUT_BUCKET $BAUS_INPUT_BUCKET
ENV BAUS_INPUT_BUCKET_PATH s3://$BAUS_INPUT_BUCKET

ARG BAUS_OUTPUT_BUCKET=urbansim-outputs
ENV BAUS_OUTPUT_BUCKET $BAUS_OUTPUT_BUCKET
ENV BAUS_OUTPUT_BUCKET_PATH s3://$BAUS_OUTPUT_BUCKET

ENV ASYNTH_PATH $HOME/activitysynth
ENV ASYNTH_DATA_PATH $ASYNTH_PATH/activitysynth/data
ENV ASYNTH_DATA_OUTPUT_PATH $ASYNTH_PATH/activitysynth/output
ENV ASYNTH_DATA_OUTPUT_FILE model_data_output.h5
ENV ASYNTH_DATA_OUTPUT_FILEPATH $ASYNTH_DATA_OUTPUT_PATH/$ASYNTH_DATA_OUTPUT_FILE

ARG SKIMS_BUCKET=urbansim-beam
ENV SKIMS_BUCKET $SKIMS_BUCKET


# update ubuntu stuff
RUN apt-get update \
	&& apt-get install -y build-essential zip unzip
RUN conda update conda


# BAUS Estimation Python Environment
RUN conda create --quiet --yes --channel conda-forge -p $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4 \
	python=2.7 \
	numpy=1.11.0 \
	scipy \
	pandas \
	s3fs \
	geopandas \
	scikit-learn \
	git \
	pip \
	boto

RUN cd $HOME && git clone https://github.com/ual/bayarea_urbansim.git \
	&& cd $BAUS_PATH \
	&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python -m pip install -r requirements.txt

RUN cd $HOME && git clone https://github.com/UDST/variable_generators.git \
	&& cd variable_generators \
	&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python setup.py install


# BAUS Simulation Python Environment
RUN conda create --quiet --yes -p $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_5 --clone $CONDA_ENV_BAUS_ORCA_1_4
RUN $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_5/bin/python -m pip install orca==1.5.1


# # # ActivitySynth Python Environment
RUN conda create --quiet --yes --channel conda-forge -p $CONDA_DIR/envs/$CONDA_ENV_ASYNTH \
	python=3.6 \
	pip \
	pyarrow==0.12.1 \
	choicemodels \
	urbansim_templates \
	s3fs \
	scipy==1.2.1
RUN cd $HOME && git clone https://github.com/ual/activitysynth.git \
	&& cd $ASYNTH_PATH \
	&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python setup.py install
RUN conda config --add channels udst
RUN conda config --add channels conda-forge
RUN $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python -m pip install pandana


# Get PILATES repo from github
RUN cd $HOME && git clone https://github.com/ual/PILATES.git

# Run PILATES
WORKDIR $PILATES_PATH
RUN chmod +x pilates.sh
ENTRYPOINT ["./pilates.sh"]
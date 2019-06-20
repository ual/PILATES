FROM continuumio/miniconda3

WORKDIR /home

# Python env names and paths
ENV CONDA_DIR /opt/conda
ENV PATH $CONDA_DIR/bin:$PATH
ENV CONDA_ENV_USIM_PY2 usim_py2
ENV CONDA_ENV_USIM_PY3 usim_py3
ENV CONDA_ENV_ASYNTH activitysynth

ENV PATH /opt/conda/envs/$CONDA_ENV_USIM_PY2/bin:$PATH
ENV PATH /opt/conda/envs/$CONDA_ENV_USIM_PY3/bin:$PATH
ENV PATH /opt/conda/envs/$CONDA_ENV_ASYNTH/bin:$PATH

# S3 credentials
ARG AWS_ACCESS_KEY_ID
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

# Runtime args
ARG IN_YEAR
ARG OUT_YEAR


# Build Python envs
RUN conda update conda

# BAUS Estimation Python Environment
RUN conda create --quiet --yes --channel conda-forge -p $CONDA_DIR/envs/$CONDA_ENV_USIM_PY2 \
	python=2.7 \
	numpy=1.10.0 \
	scipy \
	pandas \
	geopandas \
	scikit-learn \
	git \
	pip \
	boto

RUN git clone https://github.com/ual/bayarea_urbansim.git \
	&& cd bayarea_urbansim \
	&& $CONDA_DIR/envs/$CONDA_ENV_USIM_PY2/bin/python -m pip install -r requirements.txt


# BAUS Simulation Python Environment
RUN conda create --quiet --yes -p $CONDA_DIR/envs/$CONDA_ENV_USIM_PY3 --clone $CONDA_ENV_USIM_PY2
RUN conda install -p $CONDA_DIR/envs/$CONDA_ENV_USIM_PY3 -c udst orca


# ActivitySynth Python Environment
RUN conda create --quiet --yes --channel conda-forge -p $CONDA_DIR/envs/$CONDA_ENV_ASYNTH \
	python=3.6 \
	pip \
	pyarrow==0.12.1\
	choicemodels \
	urbansim_templates \
	s3fs \
	scipy < 1.3
RUN git clone https://github.com/ual/activitysynth.git \
	&& cd activitysynth \
	&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python setup.py install
RUN conda config --add channels udst
RUN conda config --add channels conda-forge
RUN conda install --quiet --yes -p $CONDA_DIR/envs/$CONDA_ENV_ASYNTH -c udst pandana


# Make model data .h5
RUN $CONDA_DIR/envs/$CONDA_ENV_USIM_PY2/bin/python make_model_data_hdf.py -y base


# Run data pre-processing step
RUN $CONDA_DIR/envs/$CONDA_ENV_USIM_PY2/bin/python baus.py --mode preprocessing
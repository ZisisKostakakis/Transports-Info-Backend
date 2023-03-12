FROM python:3.9

WORKDIR /app

COPY . /app

# Install pipenv
RUN pip3 install --user pipenv
ENV PATH="/root/.local/bin:${PATH}"

# Set PYTHONPATH
ENV PYTHONPATH=/app/generate_data:/app/utils:

# Install dependencies
COPY **/Pipfile* ./
RUN cd /app
RUN pipenv install

# Set the command to run when the container is started
CMD pipenv run pylint --rcfile=pylint.cfg /app/*.py -s true --fail-under=10 && \
    pipenv run pytest

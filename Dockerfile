FROM python:3.9

WORKDIR /app

# COPY . /app

RUN apt-get install -y git

# Clone the repository
RUN git clone https://github.com/ZisisKostakakis/Web-app-python.git /app

# Install pipenv
RUN pip3 install --user pipenv
ENV PATH="/root/.local/bin:${PATH}"

# Set PYTHONPATH
ENV PYTHONPATH=/app/generate_data:/app/utils:

# Install dependencies
COPY **/Pipfile* ./
RUN cd /app && pip install --no-cache-dir -r requirements.txt

CMD pipenv run pylint --rcfile=pylint.cfg $(git ls-files '*.py') -s true --fail-under=10 && \
    pipenv run pytest 


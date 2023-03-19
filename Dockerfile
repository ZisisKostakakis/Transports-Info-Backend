# FROM python:3.9

# WORKDIR /app

# COPY . /app

# # Install pipenv
# RUN pip3 install --user pipenv
# ENV PATH="/root/.local/bin:${PATH}"

# # Set PYTHONPATH
# ENV PYTHONPATH=/app/generate_data:/app/utils:

# # Install dependencies
# COPY **/Pipfile* ./
# RUN cd /app

# CMD ["tail", "-f", "/dev/null"]

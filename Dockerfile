FROM python:3.7

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

COPY . /app
WORKDIR /app

# Install python packages
RUN pip install --no-cache-dir -r /app/requirements.txt

CMD ["python", "./run.py"]

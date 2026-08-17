#Deriving the latest base image
FROM python:latest

#Labels as key value pair
LABEL Maintainer="Kv603"
LABEL Repo="https://github.com/Kv603/ChangeStream2MQTT"

# Any working directory can be chosen as per choice like '/' or '/home' etc
# I have chosen /usr/app/src
WORKDIR /usr/app/src

#to COPY the remote file at working directory in container
#COPY changes.sh  ./
COPY requirements.txt changes.py  ./
# Now the structure looks like this '/usr/app/src/test.py'


# We will need these environment variables 
ENV MQTT_PW="${MQTT_PW}"
ENV MQTT_USER="${MQTT_USER}"
ENV MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
ENV MQTT_PORT=${MQTT_PORT:-1883} 
ENV CHANGE_STREAM_DB="${CHANGE_STREAM_DB}"

# setup required extras
RUN pip3 install --upgrade-strategy only-if-needed -r ./requirements.txt

#CMD instruction should be used to run the software
#contained by your image, along with any arguments.
CMD [ "python3", "./changes.py"]

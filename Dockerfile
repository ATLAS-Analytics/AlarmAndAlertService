FROM ivukotic/ml_platform_auto:latest

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

RUN pip2 --no-cache-dir install  --upgrade oauth2client google-api-python-client
RUN pip3 --no-cache-dir install  --upgrade oauth2client google-api-python-client

COPY . .
RUN mkdir Images

# build info
RUN echo "Timestamp:" `date --utc` | tee /image-build-info.txt

# CMD ["/.run"]

FROM ivukotic/ml_platform_auto:latest

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

RUN pip2 --no-cache-dir install httplib2
RUN pip3 --no-cache-dir install httplib2

COPY . .
# build info
RUN echo "Timestamp:" `date --utc` | tee /image-build-info.txt

# CMD ["/.run"]

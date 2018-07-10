FROM ivukotic/ml_platform_auto:latest

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

COPY . .
# build info
RUN echo "Timestamp:" `date --utc` | tee /image-build-info.txt

# CMD ["/.run"]

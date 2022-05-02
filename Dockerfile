FROM ivukotic/ml_platform:latest

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>
RUN apt-key adv --keyserver developer.download.nvidia.com --recv-keys A4B469963BF863CC
RUN apt-get update && apt-get install sendmail -y

COPY . .
RUN mkdir Images
RUN mkdir Users/Images
# build info
RUN echo "Timestamp:" `date --utc` | tee /image-build-info.txt

# CMD ["/.run"]

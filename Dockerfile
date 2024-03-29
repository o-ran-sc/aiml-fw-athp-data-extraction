# ==================================================================================
#
#       Copyright (c) 2022 Samsung Electronics Co., Ltd. All Rights Reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# ==================================================================================
#Base Image
FROM nexus3.o-ran-sc.org:10002/o-ran-sc/bldr-ubuntu18-c-go:1.9.0 as ubuntu
FROM ubuntu:18.04

# location in the container
ENV TA_DIR /home/app/

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3 && apt-get install -y \
    python3-pip

# Install OpenJDK-8 for Spark
RUN apt-get update && \
    apt-get install openjdk-8-jre openjdk-8-jdk -y && \
    apt-get clean;
	
RUN apt-get update && \
    apt-get install scala=2.11.12-4~18.04 -y && \
    apt-get clean;
    
# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

WORKDIR ${TA_DIR}
# Copy sources into the container
COPY . .

#Install the pip3 requirements
RUN pip3 install -r requirements.txt

#Expose the ports
EXPOSE 32000

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
FROM ubuntu:22.04

# location in the container
ENV TA_DIR=/home/app

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/

WORKDIR ${TA_DIR}

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    openjdk-11-jre \
    openjdk-11-jdk \
    scala && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy sources into the container
COPY . .

#Install the pip3 requirements
RUN pip3 install --no-cache-dir -r requirements.txt

#Expose the ports
EXPOSE 32000

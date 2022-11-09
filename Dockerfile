#Base Image
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
EXPOSE 5000

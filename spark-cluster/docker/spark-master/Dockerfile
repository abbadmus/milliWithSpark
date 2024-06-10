FROM spark-base:latest

RUN apt-get update \
    && apt-get install -y dos2unix \
    && apt-get install -y  openssh-server \
    && apt-get install -y  nano \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && apt-get install -y procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Set root password dont do this in production
RUN echo 'root:docker' | chpasswd

# Configure SSH to allow root login
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
CMD service ssh start

COPY start-master.sh /

RUN dos2unix /start-master.sh && apt-get --purge remove -y dos2unix && rm -rf /var/lib/apt/lists/*

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

EXPOSE 8080 7077 6066 22


#CMD ["/bin/bash", "/start-master.sh"]
# Create directory for SSH PID file
RUN mkdir /var/run/sshd

# Start SSH service
CMD service ssh start && /bin/bash /start-master.sh
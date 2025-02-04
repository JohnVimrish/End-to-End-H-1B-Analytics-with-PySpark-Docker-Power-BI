# Start from the base Ubuntu image
FROM ubuntu:latest

# Set environment variables to avoid interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary packages including sudo, Java, Scala, wget, curl, and SSH
RUN apt-get update && apt-get install -y \
    sudo \
    openjdk-11-jdk \
    scala \
    wget \
    curl \
    openssh-server \
    apache2 \
    rpm \
    dnf \
    gnupg2 \
    && apt-get clean

# Set up SSH and make sure the SSH service starts
RUN mkdir /var/run/sshd

# Set the environment variables for Spark and Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_VERSION=3.4.4
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark



# Install Apache Spark
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -P /tmp && \
    tar xvf /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /opt && \
    ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME

# Add Spark binaries to PATH
ENV PATH=$SPARK_HOME/bin:$PATH

# Create the new user `john_user` and set the password to `abc12345`
RUN useradd -m -s /bin/bash john_user && \
    echo "john_user:abc12345" | chpasswd

# Grant the new user `john_user` sudo privileges
RUN usermod -aG sudo john_user

# Set up SSH (if you need SSH access)
RUN mkdir /home/john_user/.ssh && \
    chmod 700 /home/john_user/.ssh && \
    chown john_user:john_user /home/john_user/.ssh

# Expose necessary ports
EXPOSE 22 80 443 7077 8080 4040 8888 3306 5432 6379 27017 3389 5900

# Set the entrypoint to run SSHD (SSH daemon) and keep the container running
CMD ["/usr/sbin/sshd", "-D"]



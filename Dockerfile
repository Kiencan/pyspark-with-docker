# Use an appropriate base image with Python and Java installed
FROM openjdk:8-jdk

# Set environment variables for Spark
ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=3.2

# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y wget python3 python3-pip sqlite3

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install PySpark and Jupyter
RUN pip3 install pyspark notebook pandas

# Download and place SQLite JDBC driver
RUN wget https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.46.0.0/sqlite-jdbc-3.46.0.0.jar -P /opt/spark/jars

# Add Spark to PATH
ENV PATH=$PATH:/opt/spark/bin

# Set the working directory
WORKDIR /workspace

COPY  createdb.ipynb ./createdb.ipynb
COPY  ./bankdataset.csv ./bankdataset.csv
COPY  query.ipynb ./query.ipynb

# Expose ports for Jupyter Notebook
EXPOSE 8888

# Start Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--allow-root", "--no-browser"]
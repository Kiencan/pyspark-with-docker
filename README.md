# SparkSQL in Docker Project

**Description**

Mục tiêu của project này là cài đặt và chạy SparkSQL trong một Docker container. Container này sẽ lưu trữ một cơ sở dữ liệu (cụ thể là SQLite database) và cho phép thực hiện các truy vấn SQL sử dụng SparkSQL.

**Prerequisites**

Trước khi bắt đầu project, hãy đảm bảo rằng máy tính của bạn đủ bộ nhớ (tối thiểu 30GB) và đã cài đặt Docker. Để Docker đơn giản, dự án sử dụng Docker Desktop với một giao diện dễ nhìn và dễ thực hiện. Bạn có thể tải về trên trang web [Docker Desktop](https://www.docker.com/products/docker-desktop/) tùy theo hệ điều hành máy sử dụng.

**Installation**

Bước 1: Tạo một Dockerfile với nội dung như sau:

```
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
```

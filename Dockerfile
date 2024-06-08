# Use an image of Java
FROM openjdk:8-jdk

# Install python, sqlite3 and some necessary tool
RUN apt-get update && \
    apt-get install -y wget python3 python3-pip sqlite3

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar xvf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz

# Install PySpark and Jupyter
RUN pip3 install pyspark notebook pandas

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
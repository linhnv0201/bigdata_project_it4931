#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Trap SIGINT (Ctrl+C) to stop all services gracefully
trap 'stop_services; exit' SIGINT

# Function to stop services
stop_services() {
    echo "Stopping all services..."
    if pgrep -f "hadoop" > /dev/null; then
        echo "Stopping HDFS and YARN..."
        stop-dfs.sh
        stop-yarn.sh
    fi
    if pgrep -f "kafka" > /dev/null; then
        echo "Stopping Kafka..."
        pkill -f kafka.Kafka
    fi
    if pgrep -f "zookeeper" > /dev/null; then
        echo "Stopping Zookeeper..."
        pkill -f zookeeper
    fi
}

# Update and upgrade system packages
echo "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install required packages
echo "Installing required packages..."
sudo apt install -y openssh-server python3-pip openjdk-11-jdk scala wget tar

# Set JAVA_HOME environment variable
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Configure SSH server
echo "Configuring SSH server..."
sudo service ssh start

# Generate SSH keys if not already generated
if [ ! -f "$HOME/.ssh/id_rsa" ]; then
    echo "Generating SSH keys..."
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 0600 ~/.ssh/authorized_keys
else
    echo "SSH keys already exist."
fi

# Test SSH localhost connectivity
echo "Testing SSH connectivity..."
ssh -o StrictHostKeyChecking=no localhost exit

# Install Hadoop
if [ ! -d "$HOME/hadoop/hadoop-3.4.0" ]; then
    echo "Downloading and installing Hadoop..."
    wget https://downloads.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz -O $HOME/hadoop-3.4.0.tar.gz
    mkdir -p $HOME/hadoop
    tar -xzvf $HOME/hadoop-3.4.0.tar.gz -C $HOME/hadoop
    rm $HOME/hadoop-3.4.0.tar.gz
fi

# Set Hadoop environment variables
if ! grep -q "HADOOP_HOME" ~/.bashrc; then
    echo "Setting Hadoop environment variables..."
    cat >> ~/.bashrc <<EOL

# Hadoop Environment Variables
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop/hadoop-3.4.0
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=\$HADOOP_HOME
export HADOOP_COMMON_HOME=\$HADOOP_HOME
export HADOOP_HDFS_HOME=\$HADOOP_HOME
export YARN_HOME=\$HADOOP_HOME
ulimit -n 10000
ulimit -u 4096

EOL
    source ~/.bashrc
fi

# Configure Hadoop
echo "Configuring Hadoop..."
cd $HADOOP_HOME/etc/hadoop

# Update hadoop-env.sh with JAVA_HOME
sed -i '/JAVA_HOME/c\export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' hadoop-env.sh
# Configure core-site.xml

cat > core-site.xml <<EOL
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOL

# Configure hdfs-site.xml
cat > hdfs-site.xml <<EOL
<configuration>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:$HADOOP_HOME/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:$HADOOP_HOME/hdfs/datanode</value>
    </property>
</configuration>
EOL

# Configure mapred-site.xml
cat > mapred-site.xml <<EOL
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
EOL

# Configure yarn-site.xml
cat > yarn-site.xml <<EOL
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
</configuration>
EOL

# Prepare HDFS directories
echo "Preparing HDFS directories..."
rm -rf $HADOOP_HOME/hdfs/namenode $HADOOP_HOME/hdfs/datanode
mkdir -p $HADOOP_HOME/hdfs/namenode $HADOOP_HOME/hdfs/datanode
chmod -R 700 $HADOOP_HOME/hdfs

# Format and start Hadoop
echo "Formatting and starting Hadoop..."
hdfs namenode -format
start-dfs.sh
start-yarn.sh

# Install Spark
if [ ! -d "/opt/spark" ]; then
    echo "Installing Spark..."
    wget https://downloads.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
    tar -xzf spark-3.5.3-bin-hadoop3.tgz
    sudo mv spark-3.5.3-bin-hadoop3 /opt/spark
    rm spark-3.5.3-bin-hadoop3.tgz
fi

# Set Spark environment variables
if ! grep -q "SPARK_HOME" ~/.bashrc; then
    echo "Setting Spark environment variables..."
    cat >> ~/.bashrc <<EOL

# Spark Environment Variables
export SPARK_HOME=/opt/spark
export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
EOL
    source ~/.bashrc
fi

# Install PySpark
pip install pyspark

# Install Kafka
if [ ! -d "$HOME/kafka" ]; then
    echo "Installing Kafka..."
    wget https://downloads.apache.org/kafka/3.7.2/kafka_2.12-3.7.2.tgz
    tar -xzf kafka_2.12-3.7.2.tgz
    mkdir -p $HOME/kafka
    mv kafka_2.12-3.7.2/* $HOME/kafka
    rm -rf kafka_2.12-3.7.2 kafka_2.12-3.7.2.tgz
fi

# Add Kafka to PATH in .bashrc if not already present
if ! grep -q "KAFKA_HOME" "$HOME/.bashrc"; then
    echo "Adding Kafka environment variables to .bashrc..."
    echo "" >> "$HOME/.bashrc"
    echo "# Kafka environment variables" >> "$HOME/.bashrc"
    echo "export KAFKA_HOME=\$HOME/kafka" >> "$HOME/.bashrc"
    echo "export PATH=\$PATH:\$KAFKA_HOME/bin" >> "$HOME/.bashrc"
    source "$HOME/.bashrc"
fi

# Create systemd service for Zookeeper
sudo tee /etc/systemd/system/zookeeper.service > /dev/null << EOF
[Unit]
Description=Apache Zookeeper Server
Documentation=http://kafka.apache.org/documentation.html
Requires=network.target
After=network.target

[Service]
Type=simple
Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:$HOME/kafka/bin"
User=pc
ExecStart=$HOME/kafka/bin/zookeeper-server-start.sh $HOME/kafka/config/zookeeper.properties
ExecStop=$HOME/kafka/bin/zookeeper-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
EOF

# Create systemd service for Kafka
sudo tee /etc/systemd/system/kafka.service > /dev/null << EOF
[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org/documentation.html
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:$HOME/kafka/bin"
User=pc
ExecStart=$HOME/kafka/bin/kafka-server-start.sh $HOME/kafka/config/server.properties
ExecStop=$HOME/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd daemon and enable services
sudo systemctl daemon-reload
sudo systemctl enable zookeeper.service
sudo systemctl enable kafka.service

# Start Kafka and Zookeeper services
echo "Starting Zookeeper and Kafka services..."
sudo systemctl start zookeeper
sudo systemctl start kafka

# Check service status
echo "Checking service status..."
sudo systemctl status zookeeper --no-pager
sudo systemctl status kafka --no-pager

echo "Setup completed successfully!"
echo "You can manage the services using:"
echo "sudo systemctl start|stop|restart|status kafka"
echo "sudo systemctl start|stop|restart|status zookeeper"
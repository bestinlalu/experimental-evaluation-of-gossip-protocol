export VM1_IP="152.7.179.141"
export VM2_IP="152.7.179.79"
export USER="blalu"

export DB_USER="root"
export DB_PASS="password"
export DB_NAME="gossipdb"
export GO_DIR="/home/$USER/goroot/go/bin"


echo ""
echo "Checking Java installations..."
if [ ! -f /usr/lib/jvm/java-11-openjdk-amd64/bin/java ]; then
    echo "Java 11 not found! Installing..."
    sudo apt update -y && sudo apt install openjdk-11-jdk -y
    echo "Java 11 installed!"
else
    echo "Java 11 available!"
fi

if [ ! -f /usr/lib/jvm/java-21-openjdk-amd64/bin/java ]; then
    echo "Java 21 not found! Installing..."
    sudo apt update -y && sudo apt install openjdk-21-jdk -y
    echo "Java 21 installed!"
else
    echo "Java 21 available!"
fi


echo "Checking Go installation..."
if [ ! -f "$GO_DIR/go" ]; then
    echo "Go 1.23 not found! Installing..."
    wget -q https://go.dev/dl/go1.23.4.linux-amd64.tar.gz -O /tmp/go1.23.4.tar.gz
    mkdir -p /home/$USER/goroot
    tar -C /home/$USER/goroot -xzf /tmp/go1.23.4.tar.gz
    rm /tmp/go1.23.4.tar.gz
    echo "Go 1.23 installed!"
else
    echo "Go 1.23 available!"
fi

export GOROOT=/home/$USER/goroot/go
export PATH=$GOROOT/bin:$PATH
export GOPATH=/home/$USER/gopath
go version

echo 'export GOROOT=/home/$USER/goroot/go' >> ~/.bashrc
echo 'export PATH=$GOROOT/bin:$PATH' >> ~/.bashrc
source ~/.bashrc


echo ""
echo "[3/10] Fixing localhost resolution..."
if ! grep -q "127.0.0.1 localhost" /etc/hosts; then
    echo '127.0.0.1 localhost' | sudo tee -a /etc/hosts
    echo "Fixed!"
else
    echo "localhost already resolves correctly."
fi

# ─────────────────────────────────────────
# Clear firewall
# ─────────────────────────────────────────
echo ""
echo "[4/10] Clearing firewall rules..."
sudo iptables -F
echo "Done."


# ─────────────────────────────────────────
# Setup MySQL
# ─────────────────────────────────────────
echo ""
echo "[5/10] Setting up MySQL..."
if ! command -v mysql &> /dev/null; then
    echo "MySQL not found! Installing..."
    sudo apt update -y && sudo apt install mysql-server -y
fi

sudo systemctl start mysql
sudo systemctl status mysql --no-pager | grep "Active:"

# Set root password and create database
echo "Configuring MySQL..."
# Try with no password first (fresh install), then try with existing password
if sudo mysql -e "SELECT 1;" 2>/dev/null; then
    sudo mysql << SQLEOF
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '$DB_PASS';
CREATE DATABASE IF NOT EXISTS $DB_NAME;
GRANT ALL PRIVILEGES ON $DB_NAME.* TO 'root'@'%';
FLUSH PRIVILEGES;
SQLEOF
    echo "MySQL configured successfully!"
elif mysql -u $DB_USER -p$DB_PASS -e "SELECT 1;" 2>/dev/null; then
    mysql -u $DB_USER -p$DB_PASS -e "CREATE DATABASE IF NOT EXISTS $DB_NAME;"
    echo "MySQL already configured!"
else
    echo "WARNING: Could not configure MySQL automatically!"
fi

# UPDATE mysql.user SET Host='%' WHERE User='root' AND Host='localhost';

# -- Refresh permissions
# FLUSH PRIVILEGES;

# Allow remote connections
sudo sed -i 's/bind-address.*/bind-address = 0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf 2>/dev/null || true
sudo systemctl restart mysql

# Verify MySQL login
if mysql -u $DB_USER -p$DB_PASS -e "USE $DB_NAME;" 2>/dev/null; then
    echo "MySQL login verified!"
else
    echo "WARNING: MySQL login failed! Check credentials."
fi


# ─────────────────────────────────────────
# Setup Maven
# ─────────────────────────────────────────
wget https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz
sudo tar -xvzf apache-maven-3.9.9-bin.tar.gz -C /opt
echo 'export M2_HOME=/opt/apache-maven-3.9.9' >> ~/.bashrc
echo 'export PATH=$M2_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
mvn -version


# ─────────────────────────────────────────
# Setup Tunneling in local machine
# ─────────────────────────────────────────
ssh -L 9090:127.0.0.1:8080 blalu@152.7.179.141


# ─────────────────────────────────────────
# Setup ufw firewall rules on remote machines
# ─────────────────────────────────────────
sudo apt install ufw
# Allow the specific ports
sudo ufw allow 8080/tcp
sudo ufw allow 8081/tcp
sudo ufw allow 8082/tcp
sudo ufw allow 9000:9005/udp
sudo ufw allow 6000:8000/udp
sudo ufw allow from 152.7.179.79  # Allow EVERYTHING from VM2
sudo ufw allow 9092/tcp           # Specifically allow Kafka

# Check the status to make sure they are active
sudo ufw status


# ─────────────────────────────────────────
# Step 6: Start Zookeeper (Java 11)
# ─────────────────────────────────────────
echo ""
echo "[6/10] Starting Zookeeper with Java 11..."
sudo update-alternatives --set java /usr/lib/jvm/java-11-openjdk-amd64/bin/java
java -version 2>&1 | head -1

bin/zookeeper-server-start.sh config/zookeeper.properties



# ─────────────────────────────────────────
# Step 7: Start Kafka (Java 11)
# ─────────────────────────────────────────
echo ""
echo "[7/10] Starting Kafka with Java 11..."
bin/kafka-server-start.sh config/server.properties

# Create gossip topic
echo "Creating gossip topic..."
bin/kafka-topics.sh --create \
    --bootstrap-server $VM1_IP:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic gossip 2>/dev/null || echo "Topic already exists, skipping."
bin/kafka-topics.sh --list --bootstrap-server $VM1_IP:9092


# ─────────────────────────────────────────
# Step 8: Switch to Java 21 and Build Apps
# ─────────────────────────────────────────
echo ""
echo "[8/10] Switching to Java 21 and building apps..."
sudo update-alternatives --set java /usr/lib/jvm/java-21-openjdk-amd64/bin/java
java -version 2>&1 | head -1

mvn compile quarkus:dev
mvn compile quarkus:dev -Dquarkus.http.host=0.0.0.0

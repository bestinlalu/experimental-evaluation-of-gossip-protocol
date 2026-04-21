VM1_IP="152.7.177.154"
VM2_IP"="152.7.177.182"
USER="blalu"

GO_DIR="/home/sshunmu2/goroot/go/bin"


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
    mkdir -p /home/sshunmu2/goroot
    tar -C /home/sshunmu2/goroot -xzf /tmp/go1.23.4.tar.gz
    rm /tmp/go1.23.4.tar.gz
    echo "Go 1.23 installed!"
else
    echo "Go 1.23 available!"
fi

export GOROOT=/home/blalu/goroot/go
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
# Setup Maven
# ─────────────────────────────────────────
wget https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz
sudo tar -xvzf apache-maven-3.9.9-bin.tar.gz -C /opt
echo 'export M2_HOME=/opt/apache-maven-3.9.9' >> ~/.bashrc
echo 'export PATH=$M2_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
mvn -version


# ─────────────────────────────────────────
# Setup ufw firewall rules on remote machines
# ─────────────────────────────────────────
sudo apt install ufw
# Allow the specific ports
sudo ufw allow 8082/tcp
sudo ufw allow 9000:9005/udp

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
# Final Check on connectivity from node to main node
# ─────────────────────────────────────────
echo ""
echo "Final Status:"
echo "-------------------------------------------"
echo "VM2 -> VM1 Connectivity:"
for port in 2181 9092 8080 8081 8082; do
    CHECK=$(nc -zv $VM1_IP $port 2>&1)
    if echo "$CHECK" | grep -q "succeeded"; then
        echo "  Port $port -> OK"
    else
        echo "  Port $port -> FAILED"
    fi
done



# ─────────────────────────────────────────
# Step 8: Switch to Java 21 and Build Apps
# ─────────────────────────────────────────
echo ""
echo "[8/10] Switching to Java 21 and building apps..."
sudo update-alternatives --set java /usr/lib/jvm/java-21-openjdk-amd64/bin/java
java -version 2>&1 | head -1

mvn compile quarkus:dev -Dquarkus.http.host=0.0.0.0
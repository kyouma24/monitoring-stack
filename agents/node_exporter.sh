#!/bin/bash
 
# Define variables
COMMON_DOWNLOAD_DIR="/tmp/downloads"
TEMP_DIR="/tmp"
 
# Create Node Exporter setup function
setup_node_exporter() {
    # Node Exporter setup
    echo "Setting up Node Exporter..."
 
    # Get the latest version of Node Exporter and extract
    NODE_EXPORTER_VERSION="1.10.2"
    NODE_EXPORTER_URL="https://github.com/prometheus/node_exporter/releases/download/v${NODE_EXPORTER_VERSION}/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz"
 
    # Ensure common download directory exists
    mkdir -p "$COMMON_DOWNLOAD_DIR"
 
    # Download Node Exporter to the common download directory
    echo "Downloading Node Exporter..."
    wget -P "$COMMON_DOWNLOAD_DIR" "$NODE_EXPORTER_URL"
 
    # Extract Node Exporter from the common download directory
    echo "Extracting Node Exporter..."
    tar -xvf "${COMMON_DOWNLOAD_DIR}/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz" -C "$TEMP_DIR"
 
    # Move Node Exporter binary to /usr/local/bin and remove the old version
    echo "Installing Node Exporter..."
    sudo mv "${TEMP_DIR}/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64/node_exporter" /usr/local/bin
    rm -rf "${TEMP_DIR}/node_exporter-*-linux-amd64*"
 
    # Create the Node Exporter user
    echo "Creating the Node Exporter user..."
    sudo useradd -rs /bin/false node_exporter
 
    # Create the systemd unit file for Node Exporter
    echo "Creating Node Exporter systemd unit file..."
    cat > /etc/systemd/system/node_exporter.service <<EOF
[Unit]
Description=Node Exporter
After=network.target

[Service]
User=node_exporter
Group=node_exporter
Type=simple
ExecStart=/usr/local/bin/node_exporter \
  --collector.disable-defaults \
  --collector.cpu \
  --collector.meminfo \
  --collector.diskstats \
  --collector.filesystem \
  --collector.netdev \
  --collector.netstat \
  --collector.loadavg \
  --collector.stat \
  --collector.time \
  --collector.uname \
  --collector.vmstat
  --web.disable-exporter-metrics

[Install]
WantedBy=multi-user.target

EOF
 
    # Reload systemd and enable the Node Exporter service
    echo "Reloading systemd and enabling Node Exporter service..."
    sudo systemctl daemon-reload
    sudo systemctl enable node_exporter
    sudo systemctl start node_exporter
    echo "Node Exporter service status:"
    sudo systemctl status node_exporter
}
 
# Call the Node Exporter setup function
setup_node_exporter
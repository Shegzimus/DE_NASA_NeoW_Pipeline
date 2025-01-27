#!/bin/bash

set -e

echo "Checking preexisting specifications..."
htop --version || echo "htop not installed"
git --version || echo "git not installed"
gcloud --version || echo "gcloud not installed"

echo "Updating system packages..."
sudo apt-get update && sudo apt-get upgrade -y

echo "Installing development packages..."
ANACONDA_URL="https://repo.anaconda.com/archive/Anaconda3-2024.10-1-Linux-x86_64.sh"
wget ${ANACONDA_URL} -O ~/anaconda.sh
bash ~/anaconda.sh -b -p $HOME/anaconda3
echo 'export PATH="$HOME/anaconda3/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

echo "Installing Docker..."
sudo apt-get remove -y docker docker-engine docker.io containerd runc || true
sudo apt-get install -y docker.io
sudo systemctl start docker
sudo systemctl enable docker
sudo docker run hello-world

echo "Installing Docker Compose..."
DOCKER_COMPOSE_URL="https://github.com/docker/compose/releases/download/v2.32.4/docker-compose-linux-x86_64"
mkdir -p ~/bin
wget ${DOCKER_COMPOSE_URL} -O ~/bin/docker-compose
chmod +x ~/bin/docker-compose
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
docker-compose --version

echo "Installing pgcli via Conda..."
conda install -y -c conda-forge pgcli

echo "Setup complete!"

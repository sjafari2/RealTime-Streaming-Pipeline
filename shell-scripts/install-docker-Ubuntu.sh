#!/bin/bash

# Update the package index and remove any old Docker versions
echo "Removing old Docker versions (if any)..."
sudo apt-get remove -y docker docker-engine docker.io containerd runc

# Update the package index
echo "Updating package index..."
sudo apt-get update

# Install necessary packages
echo "Installing necessary packages..."
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# Add Docker's official GPG key
echo "Adding Docker GPG key..."
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up the Docker repository
echo "Setting up Docker repository..."
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update the package index again
echo "Updating package index with Docker repository..."
sudo apt-get update

# Install Docker Engine
echo "Installing Docker Engine and related packages..."
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Allow non-root users to run Docker (optional)
echo "Adding the current user to the Docker group..."
sudo usermod -aG docker $USER

# Inform the user to log out and back in for group changes to take effect
echo "Docker installation complete. Please log out and log back in or run 'newgrp docker' for group changes to take effect."

# Test Docker installation
echo "Running Docker Hello World to verify installation..."
sudo docker run hello-world

echo "Docker is successfully installed and verified!"


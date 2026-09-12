#!/bin/bash

# Function to check if Docker is installed
check_docker_installed() {
    if [ -d "/Applications/Docker.app" ]; then
        echo "Docker is already installed."
        return 0
    else
        echo "Docker is not installed."
        return 1
    fi
}

# Function to update Docker
update_docker() {
    echo "Updating Docker using Homebrew..."
    brew upgrade --cask docker
}

# Function to install Docker
install_docker() {
    echo "Installing Docker using Homebrew..."
    brew install --cask docker
}

# Main script logic
if check_docker_installed; then
    echo "Checking for Docker updates..."
    update_docker
else
    echo "Installing Docker..."
    install_docker
fi

# Verify Docker installation
echo "Verifying Docker installation..."
docker --version

if [ $? -eq 0 ]; then
    echo "Docker is successfully installed or updated!"
else
    echo "There was an issue installing or updating Docker."
fi


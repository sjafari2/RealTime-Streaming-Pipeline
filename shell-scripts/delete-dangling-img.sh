#!/bin/bash

## Delete dangling images
dangling_images=$(docker images -f "dangling=true" -q)
if [ -n "$dangling_images" ]; then
    docker rmi $dangling_images
else
    echo "No dangling images to remove."
fi

## Delete images with tag = none
none_tagged_images=$(docker images | grep "<none>" | awk '{print $3}')
if [ -n "$none_tagged_images" ]; then
    echo $none_tagged_images | xargs docker rmi --force
else
    echo "No images with '<none>' tag to remove."
fi

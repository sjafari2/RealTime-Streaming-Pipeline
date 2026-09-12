#!/bin/bash
#To find the top largest directories in your current directory:

du -ah . | sort -rh | head -20

#To find files larger than a certain size (e.g., 100MB) and list them
find / -type f -size +100M -exec ls -lh {} \; | awk '{ print $NF ": " $5 }'
'''
use ncdu:
Simply run ncdu in the terminal to start analyzing the current directory.
You can also specify a directory, e.g., ncdu / to analyze the entire filesystem.
ncdu allows you to navigate through directories and see the space used by each file and subdirectory.
'''
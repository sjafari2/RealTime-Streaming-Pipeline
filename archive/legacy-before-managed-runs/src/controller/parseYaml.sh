#!/usr/bin/env bash

function parse_yaml {
   local prefix=${2:-}
   local s='[[:space:]]*' 
   local w='[a-zA-Z0-9_]*'
   local fs=$(echo @|tr @ '\034')

   sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p" "$1" |
   awk -F"$fs" -v prefix="$prefix" '
      {
         indent = length($1)/2;
         vname[indent] = $2;
         for (i in vname) {
             if (i > indent) {delete vname[i]}
         }
         if (length($3) > 0) {
            vn=""; 
            for (i = 0; i < indent; i++) {
                if (vname[i] == "data") continue;  # strip 'data' key automatically
                vn = vn vname[i] "_"
            }
            printf("%s%s%s=\"%s\"\n", prefix, vn, $2, $3);
         }
      }'
}


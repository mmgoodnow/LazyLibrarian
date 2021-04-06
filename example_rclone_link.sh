#!/bin/bash
if [ "$#" -ne 1 ]; then 
  	echo "USAGE: link = rclonelink.sh /path/to/filename"
else
    A=$1
    R="Dropbox"
	F=`echo $A | rev | cut -d '/' -f 1 | rev`
	rclone copy "$A" $R:
	X=`rclone link --expire 14d $R:"$F"`
	echo "$X  The link is valid for 14 days"
fi

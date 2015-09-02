#!/bin/bash

# Creates a variable CPATH from a list of all jar files in given folder.
function makeCPATH() {
    CPATH="."
    for jar in `ls $1/*.jar`; do
	CPATH=${CPATH}:$jar
    done
    export CPATH
}

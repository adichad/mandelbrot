####################################################
#
# mandelbrot.sh
# author: Aditya Varun Chadha
# email: adichad@getitinfomedia.com
# description: process management for Mandelbrot
# 
####################################################
#!/bin/bash

MANDELBROT_HOME=$(dirname $(dirname `readlink -f $0`))
echo $MANDELBROT_HOME

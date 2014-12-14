#!/bin/bash

#Make sure GNUPLOT is installed

#HOW TO USE
#1. Change the parameters below
#2. It will generate an output folder with DATA and IMAGE of each frame
#3. Take care of all the data.

#-----------PARAMETERS BEGIN ---------#
reducer_num=1;
area_size=1000;
area_num=10;
new_atomic_rate=1;
is_all_area_random=1;
random_rate=5;
hdfs_path=/user/cloudc15
#-----------PARAMETERS END ---------#


#-----init-----------------#
#-----------random input---#
rm -f input
RAND_RANGE=32767
for i in `seq 0 $(($area_num-1))`
do
	for j in `seq 0 $(($area_num-1))`
	do
		rand_x=$(($RANDOM*10/($RAND_RANGE-1)))
		rand_y=$(($RANDOM*10/($RAND_RANGE-1)))
		echo "$(($area_size*i+$rand_x)),$((j*$area_size+$rand_y))" >> input
	done
done
#-----random input done----#

#cp input4 input
max_atomics=$((area_num*area_num*area_size*area_size));
limit=$((max_atomics/5))

time_stamp=`date +%m_%d_%H_%M`
path=output_"$area_size"_"$area_num"_"$new_atomic_rate"_"$is_all_area_random"_$time_stamp
mkdir $path
hdfs dfs -mkdir $hdfs_path/$path
#cp input $path/input
hdfs dfs -rm $hdfs_path/input
hdfs dfs -put input $hdfs_path/input
ratio=(`wc -l input`)
echo $limit | tee -a $path/process_log
count=0;

#-----loop
while [ $ratio -lt $limit ]
do
	hdfs dfs -rm -r $hdfs_path/output

	#generate random atomic or not
	echo "$rand_g Frame $count start" | tee -a $path/process_log

	yarn jar build/lib/cloudProject.jar com.left.peter.atomic.AtomicSolution $hdfs_path/input $hdfs_path/output $reducer_num $area_size $area_num $new_atomic_rate $is_all_area_random $random_rate 2>>${path}/logfile | tee -a $path/process_log
	hdfs dfs -rm $hdfs_path/input
	hdfs dfs -getmerge $hdfs_path/output $path/output$count
	hdfs dfs -put $path/output$count $hdfs_path/input
	echo "set term png; set datafile separator ','; set output '${path}/sp_output${count}.png'; plot '${path}/output${count}'" | ~/gnuplot/bin/gnuplot
	ratio=(`wc -l $path/output$count`)
	echo "Atoms Number: $ratio" | tee -a $path/process_log
	echo "Frame $count END" | tee -a $path/process_log
	count=$((count+1))
done

echo $ratio | tee -a $path/process_log

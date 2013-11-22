#!/bin/sh

# This script periodically sends metric data to CloudWatch from instance.

MAXTPS=140
INVERVAL=20

outq=`mysql -u sysbench -pproject3<<EOFMYSQL
show status like 'Queries';
EOFMYSQL`

outt=`mysql -u sysbench -pproject3<<EOFMYSQL
show status like 'Uptime';
EOFMYSQL`

queries=`echo $outq | awk '{ print $4; }'`
uptime=`echo $outq | awk '{ print $4; }'`

while sleep 20;
do
	newoutq=`mysql -u sysbench -pproject3<<EOFMYSQL
	show status like 'Queries';
	EOFMYSQL`

	newoutt=`mysql -u sysbench -pproject3<<EOFMYSQL
	show status like 'Uptime';
	EOFMYSQL`

	newqueries=`echo $newoutq | awk '{ print $4; }'`
	newuptime=`echo $newoutt | awk '{ print $4; }'`


	deltaq=$((newqueries-queries-6))
	deltat=$((newuptime-uptime))
	tps=$((deltaq/deltat/16))
	tpsutil=$((tps*100/MAXTPS))

	queries=$newqueries
	uptime=$newuptime

	timestamp=$( date -u --rfc-3339=seconds )

	mon-put-data --metric-name TPSUtilization $tpsutil --namespace "DBHorizontalScaling" --unit Percent --value $tpsutil --timestamp $timestamp -I $AWS_ACCESS_KEY_ID -S $AWS_SECRET_ACCESS_KEY

	echo "Data sent: $tps, $tpsutil"
done

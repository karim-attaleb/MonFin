#!/bin/bash

access_db=$1
file=$2
tables=$3
schema=$4



#echo "\set QUIET\nCREATE SCHEMA "$schema";\n" > $file
echo "\set QUIET\n;\n" > $file
echo "BEGIN;\n" >> $file
for i in $tables
do
	echo $i
	mdb-schema --drop-table --no-relations -T $i -N $schema $access_db postgres >> $file
done
#perl -p -i -e 's|DROP TABLE |DROP TABLE IF EXISTS |g' $file
perl -p -i -e 's|BOOLEAN|INTEGER|g' $file
perl -p -i -e 's|BOOL|INTEGER|g' $file
echo "COMMIT;\n" >> $file

#for i in `mdb-tables $access_db`
for i in $tables
do
  echo $i
  lowercase=$(echo $i | tr '[:upper:]' '[:lower:]')
  echo "BEGIN;\nLOCK TABLE \""$schema"\".\""${lowercase}"\";\n" >> $file
  mdb-export -I postgres -q \' -N $schema -R "\n" $access_db $i >> $file
  echo "COMMIT;\n" >> $file
  #perl -p -i -e 's|${i}|${i,,}|g' $file
done

perl -p -i -e 's|1900-01-00 00:00:00|1900-01-01 00:00:00|g' $file


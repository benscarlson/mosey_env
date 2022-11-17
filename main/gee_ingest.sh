#!/bin/zsh

# For each study in study.csv
#   extract and format event data. rows in event_forage, but need to join to event table to get lat, lon, timestamp
#    - see if this is performant
#   save to csv
#   upload to gcs
#   upload to gee
#
#   Need to have gee environment activated

#TODO: could pass in optional parameters for upload gcs, import gee, delete csv
#TODO: still need to generalize to use any entity (population, study, etc.).
# Right now partially there but still coded to use population
#TODO: figure out how to activate pyenv gee inside the script
#TODO: figure out how to check if gee env is activated

eval "$(docopts -h - : "$@" <<EOF
Usage: gee_ingest.sh [options] <argv> ...
Options:
      --help     Show help options.
      --version  Print program version.
      --db=db    Path to the mosey database.
----
gee_ingest.sh 0.1
EOF
)"

#Fails w/in script
#setopt interactivecomments

#For testing:
# wd=~/projects/ms2/analysis/poc/mosey_env/mosey_env1
# mkdir -p $wd
# cd $wd
# sesnm=main
# geePtsP=users/benscarlson/projects/ms2/poc/mosey_env/mosey_env1
# gcsURL=gs://mol-playground/benc/projects/ms2/poc/mosey_env/mosey_env1/ingest
# db=~/projects/ms2/analysis/main/data/mosey.db
# outP=data/anno/ingest

#args are 1 based in zsh
sesnm=${argv[1]}  # The name of the session
geePtsP=${argv[2]} # The path to the gee folder that will hold the point assets
gcsURL=${argv[3]} # The gcs url to the folder that will hold csvs for import to gee
outP=${argv[4]} # The path to the folder that will hold csvs for import to gcs

#Set defaults for optional paramters
[[ -z "$db" ]] && db=data/mosey.db

# Local parameters
groupSize=500000 #Pass in as optional argument
#table="forage_event" #TODO: probably need to pass in an sql statement or point to sql file? 
entity=population #Pass in as optional argument

#Get the session id
sesid=$(sqlite3 $db "select ses_id from session where ses_name = '$sesnm' and table_name = 'population'")

mkdir -p $outP
earthengine create folder -p $geePtsP

entIds=($(mlr --csv --opprint filter '$run == 1' then cut -f pop_id ctfs/$entity.csv | tail -n +2))
names=($(mlr --csv --opprint filter '$run == 1' then cut -f name ctfs/$entity.csv | tail -n +2))

n=${#entIds[@]}

echo Loading $n $entity.

#NOTE zsh starts at 1! updated the loop, test
# use for loop to read all values and indexes
for (( i=1; i<=${n}; i++ ));
do
  #entId=30
  #entName=east_portugal
  #i=1
  entId=${entIds[$i]}
  entName=${names[$i]}
  
  echo "*******"
  echo "Start processing ${entName} ($entity id: $entId)"
  echo "*******"
  
  csv=$outP/${entName}.csv
  gcsCSV=$gcsURL/${entName}.csv
  geePts=$geePtsP/$entName
  
  #Earthengine can't do annotation with tasks greater than 1e6 points
  #So need to break them up. row_number returns 1..n. Subtract 1 so that the
  # resulting number of groups will be correct. Since groupSize is an integer,
  # the result will be cast to an integer (equivilant to floor() operation)
  
  # Latest sql. Selects by pop instead of study
  # Calculates groups according to lon, lat, and also sorts the final dataset
  # although final sorting should not matter as long as group assignment is ordered
  sql="select f.event_id as anno_id, e.lon, e.lat,
    strftime('%Y-%m-%dT%H:%M:%SZ',e.timestamp) as timestamp, 
    	(row_number() over (order by e.lon, e.lat)-1)/$groupSize as anno_grp 
    from forage_event f 
    inner join event e on f.event_id = e.event_id
    inner join forage_seg fs on f.fs_id = fs.fs_id 
    inner join segment seg on fs.seg_id = seg.seg_id
	  inner join population pop on seg.pop_id = pop.pop_id
    where pop.pop_id = $entId and pop.ses_id = $sesid
	order by anno_grp, lon, lat"
 
  echo Extracting data...

  #echo $sql
  
  /usr/bin/time sqlite3 -header -csv $db "$sql;" > $csv
  
  #Check number of rows and skip if there are none
  #If no rows, file will be empty and rows=0
  #If there are rows, file will have header so num records is rows-1
  rows=$(  cat $csv | wc -l )
  
  if [ $rows = 0 ]; then
    echo "Extracted 0 rows, skipping."
    rm -f $csv
    continue
  fi
  
  echo "Extracted $(($rows-1)) rows" #subtract the header
  
  #---- Upload file to GCS
  echo Uploading file to gcs...
  gsutil -q cp -r $csv $gcsCSV

  #---- Import file into GEE
  echo Starting GEE import task...
  earthengine upload table $gcsCSV --asset_id $geePts --x_column lon --y_column lat --force

  #---- Cleanup
  echo rm -f $csv
done

echo "Script complete"


#---- OLD CODE ----#

  #This is the old SQL I used to annotate the covid data and my schema
  # sql="select f.event_id as anno_id, f.study_id, e.lon, e.lat, 
  #     strftime('%Y-%m-%dT%H:%M:%SZ',e.timestamp) as timestamp,
  #     (row_number() over (order by f.study_id)-1)/${groupSize} anno_grp
  #   from ${table} f
  #   inner join event e
  #   on f.event_id = e.event_id
  #   where f.study_id = ${studyId}"

  #This is older sql
  #TODO: try ordering by lon,lat instead of event_id. This will tend to group
  # records geographically, which may increase performance. One task was
  # failing with memory use errors it seems b/c the wide extent of the points
  # Another approach would be to have GEE make geographic clusters.
  # sql="select f.event_id as anno_id, i.study_id, e.lon, e.lat, 
  #   strftime('%Y-%m-%dT%H:%M:%SZ',e.timestamp) as timestamp, 
  #   	(row_number() over (order by f.event_id)-1)/${groupSize} as anno_grp 
  #   from forage_event f 
  #   inner join event e on f.event_id = e.event_id
  #   inner join forage_seg fs on f.fs_id = fs.fs_id 
  #   inner join segment seg on fs.seg_id = seg.seg_id
  #   inner join individual i on seg.individual_id = i.individual_id
  #   where i.study_id = ${studyId} and fs.ses_id = ${sesid}"
  
#---- Timing various sql statements ----#

#SQL 1
  # sql="select f.event_id, f.study_id, e.lon, e.lat, strftime('%Y-%m-%dT%H:%M:%SZ',e.timestamp) as timestamp \
  #   from event_forage f \
  #   inner join ( \
  #     select event_id, lon, lat, timestamp from event where study_id = ${studyId} \
  #   ) e on f.event_id = e.event_id \
  #   where f.study_id = ${studyId}"

#Note tests with 3 million event dataset show this method is just as fast!
#Test timing when writing the full dataset to csv

#SQL 2
  # sql="select f.event_id, f.study_id, e.lon, e.lat, strftime('%Y-%m-%dT%H:%M:%SZ',e.timestamp) as timestamp
  #   from event_forage f
  #   inner join event e
  #   on f.event_id = e.event_id
  #   where f.study_id = ${studyId}"

#SQL 3
  # sql="select f.event_id, f.study_id, e.lon, e.lat, strftime('%Y-%m-%dT%H:%M:%SZ',e.timestamp) as timestamp
  #   from event_forage f
  #   inner join event e
  #   on f.event_id = e.event_id
  #   where f.study_id = ${studyId} and e.study_id = ${studyId}"
  
#SQL 2 is simplist and is just as fast so use that query.
  #10763606 SQL 1 = 0.75 real
  #10763606 SQL 2 = 0.75 real
  #8863543 SQL 1 = 18.69 real
  #8863543 SQL 2 = 8.90 real
  #8863543 SQL 1 = 9.40 real
  #8863543 SQL 3 = 9.18 real

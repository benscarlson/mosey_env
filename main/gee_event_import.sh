# For each study in study.csv
#   extract and format event data. rows in event_forage, but need to join to event table to get lat, lon, timestamp
#    - see if this is performant
#   save to csv
#   upload to gcs
#   upload to gee

#argv[0] <geePtsP> the path to the folder that will hold the gcs assets
#argv[1] <gcsURL> the gcs url to the folder that will hold csvs for import to gee
#argv[2] <db> the path to the mosey database
#argv[3] <outP> the path to the folder that will hold csvs for import to gcs
#TODO: make sesid optional
#argv[4] <sesid> the session id for the event_forage table

eval "$(docopts -h - : "$@" <<EOF
Usage: mosey_anno_gee.sh [options] <argv> ...
Options:
      --help     Show help options.
      --version  Print program version.
----
gee_event_import.sh 0.1
EOF
)"

#TODO: could pass in optional parameters for upload gcs, import gee, delete csv

#Need to have gee environment activated
#TODO: figure out how to run this from inside the script. check to see if already activated.
#pyenv activate gee

#geePtsP=users/benscarlson/projects/ms3/tracks
#gcsURL=gs://mol-playground/benc/ingest_ee
#db=~/projects/ms3/analysis/full_workflow_poc/data/mosey.db
#outP=data/anno/ingest_gee

geePtsP=${argv[0]}
gcsURL=${argv[1]}
db=${argv[2]}
outP=${argv[3]}

#TODO: need to make sesid optional
#sesid=${argv[4]}

groupSize=1000000
#TODO: Pass in as optional argument.
table="event_clean"

# echo $geePtsP
# echo $gcsURL
# echo $db
# echo $outP

mkdir -p $outP
#TODO: make sure geePtsP exists on GEE
#TODO: make sure gcsURL exists on GCS (maybe don't need to?)

# Use miller to filter by run column and then take the study_id field
# need to use tail to remove first line, which is the header
# study ids have \r suffix. Need to remove these
studyIds=($(mlr --csv --opprint filter '$run == 1' then cut -f study_id ctfs/study.csv | tail -n +2))
#echo ${studyIds[@]} | cat -ve 
studyIds=( ${studyIds[@]%$'\r'} ) # remove \r suffix

echo Loading ${#studyIds[@]} studies.

for studyId in "${studyIds[@]}"
do 
  echo "*******"
  echo "Start processing study ${studyId}"
  echo "*******"
  
  #studyId=10763606 #LifeTrack White Stork Poland (419 rows)
  #studyId=8863543 #HUJ MPIAB White Stork E-Obs (3 million rows)
  
  #Reading study ids from csv results in \r at end. This removes them.
  #studyId=${studyId%$'\r'}
  
  csv=$outP/${studyId}.csv
  gcsCSV=$gcsURL/${studyId}.csv
  geePts=$geePtsP/$studyId
  
  #Earthengine can't do annotation with tasks greater than 1e6 points
  #So need to break them up. row_number returns 1..n. Subtract 1 so that the
  # resulting number of groups will be correct. Since groupSize is an integer,
  # the result will be cast to an integer (equivilant to floor() operation)
  sql="select f.event_id as anno_id, f.study_id, e.lon, e.lat, 
      strftime('%Y-%m-%dT%H:%M:%SZ',e.timestamp) as timestamp,
      (row_number() over (order by f.study_id)-1)/${groupSize} anno_grp
    from ${table} f
    inner join event e
    on f.event_id = e.event_id
    where f.study_id = ${studyId}"

#TODO: need to make sesid optional
# and f.ses_id = '${sesid}'
 
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

#Runs annotation script for each study in study.csv and environmental variable 
# in envs array

#argv[0]  <geePtsP> Folder holding the gee point datasets
#argv[1]  <gcsOutP> Path to the output folder for annotated csvs (excluding bucket)
## TODO: make optional: argv[1]  <envs> Array of environmental variables. Need to pass in envs as expanded 
# list (e.g. "env1 env2 env3"). Pass like this: "${envs[*]}"

eval "$(docopts -h - : "$@" <<EOF
Usage: mosey_anno_gee.sh [options] <argv> ...
Options:
      --help     Show help options.
      --version  Print program version.
----
mosey_anno_gee 0.1
EOF
)"

#For testing
#geePtsP=users/benscarlson/projects/covid/tracks
#gcsOutP=benc/projects/covid/annotated

#How to pass array to bash script
#https://stackoverflow.com/questions/17232526/how-to-pass-an-array-argument-to-the-bash-script
geePtsP=${argv[0]}
gcsOutP=${argv[1]}
#TODO: make this an optional argument. If passed in, don't read envs.csv
#TODO: don't allow passing in multiple, to keep it simple. multiple, use envs.csv
#envs=(${argv[2]}) 

# Use miller to filter by run column and then take the study_id field
# as well as env information
# need to use tail to remove first line, which is the header

#----Load variables from control files

#study.csv
studyIds=($(mlr --csv --opprint filter '$run == 1' then cut -f study_id ctfs/study.csv | tail -n +2))

#envs.csv
envs=($(mlr --csv --opprint filter '$run == 1' then cut -f env_id ctfs/env.csv | tail -n +2))
bands=($(mlr --csv --opprint filter '$run == 1' then cut -f band ctfs/env.csv | tail -n +2))
colnames=($(mlr --csv --opprint filter '$run == 1' then cut -f col_name ctfs/env.csv | tail -n +2))

# Remove \r suffix
studyIds=( ${studyIds[@]%$'\r'} )
envs=( ${envs[@]%$'\r'} )
bands=( ${bands[@]%$'\r'} )
colnames=( ${colnames[@]%$'\r'} )

echo Annotating ${#studyIds[@]} studies.

for studyId in "${studyIds[@]}"
do 
  echo "*******"
  echo "Start processing study ${studyId}"
  echo "*******"
  
  #studyId=10763606 #LifeTrack White Stork Poland (419 rows)
  #studyId=8863543 #HUJ MPIAB White Stork E-Obs (3 million rows)
  #studyId=${studyIds[0]}
  points=$geePtsP/$studyId
  
  # get length of an array
  n=${#envs[@]}

  # use for loop to read all values and indexes
  for (( i=0; i<${n}; i++ ));
  do
  
    #i=0
    
    #TODO: do this as default if user doesn't pass in col_name info
    #envN=${env##*/} #gets the name (w/o path) of the env variable
    
    #TODO: check to see if $points exists in gee before annotating
    # earthengine asset info $points
    # earthengine asset info x
    # return_value=$?
    
    #TODO: need to handle band, colname as optional parameters
    # if column is not present don't pass parameters

    #echo "index: $i, env: ${envs[$i]}, band: ${bands[$i]}, col name: ${colnames[$i]}"
    out=$gcsOutP/${studyId}_${colnames[$i]} #do not include url, bucket, or file extension
    
    echo Annotating "env: ${envs[$i]}, band: ${bands[$i]}, col name: ${colnames[$i]}"
    $MOSEYENV_SRC/main/anno_gee.r $points ${envs[$i]} $out -b ${bands[$i]} -c ${colnames[$i]}
  done

done
#!/usr/bin/env Rscript --vanilla
# chmod 744 script_template.r #Use to make executable

# This script implements the breezy philosophy: github.com/benscarlson/breezy

# RUN AT COMMAND LINE
#   pyenv activate gee

#TODO: default to using the band name as the output column name of the env variable
#TODO: add optional parameter to not start task (for debugging)
# ==== Breezy setup ====

'
Template

Usage:
anno_gee.r <dat> <env> <out> [--band=<band>] [--envcol=<envcol>] [--db=<db>] [--seed=<seed>] [--groups=<groups>] [--npts=<npts>]
anno_gee.r (-h | --help)

Control files:

Parameters:
  dat: path to gee feature collection
  env: path to gee environmental asset
  out: path to output directory on gcs. just directory, no url info or bucket

Options:
-h --help     Show this screen.
-v --version     Show version.
-b --band=<band>  Band of the asset to annotate. Defaults to: 0
-c --envcol=<envcol>  Name of column for environmental variable. Defaults to: env
-g --groups=<groups>  Run for only the specified groups. Useful for testing. Defaults to all groups.
-n --npts=<npts> Run on a subset of the first n points. Useful for testing. 
-s --seed=<seed>  Random seed. Defaults to 5326 if not passed
' -> doc

#---- Input Parameters ----#
if(interactive()) {

  .wd <- '~/projects/project_template/analysis'
  .seed <- NULL
  rd <- here::here
  
  #Required parameters
  .envPF <- 'projects/map-of-life/diegoes/dist2road_USA_full' # 'NASA/ORNL/DAYMET_V4'
  .datPF <- 'users/benscarlson/projects/covid/tracks/619097045'
  .outPF <- 'benc/projects/covid/annotated/619097045_dist2road_test'
  
  #Optional parameters
  .band <- 0 #4
  .colEnv <- 'dist2road' #  'tmax'
  .groups <- 2 #only run for these groups. handy for testing
  .npts <- 10
} else {
  suppressWarnings(
    suppressPackageStartupMessages({
        library(docopt)
        library(rprojroot)}))

  ag <- docopt(doc, version = '0.1\n')

  .wd <- getwd()
  .script <-  suppressWarnings(thisfile())
  .seed <- ag$seed
  rd <- is_rstudio_project$make_fix_file(.script)
  
  source(rd('src/funs/input_parse.r'))
  
  #Required parameters
  .envPF <- ag$env
  .datPF <- ag$dat
  .outPF <- ag$out
  
  #Optional parameters
  .band <- ifelse(is.null(ag$band),0,as.integer(ag$band))
  .colEnv <- ifelse(is.null(ag$envcol),'env',ag$envcol)
  
  #ifelse can't return NULL, which is annoying
  if(is.null(ag$groups)) {.groups <- NULL } else {.groups <- as.integer(parseCSL(ag$groups))}
  if(is.null(ag$npts)) {.npts <- NULL} else {.npts <- as.integer(ag$npts)}
    
}

message("Setting up annotation tasks...")

#---- Initialize Environment ----#
if(!is.null(.seed)) {message(paste('Random seed set to',.seed)); set.seed(as.numeric(.seed))}

t0 <- Sys.time()

source(rd('src/startup.r'))

suppressWarnings(
  suppressPackageStartupMessages({
    library(rgee)
  }))

#Source all files in the auto load funs directory
list.files(rd('src/funs/auto'),full.names=TRUE) %>% walk(source)

#Initialize gee
suppressMessages(ee_check(quiet=TRUE))
ee_Initialize(quiet=TRUE)

#TODO: do a check to make sure rgee initialized correctly

#---- Local parameters ----#
#TODO: I might not need some of these variables anymore
.colImageId <- 'image_id';
.colMillis <- 'millis'
.colTimestamp <- 'timestamp'
.colGrp <- 'anno_grp'
.bucket <- 'mol-playground'
datN <- basename(.datPF)
assetType <- ee$data$getAsset(.envPF)$type


#---- Load data ----#

pts <- ee$FeatureCollection(.datPF)

if(is.null(.groups)) {
  #Groups run from 0...n, so to get number of groups need to add 1
  maxgrp <- pts$aggregate_max(.colGrp)$getInfo()
  groups <- 0:maxgrp
} else {
  groups <- .groups
  message(glue('Running only for groups {groups}'))
}

if(length(groups)>1) message(glue('Splitting annotation into {length(groups)} tasks'))

#====

#---- Perform analysis ----#

for(group in groups) {
  
  #group <- 2
  ptsGrp <- pts$filter(ee$Filter$eq(.colGrp,group))
  
  if(!is.null(.npts)) {
    message(glue('Running for a subset of {.npts} points'))
    ptsGrp <- ee$FeatureCollection(ptsGrp$toList(.npts))
  }

  #Code for testing to reduce size in groups
  #ptsGrp <- ee$FeatureCollection(ee$Feature(ptsGrp$first()))
  #00030000000000056e8f does not return a value for dist2road. Note the property is not there after annotation.
  #ptsGrp <- ptsGrp$filter(ee$Filter$eq('system:index','00030000000000056e8f'))
  
  if(assetType=='IMAGE') {
    
    env <- ee$Image(.envPF)$select(list(.band))
    
    anno <- env$reduceRegions(
      reducer = ee$Reducer$median()$setOutputs(list(.colEnv)),
      collection = ptsGrp,
      scale = env$projection()$nominalScale()
    )
    
  } else if(assetType=='IMAGE_COLLECTION') {
    
    env <- ee$ImageCollection(.envPF)
    
    ptsGrp <- ptsGrp$map(function(f) {
      mil = ee$Date(f$get(.colTimestamp))$millis()
      f <- f$set(.colMillis,mil)
      return(f)
    })
    
    filter <- ee$Filter$And(
      ee$Filter$lessThanOrEquals('system:time_start', NULL, .colMillis),
      ee$Filter$greaterThan('system:time_end', NULL, .colMillis)
    )
    
    joined <- ee$Join$saveAll('features')$apply(env, ptsGrp, filter)
    
    anno <- joined$map(function(img) {
      img <- ee$Image(img)$select(list(.band))
      
      fc <- ee$FeatureCollection(ee$List(img$get('features')))
      
      vals <- img$reduceRegions(
        reducer=ee$Reducer$median()$setOutputs(list(.colEnv)),
        scale=img$projection()$nominalScale(),
        collection=fc)
      
      #TODO: see if this affects performance (map w/in map)
      vals <- vals$map(function(f) {
        f$set(.colImageId,img$get('system:index'))
      })
      
      return(vals)
    })$flatten()
    
  } else {
    stop(glue('Invalid asset type: {assetType}'))
  }
  
  anno <- anno$sort('anno_id')
  #anno$getInfo(); quit()
  
  task <- ee$batch$Export$table$toCloudStorage(
    collection=anno,
    description=glue('{datN}_{.colEnv}_{group}'),
    bucket=.bucket,
    fileNamePrefix=glue('{.outPF}_{group}'),
    fileFormat='csv',
    selectors=list('anno_id',.colEnv)
  )
  
  task$start()
  message(glue('Task for group {group} started'))
  message(glue('Results will be saved to gs://{.bucket}/{.outPF}_{group}.csv'))

}
#---- Finalize script ----#
message(glue('Script complete in {diffmin(t0)} minutes'))
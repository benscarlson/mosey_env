#' Based on JRC Monthly history
#' Data available 1984-03-16 - 2021-01-01
#' Implementation of a computed layer
#' This should be an S4-based oo design but for now I'm just loading the functions
#' directly into the environment
#' 
#' Should have the following two functions:
#'  getAssetType() Returns IMAGE or IMAGE_COLLECTION
#'  getLayer() Returns an image or image collection representing the layer
#'    Note that images from the layer need to return the correct espg and scale

getAssetType <- function() {return('IMAGE_COLLECTION')}

getLayer <- function() {

  water <- ee$ImageCollection("JRC/GSW1_3/MonthlyHistory")

  # Fills in data gaps with previous year, and then next year data
  # If image is from the first or last year in the collection, only the 
  # next or previous year is used to fill
  
  distIC <- water$map(function(img){
  
    yr <- ee$Number(img$get('year'))
    mth <- ee$Number(img$get('month'))
    
    #extract the previous and next year from the full water dataset
    #turn this into a list and sort by year, just to make sure
    # that previous year is applied first.
    fillIC <- water$filter(ee$Filter$And(
      ee$Filter$eq('month',mth),
      ee$Filter$inList('year',list(yr$subtract(1),yr$add(1)))))$
       sort('year')
    
    fillLS <- fillIC$toList(fillIC$size())
    
    #Only fill pixels where there is nodata
    nodata <- img$eq(0)
    
    filled <- fillLS$iterate(ee_utils_pyfunc(function(fill,img){
      
      fill <- ee$Image(fill)
      img <- ee$Image(img)
      
      return(img$max(fill$updateMask(nodata)$unmask(0)))
      
    }),img)
    
    # 0: No data
    # 1: Not water
    # 2: Water
    filled <- ee$Image(filled) #copyProperties returns Element, so need to cast
  
    # make a mask for deep water and for areas where there is no data. 
    # deep water is already masked out so this remains masked
    # masking no data is mainly to 
    # account for lots of missing data in the north in the colder months
    # remember a pixel gets masked if the mask that is applied has a 
    # pixel that is 0 or is masked
    wmask <- filled$neq(0)
    
    #Perform distance calculation
    dist <- filled$
      eq(2)$ #This makes a binary water/not water layer
      fastDistanceTransform(1000)$sqrt()$
      #clip(region)$
      multiply(ee$Image$pixelArea()$sqrt())$
      updateMask(wmask)
  
    #Set properties
    # time_end is calculated by adding 1 month to time_start
    # this is how time-series mosiaces in GEE represent start and end times
    # this means image with 2018-07-01 will have time end 2018-08-01
    # so a timestamp matches this image if it is <= time_start, but < time_end
    # 2018-07-01 <= timestamp < 2018-08-01
    dist <- dist$
      copyProperties(img,list('system:time_start','month','year'))$
      set('system:time_end',
          ee$Date(img$get('system:time_start'))$
            advance(1,'month')$
            millis())
    
    return(dist)
  })

  return(distIC)
}

#View(distIC$first()$getInfo())
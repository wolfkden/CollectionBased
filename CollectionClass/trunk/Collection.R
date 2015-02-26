setClass("CollectionBase",
representation(
bucketName="character" 
, awsAccessKey="character" 
, awsSecretAccessKey="character"
)
, package="Collection"
)

testLocal <- function(fn, data) {
btime<-Sys.time()
od<-lapply(data, fn)
etime<-Sys.time()
print(etime-btime)
od
}
setMethod(
"initialize"
, "CollectionBase"
, function(.Object) {
yml<-yaml.load_file("jobspec.yml")
.Object@bucketName<-yml$':bucket'
.Object@awsAccessKey<-yml$':access_key_id'
.Object@awsSecretAccessKey<-yml$':secret_access_key'
.Object
}
)


map<-function(func=NULL, data=NULL) {
dateExt<-gsub(" ", "-", date())
func<-if(is.null(func) || typeof(func) != "closure") function(x) x else func
funcFile <- gsub(" ", "", paste("function.file.", dateExt, "")) 
save(func, file=funcFile, compress=FALSE, ascii=TRUE)
funcName<-load(funcFile)
dataFile <- gsub(" ", "", paste("data_", dateExt, ""))
if(is.null(data) || typeof(data) == "double" && length(data) == 1) system(paste("touch", dataFile, " ")) else write(data, file=dataFile)
jobMapScript <- "ruby job_mapper.rb"
system(paste("ruby loadFileS3.rb --data", dataFile, "--code", funcFile, " "))
jobMapScript <- if(typeof(data) == "double" && length(data) == 1) paste(jobMapScript, "--r_array_size", data, "--r_function", funcName, " ") else jobMapScript
jobMapScript
system(jobMapScript)
system("ruby aggregate_results.rb")
scan(gsub(" ", "", paste("output/", dataFile, "_output", "")))
}



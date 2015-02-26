setClass("AwsWorker"
, representation(
  sequence="numeric"
, type="character"
, status="character"
, collection_size="numeric"
, created="POSIXlt"
, start="POSIXlt"
, finish="POSIXlt"
, serial_id="character"
, s3_file="character"
, s3_bucket="character"
, worker_id="character"
, worker_name="character"
, ec2_instance_id="character"
)
)

setMethod("initialize", "AwsWorker"
, function(.Object, message, type) {
.Object <- callNextMethod()
.Object@type=type
.Object@status=message$"right_grid_status"
.Object@collection_size=if(is.null(message$":collection_size")) 0 else message$":collection_size"
.Object@created=as.POSIXlt(message$":created_at")
.Object@start=as.POSIXlt(message$":starttime")
.Object@finish=as.POSIXlt(message$":finishtime")
.Object@serial_id=message$":serial"
.Object@sequence=as.integer(strsplit(message$":serial", "_")[[1]][2])
s3_file<-message$":s3_download"[[1]]
.Object@s3_bucket=strsplit(s3_file, "/")[[1]][1]
.Object@s3_file=""
for(x in strsplit(s3_file, "/")[[1]][-1]) .Object@s3_file<-if(.Object@s3_file == "") x else paste(.Object@s3_file, x, sep="/")
.Object@worker_id=message$"work_item_id"
.Object@worker_name=message$":worker_name"
.Object@ec2_instance_id=message$":audit_info"$":ec2_instance_id"
return(invisible(.Object))
})

setClass("CollectionBase"
, representation(
  awsAccessKey="character" 
, awsSecretAccessKey="character"
, awsBucket="character"
, awsBaseBucket="character"
, jobSpec="character"
, YAMLdispatch="character"
, YAMLcollection="character"
, MachineID="character"
, SessionID="character"
)
, prototype=prototype(
  awsAccessKey=character(0)
, awsSecretAccessKey=character(0)
, awsBucket=character(0)
, jobSpec=character(0)
, YAMLdispatch=character(0)
, YAMLcollection=character(0)
, MachineID=system("cat /var/lib/dbus/machine-id", intern=TRUE)
, SessionID=paste("s", as.character(Sys.getpid()), sep="")
)
)

setClass("Collection"
, representation(
  identifier="character"
, length="numeric"
, instantiated="POSIXlt"
, dirty="POSIXlt"
, fixed="POSIXlt"
, func = "list"
, workers="list"
, collection="list"
, worker = "AwsWorker"
)
, prototype=prototype(
  identifier=paste("cid", format(Sys.time(), "%Y%m%d_%H%M%0S3"), sep="")
, length=0
, instantiated=NULL
, dirty=NULL
, fixed=NULL
, func=list()
, workers=list()
, collection=list()
, worker=NULL
)
, contains=c("CollectionBase")
)

setMethod("initialize", "Collection"
, function(.Object, collection=c(), worker=NULL) {
.Object <- callNextMethod()
.Object@collection = as.list(if(typeof(collection) == "list") collection else c(collection))
dataList="";for(item in .Object@collection)
dataList<-paste(dataList, if(class(item) == "Collection")
paste("identifier=", item@identifier, ", ", "") else
if(class(item) == "numeric") paste("length=", as.character(item), ", ", ""))
.Object@identifier<-newObjectIdentifier(.Object)
#<-paste("cid", format(Sys.time(), "%Y%m%d_%H%M%0S3"), sep="") #paste("cid", format(Sys.time(), "%Y:%m:%d-%H:%M:%0S3"), sep="")

if(!is.null(worker)) .Object<-setWorkers(.Object, worker)

return(invisible(.Object))
})

setMethod("initialize", "CollectionBase"
, function(.Object) {
.Object <- callNextMethod()
yml<-yaml.load_file("jobspec.yml")
.Object@awsBucket<-yml$':bucket'
.Object@awsBaseBucket<-yml$':bucket_base'
.Object@awsAccessKey<-yml$':access_key_id'
.Object@awsSecretAccessKey<-yml$':secret_access_key'
.Object@YAMLdispatch<-yml$':task_dispatch_list'
.Object@YAMLcollection<-yml$':task_processed_list'

return(invisible(.Object))
})

setGeneric("setWorkers", function(.Object, worker=NULL) { standardGeneric("setWorkers") })
setMethod("setWorkers", "Collection", function(.Object, worker=NULL) {

collectionFile=paste(.Object@MachineID, .Object@SessionID, .Object@identifier, .Object@YAMLcollection, sep="/")

if(!is.null(worker))
.Object@worker=new("AwsWorker", worker$':message', worker$':type')
else if(file.exists(collectionFile)) {
collectionList<-yaml.load_file(collectionFile)
.Object@workers<-list(); cnt=1
for(task in collectionList) { #print(paste("set worker", cnt, task$":message"$":serial"))
baseWorker = if(length(.Object@collection) < 1) NULL else .Object@collection
baseWorker = if(class(baseWorker[[1]]) != "Collection" || length(baseWorker[[1]]@workers) < 1 || is.null(baseWorker[[1]]@workers[[task$":message"$":serial"]])) 
.Object@workers[[task$":message"$":serial"]] <-
new("Collection", c(), task)
 else
.Object@workers[[task$":message"$":serial"]] <-
new("Collection", baseWorker[[1]]@workers[[task$":message"$":serial"]], task)

cnt = cnt + 1
}}

.Object@instantiated = as.POSIXlt(Sys.time())

return(invisible(.Object))
})

setGeneric("clone", function(.Object, collection=c(), seq=NULL)
{ standardGeneric("clone") })
setMethod(
"clone"
, "Collection"
, definition=function(.Object, collection=c(), seq=NULL) {

collection <- if(length(collection) < 1) .Object@collection else collection

clone<-new("Collection", collection)

if(!is.null(seq)) clone@identifier = paste(clone@identifier, seq, sep="_")

if(!is.null(.Object@worker))
clone@worker <- .Object@worker

clone@workers <- .Object@workers

clone@func <- .Object@func

return(invisible(clone))
})

setGeneric("newObjectIdentifier", function(.Object) { 
standardGeneric("newObjectIdentifier") })
setMethod(f="newObjectIdentifier", signature=c("Collection")
, definition=function(.Object) {

return(paste("cid", format(Sys.time(), "%Y%m%d_%H%M%0S3"), sep=""))
})

setGeneric("objectDir", function(.Object, func=NULL) { 
standardGeneric("objectDir") })
setMethod(f="objectDir", signature=c("Collection")
, definition=function(.Object, func=NULL) {

directory<-paste(.Object@MachineID, .Object@SessionID, .Object@identifier, sep="/")
if(length(grep(.Object@identifier, list.files(paste(.Object@MachineID, .Object@SessionID, sep="/")))) < 1)
dir.create(paste(.Object@MachineID, .Object@SessionID, .Object@identifier, sep="/"), recursive=TRUE)

return(directory)
})

setGeneric("writeDispatch", function(.Object, dispatchP) { standardGeneric("writeDispatch") })
setMethod(f="writeDispatch", signature=c("Collection")
, definition=function(.Object, dispatchP) {
if(is.null(dispatchP) || length(dispatchP) < 1) return(invisible(.Object))

close(file(.Object@YAMLdispatch, "a"))
dispatchList <-yaml.load_file(.Object@YAMLdispatch)
if(is.null(dispatchList) || class(dispatchList) != "list") dispatchList<-list()
dispatchList[[paste(":", .Object@MachineID, sep="")]][[paste(":", .Object@SessionID, sep="")]][[paste(":", .Object@identifier, sep="")]]<-dispatchP
#print(dispatchList)
write(as.yaml(dispatchList), .Object@YAMLdispatch)

return(invisible(.Object))
})

setGeneric("setFunc", function(.Object, func=NULL) { standardGeneric("setFunc") })
setMethod("setFunc", signature=c("Collection")
, definition=function(.Object, func=NULL) {
if(is.null(.Object@collection) || length(.Object@collection) < 1) return(invisible(.Object))

.Object@func<-if(is.null(func) || length(func) < 1) {
assign("identity", as.function(function(x) x), pos=sys.frame());
funcTree(identity, sys.frame()) } else funcTree(func)

return(invisible(.Object))
})

setGeneric("dispatch", function(.Object) { standardGeneric("dispatch") })
setMethod(f="dispatch", signature=c("Collection")
, definition=function(.Object) {
dispatchPointer <- list()
if(is.null(.Object@collection) || length(.Object@collection) < 1 ||
length(.Object@workers) < 1) return(invisible(.Object))

#print(paste("Mapping collection count:", length(.Object@collection), "Worker count:", length(.Object@workers)))

dispatchPointer <- list()
for(unit_id in names(.Object@workers)) {
item<-.Object@workers[[unit_id]]
#print(paste("collection id:", unit_id, "item class", class(item)))
incr = as.integer(gsub("^:\\w*\\d*_", "", unit_id))
clonedWorker<-clone(item, item, incr)
clonedWorker@func = .Object@func
#print(paste("cloned object", class(clonedWorker)))
ftaskFile<-paste("function.file.", names(.Object@func)[1], "_", clonedWorker@identifier, sep="")
save(writeTree, clonedWorker, file=paste(objectDir(.Object), ftaskFile, sep="/"), 
compress=FALSE, ascii=TRUE)
dispatchPointer[[paste(":", clonedWorker@identifier, sep="")]]$collectionFunction = c(ftaskFile)
dispatchPointer[[paste(":", clonedWorker@identifier, sep="")]]$collectionS3 = c(item@worker@s3_file)
#print(dispatchPointer[[clonedWorker@identifier]])
}

writeDispatch(.Object, dispatchPointer)
print("Wrote Dispatch")

return(invisible(.Object))
})

setGeneric("map", function(.Object, func=NULL, collection=NULL) { standardGeneric("map") })
setMethod(f="map", signature=c("Collection")
, definition=function(.Object, func=NULL, collection=NULL) {
if(length(.Object@workers) < 1) return(.Object)

.Object<-generate(.Object)

clonedObject<-clone(.Object, .Object)

clonedObject<-setFunc(clonedObject, as.character(as.list(match.call())$func))

clonedObject<-dispatch(clonedObject)

system("ruby collection_mapper.rb")

clonedObject<-setWorkers(clonedObject)

print(paste("Input Object ID:", .Object@identifier, "collection count:", length(.Object@collection),  "worker count:", length(.Object@workers)))

print(paste("Mapped Object ID:", clonedObject@identifier, "collection count:", length(clonedObject@collection),  "worker count:", length(clonedObject@workers)))

#if(1 == length(as.character(as.list(match.call())$.Object)))
#assign(as.character(as.list(match.call())$.Object), .Object, pos=sys.frame())

return(invisible(clonedObject))
})

setGeneric("generate", function(.Object, func=NULL) { 
standardGeneric("generate") })
setMethod(f="generate", signature=c("Collection")
, definition=function(.Object, func=NULL) {

if(is.null(.Object@collection) || class(.Object@collection) != "list" || length(.Object@collection) < 1) return(invisible(.Object))

if(1 < length(.Object@collection)) {
for(it in 1:length(.Object@collection))
if(class(.Object@collection[[it]]) == "numeric") {
.Object@collection[[it]] <- generate(new("Collection", .Object@collection[[it]]))
i=0; for(c in 1:2^21) i<-i+1;
}
return(invisible(.Object))
}

if(class(.Object@collection[[1]]) != "numeric") return(invisible(.Object))

dispatchPointer <- list()
size<-.Object@collection[[1]]
if(size < 1) { .Object@collection=c(); return(invisible(.Object)) }
print(paste("function name:", as.character(as.list(match.call())$func)))
.Object<-setFunc(.Object, as.character(as.list(match.call())$func))

incr<-floor((size+1023)/1024); fincr=0;
for(base in seq(from=1, to=size, by=incr)) {
print(
genFunc<-eval(parse(text=paste("function() c(", base, ":", min(base+incr-1, size), ")", sep=""))))
clonedWorker<-clone(.Object, genFunc, fincr)
ftaskFile<-paste("function.file.", names(.Object@func)[1], "_", clonedWorker@identifier, sep="")
save(writeTree, clonedWorker, file=paste(objectDir(.Object), ftaskFile, sep="/"), compress=FALSE, ascii=TRUE)
dispatchPointer[[paste(":", clonedWorker@identifier, sep="")]]$collectionFunction = c(ftaskFile)
dispatchPointer[[paste(":", clonedWorker@identifier, sep="")]]$collectionData =
c(paste("data.file.", clonedWorker@identifier, sep=""))
fincr=fincr+1
}

writeDispatch(.Object, dispatchPointer)

system("ruby collection_mapper.rb")
#system("ruby collection_dispatcher.rb")
#system("ruby collection_collector.rb")

.Object<-setWorkers(.Object)

.Object@collection[[1]]=clone(.Object)
if(1 == length(as.character(as.list(match.call())$.Object)))
assign(as.character(as.list(match.call())$.Object), .Object, pos=sys.frame())

return(invisible(.Object))
})

setMethod(f="[", "Collection"
, function(x, i, j, drop) {
if(length(x@collection) < 1) return(NULL)
return(x@collection[[i]])
}
)
setReplaceMethod(f="[", "Collection"
, function(x, i, j, value) {
x@collection[[i]]<-value
return(x)
}
)
setMethod(f="[[", "Collection"
, function(x, i, j, drop) {
if(length(x@collection) < 1) return(NULL)
return(x@collection[[i]])
}
)
setReplaceMethod(f="[[", "Collection"
, function(x, i, j, value) {
x@collection[[i]]<-value
return(x)
}
)

setValidity("Collection", function(object) { return(0 < length(object@identifier)) })

testLocal <- function(fn, data) {
btime<-Sys.time()
od<-lapply(data, fn)
etime<-Sys.time()
print(etime-btime)
od
}


funcSearch<-function(x, fTree)
{
  rval <- ""
  for(fi in names(fTree)) if(x == fi || x == funcSearch(x, fTree[[fi]])) rval<-x

  return(rval)
}
writeTree<-function(fTree, envir=sys.frame())
{
  if(!is.list(fTree) || length(fTree) < 1) return()

  for(tag___ in names(fTree)) if(regexpr("___$", tag___) < 0)
  {
     writeTree(fTree[[tag___]], envir)
     print(tag___)
     f_alist___<-as.list(fTree[[tag___]]$formals___)
     f_alist___[[length(f_alist___)+1]]<-fTree[[tag___]]$body___
     assign(tag___, as.function(as.list(f_alist___)), pos=envir)
     tag___
  }
}
funcTree<-function(fn, last=list(), envir=sys.frame())
{
  require('codetools')
  fname___<-if(is.character(fn)) fn else as.character(as.list(match.call())$fn)
  print(fname___)

  fn<-eval(as.name(fname___), envir=envir)
  print(typeof(fn))
  if(typeof(fn) != "closure") return(NULL)
  ll<-list()
  ll[[fname___]]<-list()
  ll$last___<-last
  ll[[fname___]]$formals___<-formals(fn)
  ll[[fname___]]$body___<-body(fn)

  top<-ll
  while(0 < length(top$last___)) top<-top$last___
  for(fi in as.list(findGlobals(fn)))
  if(fi != funcSearch(fi, top) && !is.null(li<-funcTree(fi, ll)))
  { print(fi); ll[[fname___]][[fi]]<-li[[fi]] }

  return(ll)
}
timeThis<-function(...) {
start.time<-Sys.time();
eval(..., sys.frame(sys.parent(sys.parent())));
end.time<-Sys.time();
print(end.time-start.time)
}

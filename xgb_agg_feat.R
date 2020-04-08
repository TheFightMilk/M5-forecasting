##### Aggregate sales as features #####
library(data.table)
library(xgboost)
library(caret)
library(forecast)
library(forecTheta)
library(smooth)
library(doParallel)


set.seed(21081991)
# setDTthreads(11)
####
free <- function() invisible(gc())
viewdt <- function(dt,n=10){View(head(dt,n))}

#### Pre-processing #### 
## Constants
h <- 28
max_lags <- 366
tr_last <- 1913
fday <- as.IDate("2016-04-25") 

## Load and Merge ##
prices <- fread("m5-forecasting-accuracy/sell_prices.csv")
cal <- fread("m5-forecasting-accuracy/calendar.csv")[,date:=as.IDate(date,format="%Y-%m-%d")]
dt <- fread("m5-forecasting-accuracy/sales_train_validation.csv")
dt[, paste0("d_", (tr_last+1):(tr_last+2*h)) := NA_real_]

# wide to long
dt <- melt(dt,
           measure.vars = patterns("^d_"),
           variable.name = "d",
           value.name = "sales")

# merge to calendar
dt[cal,`:=`(date=i.date,
            wm_yr_wk=i.wm_yr_wk,
            event_name_1=i.event_name_1,
            snap_CA=i.snap_CA,
            snap_TX=i.snap_TX,
            snap_WI=i.snap_WI),on="d"]

# merge to prices
dt[prices, sell_price := i.sell_price, on=c("store_id", "item_id", "wm_yr_wk")]
# table(dt$event_name_1)

# encode categorical variables
free()
cat.enc <- dt[,c("id","date","item_id","dept_id","cat_id","state_id","store_id","sales","event_name_1")]

cat.enc[, item_id_tgt := mean(sales,na.rm=T),by="item_id"]
cat.enc[, dept_id_tgt := mean(sales,na.rm=T),by="dept_id"]
cat.enc[, cat_id_tgt := mean(sales,na.rm=T),by="cat_id"]
cat.enc[, store_id_tgt := mean(sales,na.rm=T),by="store_id"]
cat.enc[, state_id_tgt := mean(sales,na.rm=T),by="state_id"]
cat.enc[, event_name_tgt := mean(sales,na.rm=T),by="event_name_1"]

dt[cat.enc,`:=`(item_id_tgt=i.item_id_tgt,dept_id_tgt=i.dept_id_tgt,
                cat_id_tgt=i.cat_id_tgt,store_id_tgt=i.store_id_tgt,
                state_id_tgt=i.state_id_tgt,event_name_tgt=i.event_name_tgt),
   on = c("id","date")]

rm(cat.enc)
free()

dt[, `:=`(wday = wday(date),
          mday = mday(date),
          week = week(date),
          month = month(date),
          quarter = quarter(date),
          year = year(date))]

#### Forecast aggregate sales #### 
forcomb <- function(y,hor=56){
  ets<- as.vector(forecast(ets(y),h=hor)$mean)
  ces <- as.vector(forecast(auto.ces(as.vector(y),interval="p"),h=hor)$mean)
  ses <-  as.vector(ses(y,h=hor)$mean)
  arima <- as.vector(forecast(auto.arima(y),h=hor)$mean)
  dotm <- as.vector(dotm(y,h=hor)$mean)
  comb <- sapply(1:hor, function(x) median(c(ets[x],ces[x],ses[x],arima[x],dotm[x])))
  # comb <- sapply(1:hor, function(x) median(c(ets[x],ces[x],ses[x],dotm[x])))
  
  return(comb)
}

### aggforecast ##
aggforecast <- function(by.var=NULL,par.thres=6){
  # aggregate by time and by.vars (after d_1914, zeroes)
  # fill zeroes with forecast combination
  
  if(is.null(by.var)){
    cat("Aggregating by date and forecasting \n")
    dat <- dt[,sum(sales,na.rm=T),by=c("date",by.var)]
    tr <- ts(as.vector(dat[date<fday,c("V1"),with=F]),frequency=7)
    dat[date>=fday,V1:=forcomb(tr)]
  }else{
    cat("Aggregating by date and ",paste(by.var,collapse=" "),"\n")
    dat <- dt[,sum(sales,na.rm=T),by=c("date",by.var)]
    dat[, comb_id := do.call(paste,c(.SD,sep=".")), .SDcols=by.var]
    dat <- dcast(dat,date~comb_id,value.var="V1")
    ncol <- ncol(dat)-1
    if(ncol>par.thres){
      cat("Number of cols: ",ncol,", forecasting in parallel \n")
      cl <- makeCluster(11, outfile="") # change to whatever is your core count
      registerDoParallel(cl)
      for.dat <- foreach(i = setdiff(colnames(dat),"date"),.inorder=FALSE,.combine='cbind',
                         .packages=c('data.table','forecast','forecTheta', 'smooth'),
                         .export=c('forcomb'))%dopar%{
                           fday <- as.IDate("2016-04-25")
                           tr <- ts(as.vector(dat[date<fday,c(i),with=F]),frequency=7)
                           res <- data.frame(forcomb(tr))
                           names(res) <- i
                           res
                         }
      stopCluster(cl)
      for(i in setdiff(colnames(dat),"date")){
        dat[date>=fday,c(i) := for.dat[,i]]
      }
    }else{
      cat("Number of cols: ",ncol,", forecasting \n")
      for(i in setdiff(colnames(dat),"date")){
        cat(i,"\n")
        tr <- ts(as.vector(dat[date<fday,c(i),with=F]),frequency=7)
        dat[date>=fday,c(i) := forcomb(tr)]
      } 
    }
    dat <- melt(dat,id.vars="date")
    dat[,c(by.var) := tstrsplit(variable,".", fixed=T)]
    dat[,c("variable"):=NULL] 
  }
  return(dat)
}

##
level.list <- list(NULL, 
                   c("state_id"),
                   c("store_id"),
                   c("cat_id"),
                   c("dept_id"),
                   c("state_id","cat_id"),
                   c("state_id","dept_id"),
                   c("store_id","cat_id"),
                   c("store_id","dept_id")
                   # ,
                   # c("item_id"),
                   # c("item_id","state_id")
                   )
names(level.list) <- c(paste("agg_lvl",1:length(level.list),sep="_"))
agg.sales <- list()

t1 <- Sys.time()
for(i in 1:length(level.list)){
  level <- level.list[[i]]
  agg.sales[[i]] <- aggforecast(by.var=level)
}
Sys.time()-t1 # 47.80751 mins
# save(agg.sales,file="save/aggregate_sales.RData")

## Merge
for(i in 1:length(agg.sales)){
  if(i == 1){
    cat("Merging Levels: ",i," ")
    dt[agg.sales[[i]],names(level.list)[i] := i.V1,on=c("date")]
  }else{
    cat(i," ")
    dt[agg.sales[[i]],names(level.list)[i] := i.value,on=c("date",level.list[[i]])]
  }
}

#### Modeling #### 
## separate test and train set
test <- dt[date > fday-max_lags] # keep all date from a before real test date
train <- dt[date < fday]
# rm(dt)
free()

# create some new variables
train[,(paste0("lag_", c(7,28))) := shift(.SD,c(7,28)), .SDcols = "sales", by = "id"]
train[,(paste0("rmean7_",c(7,28))) := frollmean(lag_7, c(7,28), na.rm = TRUE), by = "id"]
train[,(paste0("rmean28_",c(7,28))) := frollmean(lag_28, c(7,28), na.rm = TRUE), by = "id"]

rm(cal,prices)
free()
train <- train[complete.cases(train)]

## Run XGBoost ##
y <- train$sales
train <- data.matrix(train[,c("id","item_id","dept_id","cat_id","state_id",
                              "event_name_1","date","sales","d","wm_yr_wk",
                              "store_id") := NULL])
free()

## Simple XGBoost ##
p <- list(eta = 0.075,
          objective = "count:poisson",
          eval_metric = "rmse",
          lambda = 0.1,
          colsample_bytree = 0.77,
          nthread=11)

t1 <- Sys.time()
bst.encvars <- xgboost(data=train,label=y,params=p,verbose=T,nrounds=2000,early_stopping_rounds=10,
                       tree_method="hist")
Sys.time()-t1 # 1.19729 hours
xgb.plot.importance(xgb.importance(model=bst.encvars), top_n = 30,measure="Gain")
# save(bst.encvars,file="save/xgb_agglvl.Rdata")

## Forecasting using 366 day windows ## 
for (day in as.list(seq(fday,length.out=2*h, by="day"))){
  cat(as.character(day), " ")
  tst <- test[date >= day - max_lags & date <= day]
  tst[,(paste0("lag_", c(7,28))) := shift(.SD,c(7,28)), .SDcols = "sales", by = "id"]
  tst[,(paste0("rmean7_",c(7,28))) := frollmean(lag_7, c(7,28), na.rm = TRUE), by = "id"]
  tst[,(paste0("rmean28_",c(7,28))) := frollmean(lag_28, c(7,28), na.rm = TRUE), by = "id"]
  
  
  tst <- data.matrix(tst[date == day][,c("id","item_id","dept_id","cat_id","state_id",
                                         "event_name_1","date","sales","d","wm_yr_wk","store_id") := NULL])
  # pred <- 1.02*predict(bst.encvars, tst)
  test[date == day, sales := 1.02*predict(bst.encvars, tst)] # magic multiplier by kyakovlev
  rm(tst)
  free()
}

submit <- test[date>= fday]
submit[date >= fday+h, id := sub("validation", "evaluation", id)]
submit[, d := paste0("F", 1:28), by = id]
submit <- submit[,c("id","d","sales"),with=F]
submit[, dcast(.SD, id ~ d, value.var = "sales")][,fwrite(.SD, "submit/xgb_agglvl.csv")] #0.56 - 0.7














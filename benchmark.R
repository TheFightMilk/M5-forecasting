#### M5 : Benchmark :: 0.67126 ####
library(data.table)
library(xgboost)
library(caret)
library(ranger)
library(pdp)

set.seed(21081991)
# setDTthreads(11)
####
free <- function() invisible(gc())
viewdt <- function(dt,n=10){View(head(dt,n))}

#### Pre-processing #### 

## Load and Merge ##
prices <- fread("m5-forecasting-accuracy/sell_prices.csv")
cal <- fread("m5-forecasting-accuracy/calendar.csv")[,date:=as.IDate(date,format="%Y-%m-%d")]
dt <- fread("m5-forecasting-accuracy/sales_train_validation.csv")

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

## Create Features ##
dt[,(paste0("lag_", c(7,28))) := shift(.SD,c(7,28)), .SDcols = "sales", by = "id"]
dt[,(paste0("rmean7_",c(7,28))) := frollmean(lag_7, c(7,28), na.rm = TRUE), by = "id"]
dt[,(paste0("rmean28_",c(7,28))) := frollmean(lag_28, c(7,28), na.rm = TRUE), by = "id"]
# dt <- dt[, item_id := as.integer(factor(item_id))]

cols <- c("item_id", "state_id", "dept_id", "cat_id", "event_name_1")
dt[, (cols) := lapply(.SD, function(x) as.integer(factor(x))), .SDcols = cols]

# dt[,item_id:=as.integer(factor(item_id))]

dt[, `:=`(wday = wday(date),
          mday = mday(date),
          week = week(date),
          month = month(date),
          quarter = quarter(date),
          year = year(date),
          store_id = NULL,
          d = NULL,
          wm_yr_wk = NULL)]
rm(cal,prices)
free()
dt <- dt[complete.cases(dt)]

xvars <- setdiff(colnames(dt),c("id","sales","date"))

##### TEST
library(microbenchmark)
sample.data <- dt[sample(1:nrow(dt),1000),]
sample.y <- sample.data$sales
sample.data <- data.matrix(sample.data[, c("id", "sales", "date") := NULL])

p1 <- list(eta = 0.075,
           objective = "count:poisson",
           eval_metric = "rmse",
           lambda = 0.1,
           nthread=11)

bench.xgb <- microbenchmark(
  boost.hist <- xgboost(data=sample.data,label=sample.y,params=p1,max_depth=8,nrounds=1000,
                        tree_method="hist",grow_policy="depthwise"),
  boost.norm <- xgboost(data=sample.data,label=sample.y,params=p1,max_depth=8,nrounds=1000,
                        tree_method="approx",grow_policy="depthwise"),
  # boost.loss <- xgboost(data=sample.data,label=sample.y,params=p1,max_depth=8,nrounds=1000,
  #                       tree_method="hist",grow_policy="lossguide"),
  times=50
)


# sparse.form <- as.formula(paste0("~-1+sales+",paste(xvars,collapse="+")))
# dt.melt.sparse <- Matrix::sparse.model.matrix(sparse.form,data=dt.melt)
y <- dt$sales
dt <- data.matrix(dt[, c("id", "sales", "date") := NULL])
free()


## Simple XGBoost ##
p <- list(eta = 0.075,
          objective = "count:poisson",
          eval_metric = "rmse",
          lambda = 0.1,
          nthread=11) ### ChANGE NTHREADS IN KAGGLE

t1 <- Sys.time()
bst.simp <- xgboost(data=dt,label=y,max.depth=2,params=p,verbose=T,nrounds=2000,early_stopping_rounds=10,
                    tree_method="hist")
Sys.time() - t1

# importance
xgb.plot.importance(xgb.importance(model=bst.simp), top_n = 10, measure = "Gain")


## Test data
h <- 28 
max_lags <- 366
tr_last <- 1913
fday <- as.IDate("2016-04-25") 

test <- fread("m5-forecasting-accuracy/sales_train_validation.csv",drop = paste0("d_", 1:(tr_last-max_lags)))
cal <- fread("m5-forecasting-accuracy/calendar.csv")[,date:=as.IDate(date,format="%Y-%m-%d")]
dt <- fread("m5-forecasting-accuracy/sales_train_validation.csv")
prices <- fread("m5-forecasting-accuracy/sell_prices.csv")

test[, paste0("d_", (tr_last+1):(tr_last+2*h)) := NA_real_]


# wide to long
test <- melt(test,
           measure.vars = patterns("^d_"),
           variable.name = "d",
           value.name = "sales")

# merge to calendar
test[cal,`:=`(date=i.date,
            wm_yr_wk=i.wm_yr_wk,
            event_name_1=i.event_name_1,
            snap_CA=i.snap_CA,
            snap_TX=i.snap_TX,
            snap_WI=i.snap_WI),on="d"]

# merge to prices
test[prices, sell_price := i.sell_price, on=c("store_id", "item_id", "wm_yr_wk")]

for (day in as.list(seq(fday, length.out = 2*h, by = "day"))){
  cat(as.character(day), " ")
  tst <- test[date >= day - max_lags & date <= day]
  
  tst[,(paste0("lag_", c(7,28))) := shift(.SD,c(7,28)), .SDcols = "sales", by = "id"]
  tst[,(paste0("rmean7_",c(7,28))) := frollmean(lag_7, c(7,28), na.rm = TRUE), by = "id"]
  tst[,(paste0("rmean28_",c(7,28))) := frollmean(lag_28, c(7,28), na.rm = TRUE), by = "id"]
  # tst <- tst[, item_id := as.integer(factor(item_id))]
  
  tst <- tst[date >= day - max_lags & date <= day]
  
  cols <- c("item_id", "state_id", "dept_id", "cat_id", "event_name_1")
  tst[, (cols) := lapply(.SD, function(x) as.integer(factor(x))), .SDcols = cols]
  
  # tst[,item_id:=as.integer(factor(item_id))]
  
  tst[, `:=`(wday = wday(date),
              mday = mday(date),
              week = week(date),
              month = month(date),
              quarter = quarter(date),
              year = year(date),
              store_id = NULL,
              d = NULL,
              wm_yr_wk = NULL)]
  
  
  tst <- data.matrix(tst[date == day][, c("id", "sales", "date") := NULL])
  test[date == day, sales := 1.02*predict(bst.simp, tst)] # magic multiplier by kyakovlev
}

test[date >= fday
     ][date >= fday+h, id := sub("validation", "evaluation", id)
       ][, d := paste0("F", 1:28), by = id
         ][, dcast(.SD, id ~ d, value.var = "sales")
           ][, fwrite(.SD, "sub_dt_xgb.csv")]

rm(cal,prices)
free()





























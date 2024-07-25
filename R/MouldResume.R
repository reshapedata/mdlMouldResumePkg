#' 模板履历表查询
#'
#' @param token
#' @param FMoldNumber
#'
#' @return
#' @export
#'
#' @examples
#' mdlMouldResume_view(token,FMoldNumber)

mdlMouldResume_view <- function(token,FMoldNumber) {
  sql=paste0("
  select
FMoldNumber as 模具编号,
FMoldName as 模具名称,
FProductCode as 产品编码,
FProductName as 产品名称,
FSpecificationsModels as 规格型号,
FProductDrawingNumber as 产品图号,
FCavitiesNumber as 型腔数,
FManufacturingUnits as 制造单位,
FPurchaseDate as 购入日期,
FLifeExpectancyNumber as 预计寿命次数,
FOperationType as 操作类型,
FOutboundDate as 出库日期,
FInboundDate as 入库日期,
FProductionModules as 生产模数,
FTotalModulus as 总模数,
FMoldStatus as 模具状态,
FIdentifyingPerson as 确认人,
FMaintenanceRecords as 维修保养记录
from rds_t_MouldResume
where FMoldNumber='",FMoldNumber,"'
    ")
  res=tsda::sql_select2(token = token,sql = sql)
  return(res)

}


#' 模板履历表删除
#'
#' @param token
#' @param FMoldNumber
#' @param FOutboundDate
#'
#' @return
#' @export
#'
#' @examples
#' MouldResumeServer_delete()
MouldResumeServer_delete <- function(token,FMoldNumber,FOutboundDate) {
  sql=paste0("delete from rds_t_MouldResume where FMoldNumber='",FMoldNumber,"' and FOutboundDate='",FOutboundDate,"'")
  res=tsda::sql_delete2(token = token,sql_str = sql)
  return(res)

}



#' 模板履历表上传
#'
#' @param token
#' @param file_name
#'
#' @return
#' @export
#'
#' @examples
#' MouldResume_upload(token,file_name)
MouldResume_upload <- function(token,file_name) {

  data <- readxl::read_excel(file_name,col_types =  c("text","text","text","text","text","text","numeric","text","date","numeric","text","date","date","numeric","numeric","text","text","text"))


  data = as.data.frame(data)
  data = tsdo::na_standard(data)


  #上传服务器----------------
  res=tsda::db_writeTable2(token = token,table_name = 'rds_t_MouldResume',r_object = data,append = TRUE)

  return(res)

}
#' 模板履历表预览
#'
#' @param token
#' @param file_name
#'
#' @return
#' @export
#'
#' @examples
#' MouldResume_preview()
MouldResume_preview <- function(token,file_name) {

  data <- readxl::read_excel(file_name,col_types =  c("text","text","text","text","text","text","numeric","text","date","numeric","text","date","date","numeric","numeric","text","text","text"))


  data = as.data.frame(data)
  res = tsdo::na_standard(data)
  return(res)

}

#' 模板履历表查询全部
#'
#' @param token
#' @param
#'
#' @return
#' @export
#'
#' @examples
#' mdlMouldResume viewall()

mdlMouldResume_viewall <- function(token) {
  sql=paste0("
select
FMoldNumber as 模具编号,
FMoldName as 模具名称,
FProductCode as 产品编码,
FProductName as 产品名称,
FSpecificationsModels as 规格型号,
FProductDrawingNumber as 产品图号,
FCavitiesNumber as 型腔数,
FManufacturingUnits as 制造单位,
FPurchaseDate as 购入日期,
FLifeExpectancyNumber as 预计寿命次数,
FOperationType as 操作类型,
FOutboundDate as 出库日期,
FInboundDate as 入库日期,
FProductionModules as 生产模数,
FTotalModulus as 总模数,
FMoldStatus as 模具状态,
FIdentifyingPerson as 确认人,
FMaintenanceRecords as 维修保养记录
from rds_t_MouldResume
    ")
  res=tsda::sql_select2(token = token,sql = sql)
  return(res)

}


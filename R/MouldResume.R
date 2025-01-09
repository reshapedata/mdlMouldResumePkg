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
select a.	MouldNumber	模具编号	,
a.	MouldName	模具名称	,
a.	productNumber	产品编码	,
a.	productName	产品名称	,
a.	FSPECIFICATION	规格型号	,
a.	FDESCRIPTION	产品图号	,
a.	Fcavities	型腔数	,
a.	FManufacturingunit	制造单位	,
a.	FpurchaseDate	购入日期	,
a.	Fexpectancy	预计寿命次数	,
a.	FOperationType	操作类型	,
a.	FOutboundDate	出库日期	,
a.	FInboundDate	入库日期	,
a.	FFINISHQTY	生产数量	,
a.	FProductionmodules	生产模数	,
a.	FTotalmodulus	总模数	,
a.	FMoldstatus	模具状态	,
a.	FIdentifyingPerson	确认人	,
a.	FMaintenanceRecords	维修保养记录
 from rds_t_MouldResume a
where  a.	MouldNumber='",FMoldNumber,"'
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
#' MouldResume_delete()
MouldResume_delete <- function(token,FMoldNumber,FOutboundDate) {
  sql=paste0("delete from rds_t_MouldResume where MouldNumber='",FMoldNumber,"' and FOutboundDate='",FOutboundDate,"'")
  res=tsda::sql_delete2(token = token,sql_str = sql)
  return(res)

}



#' 模具信息表上传
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

  data <- readxl::read_excel(file_name,
col_types =  c("text","text","text","text","text","text","numeric","text","date","numeric","date","date"))


  data = as.data.frame(data)
  data = tsdo::na_standard(data)


  #上传服务器----------------
  res=tsda::db_writeTable2(token = token,table_name = 'rds_dms_src_t_Mouldproduct',r_object = data,append = TRUE)

  return(res)

}
#' 模具表预览
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

  # res= data <- readxl::read_excel(file_name)

  data <- readxl::read_excel(file_name,
col_types =  c("text","text","text","text","text","text","numeric","text","date","numeric","date","date"))


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
select a.	MouldNumber	模具编号	,
a.	MouldName	模具名称	,
a.	productNumber	产品编码	,
a.	productName	产品名称	,
a.	FSPECIFICATION	规格型号	,
a.	FDESCRIPTION	产品图号	,
a.	Fcavities	型腔数	,
a.	FManufacturingunit	制造单位	,
a.	FpurchaseDate	购入日期	,
a.	Fexpectancy	预计寿命次数	,
a.	FOperationType	操作类型	,
a.	FOutboundDate	出库日期	,
a.	FInboundDate	入库日期	,
a.	FFINISHQTY	生产数量	,
a.	FProductionmodules	生产模数	,
a.	FTotalmodulus	总模数	,
a.	FMoldstatus	模具状态	,
a.	FIdentifyingPerson	确认人	,
a.	FMaintenanceRecords	维修保养记录
 from rds_t_MouldResume a
    ")
  res=tsda::sql_select2(token = token,sql = sql)
  return(res)

}


#' 模板履历预处理查询
#'
#' @param token
#' @param
#'
#' @return
#' @export
#'
#' @examples
#' mdlMouldResume_previewall()

mdlMouldResume_previewall <- function(dms_token,FYEAR,FMONTH) {
  if (FYEAR=='' || FMONTH==''){
    sql=paste0("
select b.MouldNumber as 模具编号,MouldName as 模具名称,productNumber as 产品编码,
productName as 产品名称,b.FSPECIFICATION as 规格型号,FDESCRIPTION as 产品图号,Fcavities as 型腔数,
FManufacturingunit as 制造单位,cast(FpurchaseDate as date) as 购入日期,Fexpectancy as 预计寿命次数,
cast(min(fdate) as date)as 出库日期 ,cast(max(a.FDATE) as date) as 入库日期,sum(FFINISHQTY) as 生产数量,round(sum(FFINISHQTY)/Fcavities,0) as 生产模数,'' as 操作类型,'' as 确认人,
'' as 维修保养记录,
case when count(1) over( partition by productNumber,year(a.fdate),month(a.fdate)  ) =1 then '仅使用一个模具'
 when count(1) over( partition by productNumber,year(a.fdate),month(a.fdate)  ) >1 then '使用多个模具，生产数量和模具数须确认'
end as 模具使用情况
 from rds_erp_src_t_Mouldproductdaily a
left join rds_dms_src_t_Mouldproduct b on a.FmaterialNumber=b.productNumber
where  a.FmaterialNumber=b.productNumber and a.FDATE>=b.fstartdate and a.FDATE<=fenddate
group by year(fdate),month(a.FDATE),FmaterialNumber,a.FSPECIFICATION,b.MouldNumber,MouldName,
productNumber,productName,b.FSPECIFICATION,FDESCRIPTION,Fcavities,Fexpectancy,FManufacturingunit,FpurchaseDate
    ")


  }
  else{
    sql=paste0("
select b.MouldNumber as 模具编号,MouldName as 模具名称,productNumber as 产品编码,
productName as 产品名称,b.FSPECIFICATION as 规格型号,FDESCRIPTION as 产品图号,Fcavities as 型腔数,
FManufacturingunit as 制造单位,cast(FpurchaseDate as date) as 购入日期,Fexpectancy as 预计寿命次数,
cast(min(fdate) as date)as 出库日期 ,cast(max(a.FDATE) as date) as 入库日期,sum(FFINISHQTY) as 生产数量,round(sum(FFINISHQTY)/Fcavities,0) as 生产模数,'' as 操作类型,'' as 确认人,
'' as 维修保养记录,
case when count(1) over( partition by productNumber,year(a.fdate),month(a.fdate)  ) =1 then '仅使用一个模具'
 when count(1) over( partition by productNumber,year(a.fdate),month(a.fdate)  ) >1 then '使用多个模具，生产数量和模具数须确认'
end as 模具使用情况
 from rds_erp_src_t_Mouldproductdaily a
left join rds_dms_src_t_Mouldproduct b on a.FmaterialNumber=b.productNumber
where  a.FmaterialNumber=b.productNumber and a.FDATE>=b.fstartdate and a.FDATE<=fenddate
and year(FDATE)= '",FYEAR,"' and month(FDATE)='",FMONTH,"'
group by year(fdate),month(a.FDATE),FmaterialNumber,a.FSPECIFICATION,b.MouldNumber,MouldName,
productNumber,productName,b.FSPECIFICATION,FDESCRIPTION,Fcavities,Fexpectancy,FManufacturingunit,FpurchaseDate
    ")

  }


  res=tsda::sql_select2(token = dms_token,sql = sql)


  return(res)

}

#' 模具表预览
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

  data <- readxl::read_excel(file_name,
                             col_types =  c("text","text","text","text","text","text","numeric","text","text","numeric","text","text"))


  data = as.data.frame(data)
  res = tsdo::na_standard(data)
  return(res)

}
#' 模具履历表预览
#'
#' @param token
#' @param file_name
#'
#' @return
#' @export
#'
#' @examples
#' MouldResume_update_preview()
MouldResume_update_preview <- function(token,file_name) {

  data <- readxl::read_excel(file_name,

                             col_types = c("text", "text", "text",
                                           "text", "text", "text", "numeric",
                                           "text", "text", "numeric", "text",
                                           "text", "numeric", "numeric", "text",
                                           "text", "text", "text"))



  data = as.data.frame(data)
  res = tsdo::na_standard(data)
  return(res)

}



#' 模具履历表上传
#'
#' @param dms_token
#' @param file_name
#'
#' @return
#' @export
#'
#' @examples
#' MouldResume_update_upload(dms_token,file_name)
MouldResume_update_upload <- function(dms_token,file_name) {


  #上传服务器----------------
  # data <- readxl::read_excel(file_name)

   data <- readxl::read_excel(file_name,col_types = c("text", "text", "text", "text", "text", "text", "numeric",
 "text", "text", "numeric", "text","text", "numeric", "numeric", "text","text", "text", "text"))



  data = as.data.frame(data)
  data = tsdo::na_standard(data)


  #上传服务器----------------
  tsda::db_writeTable2(token = dms_token,table_name = 'rds_t_MouldResume_input',r_object = data,append = TRUE)
  sql_delete=paste0("
delete a from rds_t_MouldResume a
inner join rds_t_MouldResume_input b
on a.productNumber=b.productNumber and a.MouldNumber=b.MouldNumber
and  a.FOutboundDate=b.FOutboundDate
where
 a.productNumber=b.productNumber and a.MouldNumber=b.MouldNumber
and  a.FOutboundDate=b.FOutboundDate ")
  tsda::sql_delete2(token = dms_token,sql_str =sql_delete )
  sql_insert =paste0(" insert into rds_t_MouldResume
 select a.	MouldNumber	,
a.	MouldName	,
a.	productNumber	,
a.	productName	,
a.	FSPECIFICATION	,
a.	FDESCRIPTION	,
a.	Fcavities	,
a.	FManufacturingunit	,
a.	FpurchaseDate	,
a.	Fexpectancy	,
a.	FOutboundDate	,
a.	FInboundDate	,
a.	FFINISHQTY	,
a.	FProductionmodules	,
a.	FOperationType	,
a.	FIdentifyingPerson	,
a.	FMaintenanceRecords	,0,'' from rds_t_MouldResume_input  a")
  tsda::sql_insert2(token = dms_token,sql_str =sql_insert )

  sql_update=paste0("update a
set a.FTotalmodulus=b.FTotalmodulus,
a.FMoldstatus =b.FMoldstatus
from rds_t_MouldResume  a
inner join rds_vw_MouldResume_modulus  b
on a.MouldNumber=b.MouldNumber and a.MouldName=b.MouldName and a.productNumber=b.productNumber and a.FOutboundDate=b.FOutboundDate")
  tsda::sql_update2(token = dms_token,sql_str =sql_update )
  sql_truncate =paste0("truncate table  rds_t_MouldResume_input")
 res= tsda::sql_delete2(token = dms_token,sql_str =sql_truncate )



  return(res)

}


#' 模具查询
#'
#' @param dms_token
#' @param MouldNumber
#'
#' @return
#' @export
#'
#' @examples
#' mdlMouldResume_mould_viewall()

mdlMouldResume_mould_viewall <- function(dms_token,MouldNumber) {
  if (MouldNumber=='' ){
    sql=paste0("

select MouldNumber	as	模具编号	,
MouldName	as	模具名称	,
productNumber	as	产品编码	,
productName	as	产品名称	,
FSPECIFICATION	as	规格型号	,
FDESCRIPTION	as	产品图号	,
Fcavities	as	型腔数	,
FManufacturingunit	as	制造单位	,
FpurchaseDate	as	购入日期	,
Fexpectancy	as	预计寿命次数	,
FStartDate	as	生效日期	,
FEndDate	as	失效日期
 from rds_dms_src_t_Mouldproduct


    ")


  }
  else{
    sql=paste0("

select MouldNumber	as	模具编号	,
MouldName	as	模具名称	,
productNumber	as	产品编码	,
productName	as	产品名称	,
FSPECIFICATION	as	规格型号	,
FDESCRIPTION	as	产品图号	,
Fcavities	as	型腔数	,
FManufacturingunit	as	制造单位	,
FpurchaseDate	as	购入日期	,
Fexpectancy	as	预计寿命次数	,
FStartDate	as	生效日期	,
FEndDate	as	失效日期
 from rds_dms_src_t_Mouldproduct
WHERE MouldNumber ='",MouldNumber,"'

    ")

  }


  res=tsda::sql_select2(token = dms_token,sql = sql)
  return(res)

}

#' 模具删除
#'
#' @param dms_token
#' @param MouldNumber
#'
#' @return
#' @export
#'
#' @examples
#' mdlMouldResume_mould_delete()

mdlMouldResume_mould_delete <- function(dms_token,MouldNumber) {

    sql=paste0("
delete  from rds_dms_src_t_Mouldproduct WHERE MouldNumber = '",MouldNumber,"'  ")

  res=tsda::sql_delete2(token = dms_token,sql_str = sql)
  return(res)

}


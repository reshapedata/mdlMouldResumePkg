#' 同步erp数据到数据中台
#'
#' @param erp_token
#' @param dms_token
#' @param FYEAR
#' @param FMONTH
#'
#' @return
#' @export
#'
#' @examples
#' productdaily_erpsync()
productdaily_erpsync<- function(erp_token,dms_token,FYEAR,FMONTH) {


  sql=paste0("
select cast(a.fdate as date) as fdate, isnull(nullif(f.FSRCSPLITBILLNO,''),e.FBILLNO) as FSRCSPLITBILLNO,
g.FNUMBER as FmaterialNumber,
h.FSPECIFICATION as FSPECIFICATION,a.fbillno as FProductLots,b.FFINISHQTY as FFINISHQTY
from T_PRD_MORPT a
inner join T_PRD_MORPTENTRY b on a.FID=b.FID
inner join T_PRD_MORPTENTRY_LK c on b.FENTRYID=c.FENTRYID
inner join T_PRD_MOENTRY d on c.FSID=d.FENTRYID
inner join T_PRD_MO  e on d.FID=e.FID
inner join T_PRD_MOENTRY_Q  f on d.FENTRYID=f.FENTRYID
inner join T_BD_MATERIAL g on  b.FMATERIALID=g.FMATERIALID
inner join T_BD_MATERIAL_L h on g.FMATERIALID=h.FMATERIALID
where a.FPRDORGID='100073' and
year(a.FDATE)='",FYEAR,"' and month(a.FDATE)='",FMONTH,"'

             ")

  data=tsda::sql_select2(token = erp_token,sql = sql)

  data = as.data.frame(data)
  data = tsdo::na_standard(data)
  tsda::db_writeTable2(token = dms_token,table_name = 'rds_erp_src_t_Mouldproductdaily_input',r_object = data,append = TRUE)

  sql_delete=paste0("delete a from rds_erp_src_t_Mouldproductdaily_input a inner join
rds_erp_src_t_Mouldproductdaily b on a.FProductLots=b.FProductLots
where a.FProductLots=b.FProductLots")
  tsda::sql_delete2(token =dms_token ,sql_str =sql_delete )

  sql_insert =paste0("insert into rds_erp_src_t_Mouldproductdaily
select * from rds_erp_src_t_Mouldproductdaily_input
                     ")
  tsda::sql_insert2(token =dms_token ,sql_str = sql_insert)

  sql_num=paste0("select count(1) from rds_erp_src_t_Mouldproductdaily_input")
  data_num=tsda::sql_select2(token =dms_token ,sql = sql_num)
  data_num = as.numeric(data_num)
  msg = paste("同步了",data_num,"条数据")
  tsui::pop_notice(msg)
  sql_truncate =paste0("truncate table rds_erp_src_t_Mouldproductdaily_input")
  res = tsda::sql_delete2(token = dms_token,sql_str = sql_truncate)

  return(res)

}

#' 获取日报
#'
#' @param token
#' @param
#'
#' @return
#' @export
#'
#' @examples
#' productdaily_get()
productdaily_get <- function(dms_token) {
  #除0

  sql=paste0("    select a.FSRCSPLITBILLNO as 生产订单号,a.FmaterialNumber as 物料编码,a.FSPECIFICATION  as 规格型号,a.FProductLots as 流水号,
c.[FProcessName] as 工序,a.FFINISHQTY as 汇报选单数量,b.FBoxQuantity as 每箱数量
,case when b.FBoxQuantity=0 then 0
else FLOOR(a.FFINISHQTY /b.FBoxQuantity)
end as 箱数,
isnull(a.FFINISHQTY%nullif(b.FBoxQuantity,0),0) as 零头数
,d.FScrappedQty as 报废数 ,D.FManualtime as 人工计时,d.FManualstoppage as 人工补时,d.FOperator as 操作工,d.FProDate as 生产日期,d.FInputDate as 输卡日期
from rds_erp_src_t_Mouldproductdaily a
left join rds_t_productRouting b  on a.FmaterialNumber=b.FMaterialNumber
left join [rds_t_Routing] c on b.FRoutingNumber=c.FRoutingNumber
left join  rds_t_productdaily d on a.FProductLots=d.FProductLots and c.FProcessName =d.FProcessName
where  a.FProductLots not in (select FProductLots from [rds_t_productdaily_FProductLots_black])
and( d.FProductLots is  null and d.FProcessName is   null
or  YEAR(d.FInputDate) < 2000)



             ")


  res=tsda::sql_select2(token = dms_token,sql = sql)
  return(res)

}


#' 查询数据
#'
#' @param token
#' @param FProductLots
#'
#' @return
#' @export
#'
#' @examples
#' productdaily_view()
productdaily_view <- function(dms_token,FProductLots) {

  if(FProductLots=="" )
  {sql=paste0("select FSRCSPLITBILLNO	as	生产订单	,
FmaterialNumber	as	物料编码	,
FSPECIFICATION	as	产品图号	,
FProductLots	as	产品批次	,
FProcessName	as	工序 	,
FFINISHQTY	as	实作产量	,
FBoxQuantity	as	每箱数量	,
FBoxQty	as	箱数	,
FBoxFractionQty	as	零头数	,
FScrappedQty	as	报废	,
FManualtime	as	人工计时	,
FManualstoppage	as	人工补时	,
FOperator	as	操作工	,
FProDate	as	生产日期	,
FInputDate	as	输卡日期
 from [rds_t_productdaily]
             ")}
  else{
    sql=paste0("select FSRCSPLITBILLNO	as	生产订单	,
FmaterialNumber	as	物料编码	,
FSPECIFICATION	as	产品图号	,
FProductLots	as	产品批次	,
FProcessName	as	工序 	,
FFINISHQTY	as	实作产量	,
FBoxQuantity	as	每箱数量	,
FBoxQty	as	箱数	,
FBoxFractionQty	as	零头数	,
FScrappedQty	as	报废	,
FManualtime	as	人工计时	,
FManualstoppage	as	人工补时	,
FOperator	as	操作工	,
FProDate	as	生产日期	,
FInputDate	as	输卡日期
 from [rds_t_productdaily]
 where FProductLots='",FProductLots,"'

             ")
  }

  res=tsda::sql_select2(token = dms_token,sql = sql)
  return(res)

}



#' 计时计件工资表删除
#'
#' @param token
#' @param FProductLots
#'
#' @return
#' @export
#'
#' @examples
#' productdaily_delete()
productdaily_delete <- function(dms_token,FProductLots) {
  sql=paste0(" delete  from rds_t_productdaily  where FProductLots='",FProductLots,"'
             ")
  res=tsda::sql_delete2(token = dms_token,sql_str = sql)
  return(res)

}

#' 日报表上传
#'
#' @param file_name
#' @param token
#'
#' @return
#' @export
#'
#' @examples
#' productdaily_upload()
productdaily_upload <- function(dms_token,file_name) {


  data <- readxl::read_excel(file_name,col_types =  c("text","text","text","text","text","numeric","numeric","numeric",
                                                      "numeric","numeric","numeric","numeric","text","date","date"

  ))
  data = as.data.frame(data)

  data = tsdo::na_standard(data)
  #上传服务器----------------
  tsda::db_writeTable2(token = dms_token,table_name = 'rds_t_productdaily_input',r_object = data,append = TRUE)
  sql_delete =paste0("delete a from  rds_t_productdaily a
inner join rds_t_productdaily_input b
on a.FProductLots=b.FProductLots and a.FProcessName=b.FProcessName")

  tsda::sql_delete2(token = dms_token,sql_str = sql_delete)

  sql_delete2 =paste0("delete a from rds_t_productdaily_input a
where   FOperator='' or FOperator ='0'")

  tsda::sql_delete2(token = dms_token,sql_str = sql_delete2)


  sql_insert =paste0("insert into  rds_t_productdaily select * from rds_t_productdaily_input")
  tsda::sql_insert2(token = dms_token,sql_str = sql_insert)
  sql_truncate =paste0("truncate table rds_t_productdaily_input")

  res=tsda::sql_delete2(token = dms_token,sql_str = sql_truncate)



  return(res)

  #end

}







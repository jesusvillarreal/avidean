
TRUNCATE table `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.CMBD3_4114_AMI_2024`;
Create or replace TABLE `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.CMBD3_4114_AMI_2024` as 
with 
------------codigo del CMBD2 --traemos eseta parte del cmbd2 porque para estos se toma en el id_sineistro como llave
flag_siniestro_emergencia as (
select distinct
siniestro.id_siniestro,
'E' as flag_emergencia
FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
left join unnest(atencion_salud) atencion_salud
WHERE periodo =DATE_TRUNC(current_date(), MONTH)
and extract(year from fec_hora_ocurrencia) in (2024)
--and extract(MONTH from fec_hora_ocurrencia) in (05)
and agrupacion_cobertura_negocio in ('EMERGENCIA')
and des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
),

hospitalario_resumen as (

select distinct
siniestro.id_siniestro as identificador_atencion,
ID_PRODUCTO,
cast( min(SINIESTRO.FEC_HORA_OCURRENCIA) as datetime) fec_ingreso,
cast(max(atencion_salud.fec_fin_internamiento) as datetime) as fec_salida,
FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
left join unnest(atencion_salud) atencion_salud
left join flag_siniestro_emergencia f on (f.id_siniestro=siniestro.id_siniestro)
WHERE periodo = DATE_TRUNC(current_date(), MONTH)
and extract(year from fec_hora_ocurrencia) in (2024)
--and extract(MONTH from fec_hora_ocurrencia) in (05)
and agrupacion_cobertura_negocio in ('HOSPITALARIO','ONCOLOGIA','MATERNIDAD','EMERGENCIA','AMBULATORIO')
and des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
and id_cobertura_origen not in ('A61')
group by 1,2--,3,4,5,6,7,8,9,10,11,12,13,14,15,16
),
mayor_dias_estancia as (
select * from (select a.*, date_diff(fec_salida,fec_ingreso, day) dias_estancia
   from hospitalario_resumen a)
where dias_estancia>1
),--select sum(monto_pagado),count(*) from mayor_dias_estancia,
---fin del codigo del CMBD2

consolidado_siniestros as 
(
  SELECT distinct
siniestro.id_siniestro as identificador_atencion,
case when (cod_mecanismo_pago='03' and num_procedimiento_origen='10') 
then concat('RS-10-',cod_tipo_contrato) else procedimiento_salud.num_procedimiento_origen end num_procedimiento_origen,
ID_PRODUCTO
FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
left join unnest(atencion_salud) atencion_salud
left join unnest(procedimiento_salud) procedimiento_salud
WHERE periodo = DATE_TRUNC(current_date(), MONTH)
and extract(year from fec_hora_ocurrencia) in (2024)
--and extract(MONTH from fec_hora_ocurrencia) in (05)
and des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
and agrupacion_cobertura_negocio in ('HOSPITALARIO','ONCOLOGIA','MATERNIDAD','EMERGENCIA','AMBULATORIO')
and id_cobertura_origen not in ('A61')
and siniestro.id_siniestro in (select a.identificador_atencion from mayor_dias_estancia a)
union all

--siniestro_sin_hospitalizaciones 
SELECT distinct
atencion_salud.id_pre_liquidacion_siniestro as identificador_atencion,
case when (cod_mecanismo_pago='03' and num_procedimiento_origen='10') 
then concat('RS-10-',cod_tipo_contrato) else procedimiento_salud.num_procedimiento_origen end num_procedimiento_origen,
ID_PRODUCTO
FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
left join unnest(atencion_salud) atencion_salud
left join unnest(procedimiento_salud) procedimiento_salud
WHERE periodo = DATE_TRUNC(current_date(), MONTH)
and extract(year from fec_hora_ocurrencia) in (2024)
--and extract(MONTH from fec_hora_ocurrencia) in (05)
and des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
and siniestro.id_siniestro not in (select siniestro.id_siniestro FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
left join unnest(atencion_salud) atencion_salud
left join unnest(procedimiento_salud) procedimiento_salud
WHERE periodo = DATE_TRUNC(current_date(), MONTH)
and extract(year from fec_hora_ocurrencia) in (2024)
--and extract(MONTH from fec_hora_ocurrencia) in (05)
and des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
and agrupacion_cobertura_negocio in ('HOSPITALARIO','ONCOLOGIA','MATERNIDAD','EMERGENCIA','AMBULATORIO')
and id_cobertura_origen not in ('A61')
and siniestro.id_siniestro in (select a.identificador_atencion from mayor_dias_estancia a)))

,siniestro as (

select  DISTINCT CONCAT(SPLIT(identificador_atencion,'-')[OFFSET(0)],'-',SPLIT(identificador_atencion,'-')[OFFSET(1)],'-',
  SPLIT(identificador_atencion,'-')[OFFSET(2)],'-',
  SPLIT(identificador_atencion,'-')[OFFSET(3)],'-',
  SPLIT(identificador_atencion,'-')[OFFSET(4)] ) id_siniestro
from consolidado_siniestros
where 1=1 and identificador_atencion is not null
and num_procedimiento_origen is not  null
and id_producto='AX-4114'
) --SELECT COUNT(DISTINCT ID_SINIESTRO) FROM SINIESTRO 4,183,297
,PERSONA_dni AS (select DISTINCT
    p.id_persona,
    d.num_documento

from `rs-prd-dlk-dd-stging-f0e1.stg_modelo_persona.persona` P
LEFT JOIN UNNEST(DOCUMENTO_IDENTIDAD) D
where 1=1 and d.ind_documento_principal = '1' 
--AND D.NUM_DOCUMENTO LIKE '%13180716%' ---'%51939003%' 1048290201
--and id_persona='AX-16225496'
),TRAMA_AFILIADOS AS 
(SELECT DISTINCT NRO_DOCUMENTO_IDENTIDAD,COD_PACIENTE_ASEGURADO,ID_PERSONA 
FROM `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_M` TR
LEFT JOIN PERSONA_dni P ON TR.NRO_DOCUMENTO_IDENTIDAD=P.num_documento 
union all 
select distinct NRO_DOCUMENTO_IDENTIDAD,COD_PACIENTE_ASEGURADO,id_persona
from  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_N` nro
LEFT JOIN PERSONA_dni P ON nro.NRO_DOCUMENTO_IDENTIDAD=P.num_documento 
--WHERE COD_PACIENTE_ASEGURADO='00000000000005564774' --'00000002978433' 
)
,atencion as (select CONCAT('AX-',M14.COD_PROVEEDOR,'-',M01.SEDE_FACTURADOR) ID_PRESTADOR,CONCAT(SUBSTRING(M02.FEC_INICIO_ATENCION, 1, 4), '-', SUBSTRING(M02.FEC_INICIO_ATENCION, 5, 2), '-', SUBSTRING(M02.FEC_INICIO_ATENCION, 7, 2)) date_str,
   CONCAT('AX-',M02.COD_PACIENTE_ASEGURADO) NUMERO_AFILIADO,
CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION)ID_siniestro_atencion,
CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO)ID_siniestro
  FROM `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF014_M` M14,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF001_M` M01,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_M` M02 --ATENCION
  WHERE M14.NRO_PROCESO = M14.NRO_PROCESO + 0
    AND M14.TIPO_ENTIDAD_SALUD = M14.TIPO_ENTIDAD_SALUD + 0
    AND M14.COD_ENTIDAD_SALUD =M14.COD_ENTIDAD_SALUD
    AND M14.COD_PROVEEDOR = M14.COD_PROVEEDOR + 0
    AND M14.NRO_SUCURSAL = M14.NRO_SUCURSAL
    AND M14.NRO_LOTE = M14.NRO_LOTE + 0
    AND M14.STATUS_REGISTRO = 'P'
     AND M01.NRO_PROCESO = M14.NRO_PROCESO
    AND M01.NRO_LOTE = lpad(cast(M14.NRO_LOTE as string),7,'0')
    AND M01.NRO_PROCESO = M02.NRO_PROCESO
    AND M01.RUC_FACTURADOR = M02.RUC_FACTURADOR
    AND M01.SEDE_FACTURADOR = M02.SEDE_FACTURADOR
    AND M01.TIPO_DOCUMENTO_PAGO = M02.TIPO_DOCUMENTO_PAGO
    AND M01.NRO_DOCUMENTO_PAGO = M02.NRO_DOCUMENTO_PAGO
    AND CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO)
    IN (SELECT DISTINCT ID_SINIESTRO FROM siniestro)
    UNION ALL 
    select CONCAT('AX-',M14.COD_PROVEEDOR,'-',M01.SEDE_FACTURADOR) ID_PRESTADOR,CONCAT(SUBSTRING(M02.FEC_INICIO_ATENCION, 1, 4), '-', SUBSTRING(M02.FEC_INICIO_ATENCION, 5, 2), '-', SUBSTRING(M02.FEC_INICIO_ATENCION, 7, 2)) date_str,
   CONCAT('AX-',M02.COD_PACIENTE_ASEGURADO) NUMERO_AFILIADO,
CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION)ID_siniestro_atencion,CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO)ID_siniestro
  FROM `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF014_N` M14,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF001_N` M01,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_N` M02 --ATENCION
  WHERE M14.NRO_PROCESO = M14.NRO_PROCESO + 0
    AND M14.TIPO_ENTIDAD_SALUD = M14.TIPO_ENTIDAD_SALUD + 0
    AND M14.COD_ENTIDAD_SALUD =M14.COD_ENTIDAD_SALUD
    AND M14.COD_PROVEEDOR = M14.COD_PROVEEDOR + 0
    AND M14.NRO_SUCURSAL = M14.NRO_SUCURSAL
    AND M14.NRO_LOTE = M14.NRO_LOTE + 0
    AND M14.STATUS_REGISTRO = 'P'
     AND M01.NRO_PROCESO = M14.NRO_PROCESO
    AND M01.NRO_LOTE = lpad(cast(M14.NRO_LOTE as string),7,'0')
    AND M01.NRO_PROCESO = M02.NRO_PROCESO
    AND M01.RUC_FACTURADOR = M02.RUC_FACTURADOR
    AND M01.SEDE_FACTURADOR = M02.SEDE_FACTURADOR
    AND M01.TIPO_DOCUMENTO_PAGO = M02.TIPO_DOCUMENTO_PAGO
    AND M01.NRO_DOCUMENTO_PAGO = M02.NRO_DOCUMENTO_PAGO
    AND CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO)
    IN (SELECT DISTINCT ID_SINIESTRO FROM siniestro))
 --SELECT COUNT(DISTINCT ID_SINIESTRO) FROM atencion
,PROCE_TRAMA AS
    (SELECT DISTINCT
  -- M14
   CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION)ID,
  -- M03
  M03.TIPO_CLASIFICACION_GASTO, M03.DESCRIPCIONSERVICIO, M03.PRECIO_UNITARIO_SIN_IMPUESTO, CAST(M03.COPAGO_SERVICIO AS FLOAT64)COPAGO_SERVICIO,  
  M03.MONTO_NO_CUBIERTO_SERVICIO, M03.FEC_SERVICIO, CAST(M03.COPAGOFIJO AS FLOAT64)COPAGOFIJO ,
  M03.COD_CLASIFICACION_GASTO,
  cast(M03.MONTO_CUBIERTO_SERVICIO as float64) MONTO_CUBIERTO_SERVICIO,CORRELATIVO_ITEM_PROC,
  (cast(M03.MONTO_CUBIERTO_SERVICIO as float64)-CAST(M03.COPAGOFIJO AS FLOAT64)-CAST(M03.COPAGO_SERVICIO AS FLOAT64))copago
    FROM `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF014_M` M14,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF001_M` M01,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_M` M02, --ATENCION
   `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF003_M` M03 --PROCEDIMIENTO
  WHERE M14.NRO_PROCESO = M14.NRO_PROCESO + 0
    AND M14.TIPO_ENTIDAD_SALUD = M14.TIPO_ENTIDAD_SALUD + 0
    AND M14.COD_ENTIDAD_SALUD =M14.COD_ENTIDAD_SALUD
    AND M14.COD_PROVEEDOR = M14.COD_PROVEEDOR + 0
    AND M14.NRO_SUCURSAL = M14.NRO_SUCURSAL
    AND M14.NRO_LOTE = M14.NRO_LOTE + 0
    AND M14.STATUS_REGISTRO = 'P'
    AND M01.NRO_PROCESO = M14.NRO_PROCESO
    AND M01.NRO_LOTE = lpad(cast(M14.NRO_LOTE as string),7,'0')
    AND M01.NRO_PROCESO = M02.NRO_PROCESO
    AND M01.RUC_FACTURADOR = M02.RUC_FACTURADOR
    AND M01.SEDE_FACTURADOR = M02.SEDE_FACTURADOR
    AND M01.TIPO_DOCUMENTO_PAGO = M02.TIPO_DOCUMENTO_PAGO
    AND M01.NRO_DOCUMENTO_PAGO = M02.NRO_DOCUMENTO_PAGO
    AND M02.NRO_PROCESO = M03.NRO_PROCESO
    AND M02.RUC_FACTURADOR = M03.RUC_FACTURADOR
    AND M02.SEDE_FACTURADOR = M03.SEDE_FACTURADOR
    AND M02.TIPO_DOCUMENTO_PAGO = M03.TIPO_DOCUMENTO_PAGO
    AND M02.NRO_DOCUMENTO_PAGO = M03.NRO_DOCUMENTO_PAGO
    AND M02.CORRELATIVO_ATENCION = M03.CORRELATIVO_ATENCION
    --AND extract ( year from fec_hora_ocurrencia)in ( 2023)  
    AND CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION) IN (SELECT DISTINCT ID_siniestro_atencion FROM atencion )
    UNION ALL
  
  SELECT DISTINCT
  -- M14
   CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION)ID,
  -- M03
  M03.TIPO_CLASIFICACION_GASTO, M03.DESCRIPCIONSERVICIO, M03.PRECIO_UNITARIO_SIN_IMPUESTO, CAST(M03.COPAGO_SERVICIO AS FLOAT64)COPAGO_SERVICIO,  
  M03.MONTO_NO_CUBIERTO_SERVICIO, M03.FEC_SERVICIO, CAST(M03.COPAGOFIJO AS FLOAT64)COPAGOFIJO ,
  M03.COD_CLASIFICACION_GASTO,
  cast(M03.MONTO_CUBIERTO_SERVICIO as float64) MONTO_CUBIERTO_SERVICIO,CORRELATIVO_ITEM_PROC,
  (cast(M03.MONTO_CUBIERTO_SERVICIO as float64)-CAST(M03.COPAGOFIJO AS FLOAT64)-CAST(M03.COPAGO_SERVICIO AS FLOAT64))copago
    FROM `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF014_N` M14,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF001_N` M01,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_N` M02, --ATENCION
   `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF003_N` M03 --PROCEDIMIENTO
  WHERE M14.NRO_PROCESO = M14.NRO_PROCESO + 0
    AND M14.TIPO_ENTIDAD_SALUD = M14.TIPO_ENTIDAD_SALUD + 0
    AND M14.COD_ENTIDAD_SALUD =M14.COD_ENTIDAD_SALUD
    AND M14.COD_PROVEEDOR = M14.COD_PROVEEDOR + 0
    AND M14.NRO_SUCURSAL = M14.NRO_SUCURSAL
    AND M14.NRO_LOTE = M14.NRO_LOTE + 0
    AND M14.STATUS_REGISTRO = 'P'
    -- AND cast(M14.FEC_ENVIO as string) BETWEEN '01/01/2023' AND '01/01/2024'
    AND M01.NRO_PROCESO = M14.NRO_PROCESO
    AND M01.NRO_LOTE = lpad(cast(M14.NRO_LOTE as string),7,'0')
    AND M01.NRO_PROCESO = M02.NRO_PROCESO
    AND M01.RUC_FACTURADOR = M02.RUC_FACTURADOR
    AND M01.SEDE_FACTURADOR = M02.SEDE_FACTURADOR
    AND M01.TIPO_DOCUMENTO_PAGO = M02.TIPO_DOCUMENTO_PAGO
    AND M01.NRO_DOCUMENTO_PAGO = M02.NRO_DOCUMENTO_PAGO
    AND M02.NRO_PROCESO = M03.NRO_PROCESO
    AND M02.RUC_FACTURADOR = M03.RUC_FACTURADOR
    AND M02.SEDE_FACTURADOR = M03.SEDE_FACTURADOR
    AND M02.TIPO_DOCUMENTO_PAGO = M03.TIPO_DOCUMENTO_PAGO
    AND M02.NRO_DOCUMENTO_PAGO = M03.NRO_DOCUMENTO_PAGO
    AND M02.CORRELATIVO_ATENCION = M03.CORRELATIVO_ATENCION
    --AND extract ( year from fec_hora_ocurrencia)in ( 2023)  
    AND CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION) IN (SELECT DISTINCT ID_siniestro_atencion FROM atencion )
    ),
MEDI_TRAMA AS
  (SELECT DISTINCT
-- M14
  CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION)ID,
-- M05
M05.TIPO_PRODUCTO_FARMACIA, M05.COD_PRODUCTO_FARMACIA,
cast(M05.MONTO_NO_CUBIERTO_PRODUCTO as float64) AS MONTO_NO_CUBIERTO_PRODUCTO,
cast(M05.MONTO_CUBIERTO_PRODUCTO as float64) as MONTO_CUBIERTO_PRODUCTO,
cast(M05.COPAGO_PRODUCTO_FARMACIA as float64) COPAGO_PRODUCTO_FARMACIA,
CANT_VENTA_PRODUCTO, PRECIO_UNITARIO_SIMPUESTO
,(cast(M05.MONTO_CUBIERTO_PRODUCTO as float64)-CAST(M05.COPAGO_PRODUCTO_FARMACIA AS FLOAT64))copago_medicamento
  FROM `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF014_M` M14,
`rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF001_M` M01,
`rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_M` M02, --ATENCION
  -- `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF003_M` M03 --PROCEDIMIENTO
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF005_M` M05 --MEDICAMENTO
WHERE M14.NRO_PROCESO = M14.NRO_PROCESO + 0
  AND M14.TIPO_ENTIDAD_SALUD = M14.TIPO_ENTIDAD_SALUD + 0
  AND M14.COD_ENTIDAD_SALUD =M14.COD_ENTIDAD_SALUD
  AND M14.COD_PROVEEDOR = M14.COD_PROVEEDOR + 0
  AND M14.NRO_SUCURSAL = M14.NRO_SUCURSAL
  AND M14.NRO_LOTE = M14.NRO_LOTE + 0
  AND M14.STATUS_REGISTRO = 'P'
  -- AND cast(M14.FEC_ENVIO as string) BETWEEN '01/01/2023' AND '01/01/2024'
  AND M01.NRO_PROCESO = M14.NRO_PROCESO
  AND M01.NRO_LOTE = lpad(cast(M14.NRO_LOTE as string),7,'0')
  AND M01.NRO_PROCESO = M02.NRO_PROCESO
  AND M01.RUC_FACTURADOR = M02.RUC_FACTURADOR
  AND M01.SEDE_FACTURADOR = M02.SEDE_FACTURADOR
  AND M01.TIPO_DOCUMENTO_PAGO = M02.TIPO_DOCUMENTO_PAGO
  AND M01.NRO_DOCUMENTO_PAGO = M02.NRO_DOCUMENTO_PAGO
  AND M02.NRO_PROCESO = M05.NRO_PROCESO
  AND M02.RUC_FACTURADOR = M05.RUC_FACTURADOR
  AND M02.SEDE_FACTURADOR = M05.SEDE_FACTURADOR
  AND M02.TIPO_DOCUMENTO_PAGO = M05.TIPO_DOCUMENTO_PAGO
  AND M02.NRO_DOCUMENTO_PAGO = M05.NRO_DOCUMENTO_PAGO
  AND M02.CORRELATIVO_ATENCION = M05.CORRELATIVO_ATENCION
  --AND extract ( year from fec_hora_ocurrencia)in ( 2023)  
  AND CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION) IN (SELECT DISTINCT ID_siniestro_atencion FROM atencion )
  UNION ALL 
  SELECT DISTINCT
-- M14
  CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION)ID,
-- M05
M05.TIPO_PRODUCTO_FARMACIA, M05.COD_PRODUCTO_FARMACIA,
cast(M05.MONTO_NO_CUBIERTO_PRODUCTO as float64) AS MONTO_NO_CUBIERTO_PRODUCTO,
cast(M05.MONTO_CUBIERTO_PRODUCTO as float64) as MONTO_CUBIERTO_PRODUCTO,
cast(M05.COPAGO_PRODUCTO_FARMACIA as float64) COPAGO_PRODUCTO_FARMACIA,
CANT_VENTA_PRODUCTO, PRECIO_UNITARIO_SIMPUESTO
,(cast(M05.MONTO_CUBIERTO_PRODUCTO as float64)-CAST(M05.COPAGO_PRODUCTO_FARMACIA AS FLOAT64))copago_medicamento
  FROM `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF014_N` M14,
`rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF001_N` M01,
`rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_N` M02, --ATENCION
  -- `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF003_M` M03 --PROCEDIMIENTO
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF005_N` M05 --MEDICAMENTO
WHERE M14.NRO_PROCESO = M14.NRO_PROCESO + 0
  AND M14.TIPO_ENTIDAD_SALUD = M14.TIPO_ENTIDAD_SALUD + 0
  AND M14.COD_ENTIDAD_SALUD =M14.COD_ENTIDAD_SALUD
  AND M14.COD_PROVEEDOR = M14.COD_PROVEEDOR + 0
  AND M14.NRO_SUCURSAL = M14.NRO_SUCURSAL
  AND M14.NRO_LOTE = M14.NRO_LOTE + 0
  AND M14.STATUS_REGISTRO = 'P'
  -- AND cast(M14.FEC_ENVIO as string) BETWEEN '01/01/2023' AND '01/01/2024'
  AND M01.NRO_PROCESO = M14.NRO_PROCESO
  AND M01.NRO_LOTE = lpad(cast(M14.NRO_LOTE as string),7,'0')
  AND M01.NRO_PROCESO = M02.NRO_PROCESO
  AND M01.RUC_FACTURADOR = M02.RUC_FACTURADOR
  AND M01.SEDE_FACTURADOR = M02.SEDE_FACTURADOR
  AND M01.TIPO_DOCUMENTO_PAGO = M02.TIPO_DOCUMENTO_PAGO
  AND M01.NRO_DOCUMENTO_PAGO = M02.NRO_DOCUMENTO_PAGO
  AND M02.NRO_PROCESO = M05.NRO_PROCESO
  AND M02.RUC_FACTURADOR = M05.RUC_FACTURADOR
  AND M02.SEDE_FACTURADOR = M05.SEDE_FACTURADOR
  AND M02.TIPO_DOCUMENTO_PAGO = M05.TIPO_DOCUMENTO_PAGO
  AND M02.NRO_DOCUMENTO_PAGO = M05.NRO_DOCUMENTO_PAGO
  AND M02.CORRELATIVO_ATENCION = M05.CORRELATIVO_ATENCION
  --AND extract ( year from fec_hora_ocurrencia)in ( 2023)  
  AND CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION) IN (SELECT DISTINCT ID_siniestro_atencion FROM atencion )
  ),
total_procedimientos as(
SELECT distinct 2 CODIGO_ENTIDAD,
  ID_PRESTADOR,
  coalesce(id_persona,numero_afiliado) NUMERO_AFILIADO,s.ID_siniestro_atencion AS IDENTIFICADOR_ATENCION,
    'A' ESTADO,s.ID_siniestro_atencion ID_AUTORIZACION,
  case when REGEXP_REPLACE(COD_CLASIFICACION_GASTO, r'[^0-9]', '') = '' then COD_CLASIFICACION_GASTO else REGEXP_REPLACE(COD_CLASIFICACION_GASTO, r'[^0-9]', '') end num_procedimiento_origen,
    CASE WHEN REGEXP_REPLACE(COD_CLASIFICACION_GASTO, r'[^0-9]', '') = '' THEN TRIM(DESCRIPCIONSERVICIO) ELSE TRIM(GROUP_SERVICIO) END AS DES_PROCEDIMIENTO,
    IF(
      SAFE.PARSE_DATE('%Y-%m-%d', date_str) IS NOT NULL,
      SAFE.PARSE_DATE('%Y-%m-%d', date_str),
      DATE_TRUNC(
        DATE_ADD(
          DATE_TRUNC(SAFE.PARSE_DATE('%Y-%m-01', CONCAT(SUBSTR(date_str, 1, 7), '-01')), MONTH),
          INTERVAL 1 MONTH
        ),
        MONTH
      ) - INTERVAL 1 DAY
    ) AS FECHA_PRESTACION,
    '1' CANTIDAD,
    SUM(MONTO_CUBIERTO_SERVICIO) AS VALOR_FACTURADO,
    SUM((MONTO_CUBIERTO_SERVICIO-copago)) MONTO_PAGADO,
    (sum(MONTO_CUBIERTO_SERVICIO)-sum(copago)) AS COPAGO,
 
  FROM atencion s
  inner join PROCE_TRAMA ST
  ON s.ID_siniestro_atencion = ST.ID
  LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF003_maestro_procedimientos` MAESTRO ON
  CAST(REGEXP_REPLACE(COD_CLASIFICACION_GASTO, r'[^0-9]', '') AS STRING)= COD_SERVICIO
  LEFT JOIN TRAMA_AFILIADOS T ON SUBSTRING(s.NUMERO_AFILIADO,4)=T.COD_PACIENTE_ASEGURADO
  -- WHERE st.id='RS-1-4-24-44669355-1'
  GROUP BY 1,2,3,4,5,6,7,8,9,10
  ORDER BY s.ID_siniestro_atencion DESC
),
total_medicamentos AS (
  SELECT distinct 2 CODIGO_ENTIDAD,
  ID_PRESTADOR,
  coalesce(id_persona,numero_afiliado) NUMERO_AFILIADO,s.ID_siniestro_atencion AS IDENTIFICADOR_ATENCION,
    'A' ESTADO,s.ID_siniestro_atencion ID_AUTORIZACION,

    CONCAT(TIPO_PRODUCTO_FARMACIA,"-",TRIM(REPLACE(COD_PRODUCTO_FARMACIA,"'","")))  AS num_procedimiento_origen,-- lo cambio por med?
    TRIM(DESCRIPCION_MEDI) DES_PROCEDIMIENTO, -- LE CAMBIO DE NOMBRE?
    IF(
      SAFE.PARSE_DATE('%Y-%m-%d', date_str) IS NOT NULL,
      SAFE.PARSE_DATE('%Y-%m-%d', date_str),
      DATE_TRUNC(
        DATE_ADD(
          DATE_TRUNC(SAFE.PARSE_DATE('%Y-%m-01', CONCAT(SUBSTR(date_str, 1, 7), '-01')), MONTH),
          INTERVAL 1 MONTH
        ),
        MONTH
      ) - INTERVAL 1 DAY
    ) AS FECHA_PRESTACION, '1' CANTIDAD,
    SUM(MONTO_CUBIERTO_PRODUCTO) AS VALOR_FACTURADO,
    SUM((MONTO_CUBIERTO_PRODUCTO-copago_medicamento)) MONTO_PAGADO
    ,(sum(MONTO_CUBIERTO_PRODUCTO)-sum(copago_medicamento)) AS COPAGO
 
  FROM atencion s
  inner join MEDI_TRAMA ST
  ON s.ID_siniestro_atencion = ST.ID
  LEFT JOIN `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF005_M_maestro_medicamento` MAESTRO ON
  (CONCAT(TIPO_PRODUCTO_FARMACIA,"-",TRIM(REPLACE(COD_PRODUCTO_FARMACIA,"'","")))) = CONCAT_COD
  LEFT JOIN TRAMA_AFILIADOS T ON SUBSTRING(s.NUMERO_AFILIADO,4)=T.COD_PACIENTE_ASEGURADO
  -- WHERE st.id='RS-1-4-24-44669355-1'
  GROUP BY 1,2,3,4,5,6,7,8,9,10
  ORDER BY s.ID_siniestro_atencion DESC
), --select distinct ID from medi_trama
--4,645,001
--3,503,414
consolidado as (
select * from total_procedimientos
union all 
select * from total_medicamentos
)
SELECT *  FROM consolidado;
--CHERE FECHA_PRESTACION IS NULL;


--GROUP BY 1,2,3

--4183291

--4 183 291
--4 183 291

--3,626,455 -- TRAMA 


 EXPORT DATA OPTIONS(
 uri='gs://rs-prd-dlk-sbx-evco-telemedicina/CMBD3_4114_AMI_2024*.csv', format='CSV', overwrite=true, header=true,
 field_delimiter=';') as
 (
   SELECT * FROM `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.CMBD3_4114_AMI_2024`
/*CMBD2_404017_AMCC,CMBD2_4030_AMCC,CMBD2_4036_AMCC*/
   WHERE 1=1
 )
 LIMIT 200000000000000

-- EXPORT DATA OPTIONS(
-- uri='gs://rs-prd-dlk-sbx-evco-telemedicina/CMBD2_AX-6101*.csv',
-- format='CSV',
-- overwrite=true,
-- header=true,
-- field_delimiter=';') as
-- (
--   SELECT * FROM `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.pruebacmbd02`
--   WHERE 1=1
-- )
-- LIMIT 200000000000000

-- create or replace table `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.CMBD2_AX-6101` AS
CREATE OR REPLACE TABLE `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.pruebacmbd02` AS 
WITH
cartas_resumen as ( 
  SELECT id_siniestro, STRING_AGG(id_carta_garantia, ',') AS id_cartas 
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.carta_garantia_solicitud` carta
  LEFT JOIN UNNEST(CARTA_GARANTIA_VERSION)CGV
     WHERE carta.periodo=DATE_TRUNC(current_date(), MONTH)
    AND CGV.des_est_solicitud_origen <> 'ANULADO'
    AND date(CGV.fec_solicitud)>= '2019-01-01' 
  GROUP BY 1
),

siniestro_procedimiento as (
  SELECT 
    atencion_salud.id_pre_liquidacion_siniestro,
    agrupacion_cobertura_negocio,
    STRING_AGG(procedimiento_salud.num_colegio_medico, '-') AS id_medicos, 
    STRING_AGG(procedimiento_salud.nom_medico, '-') AS nom_medicos
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
    LEFT JOIN UNNEST(siniestro.atencion_salud) atencion_salud
    LEFT JOIN UNNEST(atencion_salud.procedimiento_salud) procedimiento_salud
  WHERE siniestro.periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND EXTRACT(YEAR FROM siniestro.fec_hora_ocurrencia)=2023
    AND siniestro.des_estado_siniestro_origen IN ('TRANSFERIDO A FINANZAS', 'CANCELADO')
  GROUP BY 1,2
),
--con esto nos quedamos con un solo medico por atencion
resumen_atencion as ( 
  SELECT DISTINCT id_pre_liquidacion_siniestro ,id_medicos ,nom_medicos,
    SPLIT(id_medicos, '-')[OFFSET(0)] id_medico,
    SPLIT(nom_medicos, '-')[OFFSET(0)] nom_medico
  FROM siniestro_procedimiento 
),--select * from resumen_atencion --21840931

resumen_siniestro_nivel as (
  SELECT DISTINCT id_siniestro ,id_medicos ,nom_medicos,
    SPLIT(id_medicos, '-')[OFFSET(0)] id_medico,
    SPLIT(nom_medicos, '-')[OFFSET(0)] nom_medico
  FROM (
    SELECT DISTINCT 
    id_siniestro  ,
    string_agg(num_colegio_medico,'-') id_medicos, 
    string_agg(nom_medico,'-') nom_medicos
    FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
      left join unnest(atencion_salud) atencion_salud
      left join unnest(procedimiento_salud) procedimiento_salud
    WHERE periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
      AND EXTRACT(YEAR FROM siniestro.fec_hora_ocurrencia) = 2023
      AND des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
    GROUP BY 1
  ) as siniestro_nivel 
),

-- con esto flageamos los siniestros de emergencia
flag_siniestro_emergencia as ( 
  SELECT DISTINCT siniestro.id_siniestro, 'E' as flag_emergencia
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
    left join unnest(atencion_salud) atencion_salud
  WHERE periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND EXTRACT(YEAR FROM siniestro.fec_hora_ocurrencia) =2023
    AND agrupacion_cobertura_negocio in ('EMERGENCIA')
    AND des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
), --1429653

-- con esto flageamos los siniestros de hospitalizacion
hospitalario_resumen as ( 
  SELECT DISTINCT
    2 as codigo_entidad ,
    'RIMAC' as descripcion,
    siniestro.id_persona_afiliado as numero_afiliado,
    siniestro.id_siniestro as identificador_atencion,
    siniestro.id_siniestro as nro_factura,
    '1' tipo,
    coalesce(concat(id_persona_proveedor_siniestro,'-',cod_sede_proveedor_siniestro),'AX-99999999') as id_prestador,
    nom_sede_proveedor_siniestro,
    1 as origen_atencion,
    case 
    when flag_emergencia ='E' THEN '1'
    else '8' end via_ingreso,
    case when agrupacion_cobertura_negocio = 'SEPELIO' then '2' else '9' end tipo_alta,
    "" as fecha_primera_intervencion_quirurgica,
    "0.00" as tiempo_quirurgico_total,
    "0.00" as tiempo_ventilacion_mecanica,
    ID_PRODUCTO,
    des_producto_agrupado,
    string_agg(atencion_salud.num_diagnostico_origen,',') as num_diagnostico_origen,
    cast( min(siniestro.fec_hora_ocurrencia) as datetime) fec_ingreso,
    cast(max(atencion_salud.fec_fin_internamiento) as datetime) as fec_salida,
    sum(atencion_salud.mnt_beneficio_tec_sol) as monto_facturado,
    sum(atencion_salud.mnt_beneficio_sin_impuesto_aprobado_sol) as monto_pagado,
    sum(atencion_salud.mnt_gasto_presentado_tec_sol) - sum(atencion_salud.mnt_beneficio_sin_impuesto_aprobado_sol) as copago_afiliado
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
    left join unnest(atencion_salud) atencion_salud
    left join flag_siniestro_emergencia f on (siniestro.id_siniestro=f.id_siniestro)
  WHERE periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND EXTRACT(YEAR FROM siniestro.fec_hora_ocurrencia)=2023
    AND agrupacion_cobertura_negocio in ('HOSPITALARIO','ONCOLOGIA','MATERNIDAD','EMERGENCIA','AMBULATORIO')
    AND des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
    AND id_cobertura_origen not in ('A61')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12 ,13,14,15,16
) 
-- con esto solo aquellos siniestros con mas de un dia de estancia
,mayor_dias_estancia as ( 
  SELECT DISTINCT * 
  FROM (SELECT a.*, date_diff(fec_salida,fec_ingreso, day) dias_estancia
  FROM hospitalario_resumen a)--hospitalario_resumen_con_estancia
  WHERE dias_estancia > 1
) 
,tipo1 as ( -- son los de origen 1
  SELECT 
    codigo_entidad ,
    descripcion,
    numero_afiliado,
    identificador_atencion,
    COALESCE(id_cartas,siniestro.identificador_atencion) AS id_autorizacion,
    nro_factura,
    '1' tipo,
    id_prestador,
    nom_sede_proveedor_siniestro,
    id_medico as id_efector,
    nom_medico as descripcion_efector,
    id_medico as id_prescriptor, 
    nom_medico as des_prescriptor,
    fec_ingreso,
    fec_salida,
    1 as origen_atencion,
    via_ingreso,
    tipo_alta,
    num_diagnostico_origen,
    fecha_primera_intervencion_quirurgica,
    tiempo_quirurgico_total,
    tiempo_ventilacion_mecanica,
    ID_PRODUCTO,
    des_producto_agrupado,
    monto_facturado,
    monto_pagado,
    copago_afiliado
  FROM mayor_dias_estancia siniestro
    left join cartas_resumen d on (siniestro.identificador_atencion=d.id_siniestro)
    left join resumen_siniestro_nivel f on (siniestro.identificador_atencion=f.id_siniestro)
)--select * from tipo1

,tipo5 as ( --son los de tipo5
  SELECT 
    2 as codigo_entidad ,
    'RIMAC' as descripcion,
    siniestro.id_persona_afiliado as numero_afiliado,
    atencion_salud.id_pre_liquidacion_siniestro as identificador_atencion,
    COALESCE(id_cartas,atencion_salud.id_pre_liquidacion_siniestro) AS id_autorizacion,
    siniestro.id_siniestro as nro_factura,
    '5' tipo,
    coalesce(concat(id_persona_proveedor_siniestro,'-',cod_sede_proveedor_siniestro),'AX-99999999') as id_prestador,
    nom_sede_proveedor_siniestro,
    id_medico as id_efector,
    nom_medico as descripcion_efector,
    id_medico as id_prescriptor, 
    nom_medico as des_prescriptor,
    cast(coalesce (cast(siniestro.fec_hora_ocurrencia as datetime),atencion_salud.fec_inicio_internamiento) as datetime) fec_ingreso,
    cast(coalesce(cast(siniestro.fec_hora_ocurrencia as datetime),atencion_salud.fec_inicio_internamiento) as datetime) as fec_salida,
    1 as origen_atencion,
    case when agrupacion_cobertura_negocio = 'ONCOLOGIA' then '6' else '8' end via_ingreso,
    case when agrupacion_cobertura_negocio = 'SEPELIO' then '2' else '9' end tipo_alta,
    atencion_salud.num_diagnostico_origen,
    "" as fecha_primera_intervencion_quirurgica,
    "0.00" as tiempo_quirurgico_total,
    "0.00" as tiempo_ventilacion_mecanica,
    ID_PRODUCTO,
    des_producto_agrupado,
    atencion_salud.mnt_beneficio_tec_sol as monto_facturado,
    atencion_salud.mnt_beneficio_sin_impuesto_aprobado_sol as monto_pagado,
    atencion_salud.mnt_gasto_presentado_tec_sol - atencion_salud.mnt_beneficio_sin_impuesto_aprobado_sol as copago_afiliado
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
    left join unnest(atencion_salud) atencion_salud
    left join cartas_resumen d on (siniestro.id_siniestro=d.id_siniestro)
    left join resumen_atencion f on (atencion_salud.id_pre_liquidacion_siniestro=f.id_pre_liquidacion_siniestro)
  WHERE periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND EXTRACT(YEAR FROM siniestro.fec_hora_ocurrencia) =2023
    AND agrupacion_cobertura_negocio in ('AMBULATORIO')
    AND id_cobertura_origen in ('A55','A26')
    AND siniestro.id_siniestro not in (SELECT identificador_atencion FROM tipo1)
    AND des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
),

tipo4 as( --son los tipo4
  SELECT 
  2 as codigo_entidad ,
    'RIMAC' as descripcion,
    siniestro.id_persona_afiliado as numero_afiliado,
    atencion_salud.id_pre_liquidacion_siniestro as identificador_atencion,
    COALESCE(id_cartas,atencion_salud.id_pre_liquidacion_siniestro) AS id_autorizacion,
    siniestro.id_siniestro as nro_factura,
    '4' tipo,
    coalesce(concat(id_persona_proveedor_siniestro,'-',cod_sede_proveedor_siniestro),'AX-99999999') as id_prestador,
    nom_sede_proveedor_siniestro,
    id_medico as id_efector,
    nom_medico as descripcion_efector,
    id_medico as id_prescriptor, 
    nom_medico as des_prescriptor,
    cast(coalesce (cast(siniestro.fec_hora_ocurrencia as datetime),atencion_salud.fec_inicio_internamiento) as datetime) fec_ingreso,
    cast(coalesce(cast(siniestro.fec_hora_ocurrencia as datetime),atencion_salud.fec_inicio_internamiento) as datetime) as fec_salida,
    1 as origen_atencion,
    case when agrupacion_cobertura_negocio = 'ONCOLOGIA' then '6' else '8' end via_ingreso,
    case when agrupacion_cobertura_negocio = 'SEPELIO' then '2' else '9' end tipo_alta,
    atencion_salud.num_diagnostico_origen,
    "" as fecha_primera_intervencion_quirurgica,
    "0.00" as tiempo_quirurgico_total,
    "0.00" as tiempo_ventilacion_mecanica,
    ID_PRODUCTO,
    des_producto_agrupado,
    atencion_salud.mnt_beneficio_tec_sol as monto_facturado,
    atencion_salud.mnt_beneficio_sin_impuesto_aprobado_sol as monto_pagado,
    atencion_salud.mnt_gasto_presentado_tec_sol - atencion_salud.mnt_beneficio_sin_impuesto_aprobado_sol as copago_afiliado
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
    left join unnest(atencion_salud) atencion_salud
    left join cartas_resumen d on (siniestro.id_siniestro=d.id_siniestro)
    left join resumen_atencion f on (f.id_pre_liquidacion_siniestro=atencion_salud.id_pre_liquidacion_siniestro)
  WHERE periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND EXTRACT(YEAR FROM siniestro.fec_hora_ocurrencia) =2023
    AND agrupacion_cobertura_negocio='ONCOLOGIA'
    AND id_cobertura_origen in ('O17','O03','O05','O07','O09','O00','O01','O04','O18')
    AND siniestro.id_siniestro not in (SELECT identificador_atencion FROM TIPO1)
    AND des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
),

tipo2 as ( -- son los tipo2
  SELECT DISTINCT
    2 as codigo_entidad ,
    'RIMAC' as descripcion,
    siniestro.id_persona_afiliado as numero_afiliado,
    atencion_salud.id_pre_liquidacion_siniestro as identificador_atencion,
    COALESCE(id_cartas,atencion_salud.id_pre_liquidacion_siniestro) AS id_autorizacion,
    siniestro.id_siniestro as nro_factura,
    '2' tipo,
    coalesce(concat(id_persona_proveedor_siniestro,'-',cod_sede_proveedor_siniestro),'AX-99999999') as id_prestador,
    nom_sede_proveedor_siniestro,
    id_medico as id_efector,
    nom_medico as descripcion_efector,
    id_medico as id_prescriptor, 
    nom_medico as des_prescriptor,
    cast(coalesce (cast(siniestro.fec_hora_ocurrencia as datetime),atencion_salud.fec_inicio_internamiento) as datetime) fec_ingreso,
    cast(coalesce(cast(siniestro.fec_hora_ocurrencia as datetime),atencion_salud.fec_inicio_internamiento) as datetime) as fec_salida,
    1 as origen_atencion,
    case when agrupacion_cobertura_negocio = 'ONCOLOGIA' then '6' else '8' end via_ingreso,
    case when agrupacion_cobertura_negocio = 'SEPELIO' then '2' else '9' end tipo_alta,
    atencion_salud.num_diagnostico_origen,
    "" as fecha_primera_intervencion_quirurgica,
    "0.00" as tiempo_quirurgico_total,
    "0.00" as tiempo_ventilacion_mecanica,
    ID_PRODUCTO,
    des_producto_agrupado,
    atencion_salud.mnt_beneficio_tec_sol as monto_facturado,
    atencion_salud.mnt_beneficio_sin_impuesto_aprobado_sol as monto_pagado,
    atencion_salud.mnt_gasto_presentado_tec_sol - atencion_salud.mnt_beneficio_sin_impuesto_aprobado_sol as copago_afiliado
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestrO
    left join unnest(atencion_salud) atencion_salud
    left join cartas_resumen d on (siniestro.id_siniestro=d.id_siniestro)
    left join resumen_atencion f on (atencion_salud.id_pre_liquidacion_siniestro=f.id_pre_liquidacion_siniestro)
  WHERE periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND EXTRACT(YEAR FROM siniestro.fec_hora_ocurrencia)=2023
    AND agrupacion_cobertura_negocio='AMBULATORIO'
    AND siniestro.id_siniestro not in (SELECT identificador_atencion FROM tipo1)
    AND id_cobertura_origen not in ('A55','A26')
    AND des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
),

tipo6 as ( --son los tipo6
  SELECT DISTINCT
    2 as codigo_entidad ,
    'RIMAC' as descripcion,
    siniestro.id_persona_afiliado as numero_afiliado,
    atencion_salud.id_pre_liquidacion_siniestro as identificador_atencion,
    COALESCE(id_cartas,atencion_salud.id_pre_liquidacion_siniestro) AS id_autorizacion,
    siniestro.id_siniestro as nro_factura,
    '6' tipo,
    coalesce(concat(id_persona_proveedor_siniestro,'-',cod_sede_proveedor_siniestro),'AX-99999999') as id_prestador,
    nom_sede_proveedor_siniestro,
    id_medico as id_efector,
    nom_medico as descripcion_efector,
    id_medico as id_prescriptor, 
    nom_medico as des_prescriptor,
    cast(coalesce (cast(siniestro.fec_hora_ocurrencia as datetime),atencion_salud.fec_inicio_internamiento) as datetime) fec_ingreso,
    cast(coalesce(cast(siniestro.fec_hora_ocurrencia as datetime),atencion_salud.fec_inicio_internamiento) as datetime) as fec_salida,
    1 as origen_atencion,
    case when agrupacion_cobertura_negocio = 'ONCOLOGIA' then '6' else '8' end via_ingreso,
    case when agrupacion_cobertura_negocio = 'SEPELIO' then '2' else '9' end tipo_alta,
    atencion_salud.num_diagnostico_origen,
    "" as fecha_primera_intervencion_quirurgica,
    "0.00" as tiempo_quirurgico_total,
    "0.00" as tiempo_ventilacion_mecanica,
    ID_PRODUCTO,
    des_producto_agrupado,
    atencion_salud.mnt_beneficio_tec_sol as monto_facturado,
    atencion_salud.mnt_beneficio_sin_impuesto_aprobado_sol as monto_pagado,
    atencion_salud.mnt_gasto_presentado_tec_sol - atencion_salud.mnt_beneficio_sin_impuesto_aprobado_sol as copago_afiliado
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` siniestro
    left join unnest(atencion_salud) atencion_salud
    left join cartas_resumen d on (siniestro.id_siniestro=d.id_siniestro)
    left join resumen_atencion f on (f.id_pre_liquidacion_siniestro=atencion_salud.id_pre_liquidacion_siniestro)
  WHERE periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND EXTRACT(YEAR FROM siniestro.fec_hora_ocurrencia) =2023
    AND siniestro.id_siniestro not in (SELECT identificador_atencion FROM tipo1)
    AND atencion_salud.id_pre_liquidacion_siniestro not in (SELECT identificador_atencion FROM tipo2)
    AND atencion_salud.id_pre_liquidacion_siniestro not in (SELECT identificador_atencion FROM tipo4)
    AND atencion_salud.id_pre_liquidacion_siniestro not in (SELECT identificador_atencion FROM tipo5)
    AND des_estado_siniestro_origen in ('TRANSFERIDO A FINANZAS','CANCELADO')
    --AND id_producto in (SELECT id_producto FROM productos_poblacion)
)


,consolidado_MDM as (
  SELECT * FROM tipo6
  union all
  SELECT * FROM tipo2
  union all 
  SELECT * FROM tipo4
  union all
  SELECT * FROM tipo5 
  union all 
  SELECT * FROM tipo1
) --1581116887.511

,siniestro as (

select  DISTINCT CONCAT(SPLIT(identificador_atencion,'-')[OFFSET(0)],'-',SPLIT(identificador_atencion,'-')[OFFSET(1)],'-',
  SPLIT(identificador_atencion,'-')[OFFSET(2)],'-',
  SPLIT(identificador_atencion,'-')[OFFSET(3)],'-',
  SPLIT(identificador_atencion,'-')[OFFSET(4)] ) id_siniestro,TIPO
from consolidado_MDM
where 1=1 
and id_producto='AX-6101' ----FILTRA PRODUCTO Y AÑO

) --SELECT COUNT(DISTINCT ID_SINIESTRO) FROM SINIESTRO 4,183,291
,PERSONA_dni AS (select DISTINCT
    p.id_persona,
    d.num_documento

from `rs-prd-dlk-dd-stging-f0e1.stg_modelo_persona.persona` P
LEFT JOIN UNNEST(DOCUMENTO_IDENTIDAD) D
where 1=1 and d.ind_documento_principal = '1' 
--AND D.NUM_DOCUMENTO LIKE '%13180716%' ---'%51939003%' 1048290201
--and id_persona='AX-16225496'
)
,TRAMA_AFILIADOS AS 
(SELECT DISTINCT NRO_DOCUMENTO_IDENTIDAD,COD_PACIENTE_ASEGURADO,ID_PERSONA 
FROM `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_M` TR
LEFT JOIN PERSONA_dni P ON concat("AX-",COD_PACIENTE_ASEGURADO)=P.id_persona 
-- union all 
-- select distinct NRO_DOCUMENTO_IDENTIDAD,COD_PACIENTE_ASEGURADO,id_persona
-- from  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_N` nro
-- LEFT JOIN PERSONA_dni P ON nro.NRO_DOCUMENTO_IDENTIDAD=P.num_documento 
-- --WHERE COD_PACIENTE_ASEGURADO='00000000000005564774' --'00000002978433' 
)
,atencion as (select CONCAT('AX-',M14.COD_PROVEEDOR,'-',M01.SEDE_FACTURADOR) ID_PRESTADOR,M02.FEC_INICIO_ATENCION date_str,
   CONCAT('AX-',M02.COD_PACIENTE_ASEGURADO) NUMERO_AFILIADO,
CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION)ID_siniestro_atencion,
CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO)ID_siniestro,

FEC_INICIO_ATENCION FEC_INGRESO_HOSPITALARIO,  --DATE_ADD(PARSE_DATE('%Y-%m-%d', FEC_EGRESO_HOSPITALARIO), INTERVAL dias_estancia DAY)
CASE WHEN dias_estancia IS NOT NULL THEN DATE_ADD(PARSE_DATE('%Y%m%d', FEC_INICIO_ATENCION), INTERVAL dias_estancia DAY) 
 ELSE PARSE_DATE('%Y%m%d', FEC_INICIO_ATENCION) END FEC_EGRESO_HOSPITALARIO 

,COPAGO_FIJO_AFEC_IGV,COPAGO_VARIABLE_AFEC_IGV,COPAGO_FIJO_EXON_IGV,COPAGO_VARIABLE_EXON_IGV,CODIGO_CIE101,      CASE 
      WHEN M02.COPAGO_VARIABLE_AFEC_IGV<='0.0' THEN cast(M02.COPAGO_VARIABLE_EXON_IGV as float64) ELSE cast(M02.COPAGO_VARIABLE_AFEC_IGV as float64) END COPAGO_VARIABLE,
      CASE 
        WHEN M02.COPAGO_FIJO_AFEC_IGV <='0.0' THEN CAST(M02.COPAGO_FIJO_EXON_IGV AS FLOAT64) ELSE CAST(M02.COPAGO_FIJO_AFEC_IGV AS FLOAT64) END COPAGO_FIJO,
    cast(M02.TOTAL_GTO_PRESENTADO as float64)TOTAL_GTO_PRESENTADO
  FROM `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF014_M` M14,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF001_M` M01,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_M` M02 --ATENCION
  LEFT JOIN mayor_dias_estancia estancia ON (CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO) = estancia.identificador_atencion)
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
    select CONCAT('AX-',M14.COD_PROVEEDOR,'-',M01.SEDE_FACTURADOR) ID_PRESTADOR,M02.FEC_INICIO_ATENCION date_str,
   CONCAT('AX-',M02.COD_PACIENTE_ASEGURADO) NUMERO_AFILIADO,
CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO,'-',M02.CORRELATIVO_ATENCION)ID_siniestro_atencion,CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO)ID_siniestro,
FEC_INICIO_ATENCION FEC_INGRESO_HOSPITALARIO,
CASE WHEN dias_estancia IS NOT NULL THEN DATE_ADD(PARSE_DATE('%Y%m%d', FEC_INICIO_ATENCION), INTERVAL dias_estancia DAY) 
 ELSE PARSE_DATE('%Y%m%d', FEC_INICIO_ATENCION) END FEC_EGRESO_HOSPITALARIO 
,COPAGO_FIJO_AFEC_IGV,COPAGO_VARIABLE_AFEC_IGV,COPAGO_FIJO_EXON_IGV,COPAGO_VARIABLE_EXON_IGV,CODIGO_CIE101,      CASE 
      WHEN M02.COPAGO_VARIABLE_AFEC_IGV<='0.0' THEN cast(M02.COPAGO_VARIABLE_EXON_IGV as float64) ELSE cast(M02.COPAGO_VARIABLE_AFEC_IGV as float64) END COPAGO_VARIABLE,
      CASE 
        WHEN M02.COPAGO_FIJO_AFEC_IGV <='0.0' THEN CAST(M02.COPAGO_FIJO_EXON_IGV AS FLOAT64) ELSE CAST(M02.COPAGO_FIJO_AFEC_IGV AS FLOAT64) END COPAGO_FIJO,
    cast(M02.TOTAL_GTO_PRESENTADO as float64)TOTAL_GTO_PRESENTADO
  FROM `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF014_N` M14,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF001_N` M01,
  `rs-nprd-dlk-data-rwz-51a6.bdrsa__app_eps.TNVF002_N` M02 --ATENCION
  LEFT JOIN mayor_dias_estancia estancia ON (CONCAT ( 'RS-' ,M02.TIPO_ENTIDAD_SALUD,'-',M02.COD_ENTIDAD_SALUD,'-',M02.ANO_DOCUMENTO,'-',M02.NRO_DOCUMENTO) = estancia.identificador_atencion)
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
,sede_proveedor AS (
  select distinct concat(id_persona_proveedor_siniestro,"-",cod_sede_proveedor_siniestro) ID_PRESTADOR, num_documento_proveedor_siniestro, nom_sede_proveedor_siniestro
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud`
  where 1=1
  AND id_persona_proveedor_siniestro is not null
) --select * from atencion where dias_estancia is not null

,final as(
SELECT distinct 2 CODIGO_ENTIDAD,'RIMAC' as descripcion,coalesce(id_persona,S.numero_afiliado) NUMERO_AFILIADO,s.ID_siniestro_atencion AS IDENTIFICADOR_ATENCION,s.ID_siniestro_atencion AS ID_AUTORIZACION,s.ID_siniestro_atencion AS NRO_FACTURA,TIPO,coalesce(S.ID_PRESTADOR,'AX-99999999')ID_PRESTADOR, coalesce(nom_sede_proveedor_siniestro,'NO VIENE EN LA DATA') nom_sede_proveedor_siniestro,
    '17645948' as id_efector,
   'POR DEFINIR  POR DEFINIR POR DEFINIR' as descripcion_efector ,
  '17645948' as id_prescriptor, 
  'POR DEFINIR  POR DEFINIR POR DEFINIR' as des_prescriptor,
  FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', DATETIME(PARSE_DATE('%Y%m%d', FEC_INGRESO_HOSPITALARIO))) AS fec_ingreso,
  FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', DATETIME(FEC_EGRESO_HOSPITALARIO)) AS fec_salida
  ,1 origen_atencion,
  '8'via_ingreso,
  '9'tipo_alta,CODIGO_CIE101 num_diagnostico_origen,
   "" as fecha_primera_intervencion_quirurgica,
    "0.00" as tiempo_quirurgico_total,
    "0.00" as tiempo_ventilacion_mecanica,
    TOTAL_GTO_PRESENTADO monto_facturado,
    (TOTAL_GTO_PRESENTADO-COPAGO_VARIABLE-COPAGO_FIJO) monto_pagado,
    (TOTAL_GTO_PRESENTADO - (TOTAL_GTO_PRESENTADO-COPAGO_VARIABLE-COPAGO_FIJO))as copago_afiliado
    

FROM atencion S
LEFT JOIN TRAMA_AFILIADOS T ON SUBSTRING(S.NUMERO_AFILIADO,4)=T.COD_PACIENTE_ASEGURADO
LEFT JOIN SINIESTRO C ON S.ID_siniestro=C.id_siniestro
LEFT JOIN sede_proveedor sp ON s.ID_PRESTADOR = sp.ID_PRESTADOR
--LEFT JOIN resumen_siniestro_nivel R ON S
/*total_procedimientos as(
SELECT distinct 2 CODIGO_ENTIDAD,
  ID_PRESTADOR,
  coalesce(id_persona,numero_afiliado) NUMERO_AFILIADO,s.ID_siniestro_atencion AS IDENTIFICADOR_ATENCION,
    'A' ESTADO,s.ID_siniestro_atencion ID_AUTORIZACION,
  case when REGEXP_REPLACE(COD_CLASIFICACION_GASTO, r'[^0-9]', '') = '' then COD_CLASIFICACION_GASTO else REGEXP_REPLACE(COD_CLASIFICACION_GASTO, r'[^0-9]', '') end num_procedimiento_origen,
    CASE WHEN REGEXP_REPLACE(COD_CLASIFICACION_GASTO, r'[^0-9]', '') = '' THEN DESCRIPCIONSERVICIO ELSE GROUP_SERVICIO END AS DES_PROCEDIMIENTO,
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
),**/
)
 --SELECT sum(monto_pagado),count(*) FROM consolidado
select * from final 
-- where IDENTIFICADOR_ATENCION = 'RS-1-4-23-39249941-1'
-- where fec_ingreso is not null


-- select  *
-- from `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.pruebacmbd02` 
-- where IDENTIFICADOR_ATENCION = 'RS-1-4-23-39249941-1'
-- group by 1


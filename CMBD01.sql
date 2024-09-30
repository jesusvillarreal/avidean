--create OR REPLACE table `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.CMBD1PO` AS
--with 
DECLARE max_date_dir,periodo_proc date;
SET periodo_proc = '2024-04-01'; --peridoo mas acutal
SET max_date_dir = (SELECT MAX(periodo) FROM `rs-prd-dlk-dd-stging-f0e1.stg_modelo_persona.direccion_persona` WHERE periodo <= periodo_proc); 

--CREATE or replace table `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.CMBD_1_histt` as  

WITH
dx_grupo_riesgo AS (
  select * from `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.diagnosticos_grupo_riesgo`  --data entry
),

siniestros as (
SELECT distinct
sn.fec_hora_ocurrencia,
cast(sn.fec_hora_ocurrencia as string format 'YYYYMM') aniomes,
sn.id_siniestro, 
sn.id_persona_afiliado,
sd.num_diagnostico_origen,
sd.id_cobertura_origen,
from `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` sn
left join unnest(atencion_salud) as sd
where 1=1
and sn.periodo='2024-04-01' --periodo mas actual
and fec_hora_ocurrencia between '2023-03-01' and '2023-08-31' --rango de tiempo de analisis para avedian de meses a evaluar cronicos
order by fec_hora_ocurrencia desc
),
base0 as (
select a.*,b.grupo_riesgo 
from siniestros a
left join dx_grupo_riesgo b on a.num_diagnostico_origen=b.num_diagnostico_origen
where 1=1
and b.grupo_riesgo in ('G3 - CRONICOS COMPLEJOS'	,'G2 - CRONICOS SIMPLES')
),

Base1 as (
select distinct
id_persona_afiliado,
num_diagnostico_origen,
grupo_riesgo,
count (distinct id_siniestro) as cantidad
from Base0
group by id_persona_afiliado,num_diagnostico_origen,grupo_riesgo
),

flag_prioridad AS (
select id_persona_afiliado, num_diagnostico_origen,grupo_riesgo
from Base1
where 1=1
and cantidad>=2  --2 o mas a tenciones cronicas por el mismo dx
),

afiliado_dx_antiguedad as (

  select distinct id_persona_afiliado, num_diagnostico_origen ,min(fec_hora_ocurrencia) fecha_antiguedad
  from `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` a
  left join unnest(atencion_salud) b
  where periodo='2024-04-01'  --periodo mas actual
  and extract (year from fec_hora_ocurrencia) >2000  --desde el año 2000 en adelante
  group by 1,2
),
flag_prioridad_fechas_antiguedad as (  -- con esto teneos el identificamos la fecha mas antigua del diangostico del afiliado
select a.*,cast(b.fecha_antiguedad as datetime) fecha_antiguedad
from flag_prioridad a 
left join afiliado_dx_antiguedad b on (a.id_persona_afiliado=b.id_persona_afiliado and a.num_diagnostico_origen=b.num_diagnostico_origen)
),

cronicos_concepto_negocio as (

select id_persona_afiliado,string_agg(num_diagnostico_origen,',') as diag_h_2_cronicos, string_agg(cast(fecha_antiguedad as string),',') as fecha_antiguedad_dx  --con esto generamos una lista
from flag_prioridad_fechas_antiguedad
group by 1
),

persona as (
SELECT id_persona,nom_corto, concat(ape_paterno,' ', ape_materno) as apellidos ,b.num_documento
FROM `rs-prd-dlk-dd-stging-f0e1.stg_modelo_persona.persona` a
left join unnest(documento_identidad) b 
where 1=1
 and ind_documento_principal='1'
),

departamento as (
select distinct COD_DEPARTAMENTO,DESCRIPCION 
from `rs-prd-dlk-dd-rawzone-a40f.bdrsa__app_eps.TABLA_DEPARTAMENTO`
where 1=1

),
provincia as (
select distinct cod_provincia, DESCRIPCION  
from `rs-prd-dlk-dd-rawzone-a40f.bdrsa__app_eps.TABLA_PROVINCIA`
WHERE 1=1

),
ruc as (
SELECT id_persona,b.num_documento
from `rs-prd-dlk-dd-stging-f0e1.stg_modelo_persona.persona`
left join unnest(documento_identidad) b 
where 1=1
and b.tip_documento='RUC'
)
,vig as (
   select  distinct id_unidad_asegurable,id_persona_afiliado,'1000-01-01 00:00:00' fecha_de_baja
 from  `rs-shr-al-analyticsz-prj-ebc1.anl_produccion.prima_inducida_salud`
 where 1=1 --and id_persona_afiliado='AX-2935332'
 and periodo='2024-05-01' and periodo_bitacora='2024-05-01'
 --and des_producto_agrupado='PLANES MEDICOS'
),fin as (
SELECT 
a.id_unidad_asegurable ,fec_fin_movimiento,
 row_number() over (partition by a.id_unidad_asegurable order by fec_fin_movimiento desc) r,
FROM  `rs-nprd-dlk-dd-stgz-8ece.stg_modelo_poliza.afiliado_movimiento`  a
left join vig b on a.id_unidad_asegurable=b.id_unidad_asegurable
where 1=1 
and fecha_de_baja <>'1000-01-01 00:00:00'--and tip_movimiento='EXCLUSION'
--and id_unidad_asegurable='RS-2-1-80-2935332-0000010282-2935332-2935332'
),afiliado_fecha_ultima as (
select distinct
id_unidad_asegurable,
fec_fin_movimiento  
from fin
where r=1),resultado as(
SELECT distinct
2 codigo_entidad , --check
cod_producto_origen,
'RIMAC' as descripcion,--check
a.id_persona_afiliado as numero_afiliado, -- check
fec_afiliacion_asegurado,
cast(fec_afiliacion_asegurado as datetime) as fecha_de_alta, 
case when a.id_unidad_asegurable=bb.id_unidad_asegurable then cast(bb.fecha_de_baja as string) else cast(fec_fin_movimiento as string) end fecha_de_baja,
b.nom_corto , 
b.apellidos, 
i.num_documento ruc, 
b.num_documento,
cast(fec_nacimiento_afiliado as datetime) as fecha_nacimiento,
trim(des_sexo_afiliado) as des_sexo_afiliado , 
case when c.des_departamento in('PROV. CONST. DEL CAL') then 'CALLAO' 
when coalesce(c.des_departamento, 'ND') in ('','ND') then 'NO DETERMINADO' else c.des_departamento end as des_departamento_gestion_servicio_asegurado,
case when c.des_provincia in('PROV. CONST. DEL CALLAO') then 'CALLAO' 
when coalesce(c.des_provincia, 'ND') in ('','ND') then 'NO DETERMINADO' else c.des_provincia end as des_provincia_gestion_servicio_asegurado,
id_poliza, 
num_poliza, 
des_producto_agrupado,
case when id_producto='AX-6101' THEN 'NG'
ELSE 'G' END TIPO_AFILIACION,
mnt_prima_neta_inducida_afiliado_sol, 
j.diag_h_2_cronicos as dia_cronicos_regla_negocio_adicional,
fecha_antiguedad_dx,
CASE WHEN fec_afiliacion_asegurado>MIN(periodo_bitacora) THEN MIN(periodo_bitacora) ELSE fec_afiliacion_asegurado END periodo_afiliacion, --check
FROM 
`rs-shr-al-analyticsz-prj-ebc1.anl_produccion.prima_inducida_salud` a 
left join vig bb on a.id_unidad_asegurable=bb.id_unidad_asegurable
left join afiliado_fecha_ultima cc on a.id_unidad_asegurable=cc.id_unidad_asegurable
left join persona b on (a.id_persona_afiliado=b.id_persona)
left join `rs-prd-dlk-dd-stging-f0e1.stg_modelo_persona.direccion_persona` c 
        on a.id_persona_afiliado = c.id_persona and c.ind_principal = 1 and c.ind_royal!="1" --se filtra la marca royal
        and c.periodo = max_date_dir
left join departamento e on (e.descripcion=c.des_departamento)
left join provincia f on (f.descripcion=c.des_provincia)
--left join base_preexistencias_final h on (a.id_persona_afiliado=h.id_persona_afiliado)
left join ruc i on (a.id_persona_afiliado=i.id_persona)
left join cronicos_concepto_negocio j on (a.id_persona_afiliado=j.id_persona_afiliado)
WHERE  1=1 AND a.PERIODO='2024-05-01'
  and periodo_bitacora >='2019-01-01'
  and des_producto_agrupado='PLANES MEDICOS'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  ),
  final as 
  (select distinct
codigo_entidad , --check
a.descripcion,--check
numero_afiliado, -- check
format_date("%Y%m", periodo_afiliacion) as periodo_afiliacion, --check
fecha_de_alta, --check
fecha_de_baja,
nom_corto,
apellidos,
ruc,
num_documento,
fecha_nacimiento,
case when des_sexo_afiliado='MASCULINO' then 'M'
when des_sexo_afiliado='FEMENINO' then 'F' ELSE 'I' END des_sexo_afiliado , 
cod_departamento, 
des_departamento_gestion_servicio_asegurado, 
cod_provincia , 
des_provincia_gestion_servicio_asegurado,
id_poliza, 
num_poliza, 
des_producto_agrupado,
TIPO_AFILIACION,
mnt_prima_neta_inducida_afiliado_sol, 
dia_cronicos_regla_negocio_adicional as dx_cronicos_regla_negocio_adicional,
fecha_antiguedad_dx
from resultado a 
left join departamento e on (e.descripcion=a.des_departamento_gestion_servicio_asegurado)
left join provincia f on (f.descripcion=a.des_provincia_gestion_servicio_asegurado))
,
 tablafin as
 (select distinct codigo_entidad , --check
descripcion,--check
numero_afiliado, -- check
concat(SUBSTR( CAST(periodo_afiliacion AS STRING),0,4) ,'-',SUBSTR( CAST(periodo_afiliacion AS STRING),5,2) ) periodo_afiliacion,
fecha_de_alta, --check
fecha_de_baja,
nom_corto , --check
apellidos, --cchek
coalesce(cast(ruc as string),'0') as ruc, --check
num_documento,
coalesce(fecha_nacimiento) AS fecha_nacimiento,
 des_sexo_afiliado , 
cod_departamento, 
des_departamento_gestion_servicio_asegurado, 
cod_provincia , 
des_provincia_gestion_servicio_asegurado,--check
id_poliza, 
num_poliza, 
des_producto_agrupado,
TIPO_AFILIACION,
mnt_prima_neta_inducida_afiliado_sol, 
dx_cronicos_regla_negocio_adicional as dx_cronicos_relacionados,
fecha_antiguedad_dx as fecha_antiguedad_dx_afiliado,
--row_number() over (partition by numero_afiliado order by periodo_afiliacion desc,prioridad_producto asc) r,
from final a)
,finn as (
select  
codigo_entidad , --check
descripcion,--check
numero_afiliado, -- check
periodo_afiliacion,
fecha_de_alta, --check
cast(fecha_de_baja as datetime) fecha_de_baja, --check
'.' as nom_corto , --check
'.' as apellidos, --cchek
numero_afiliado as ruc, --check
numero_afiliado as num_documento,
fecha_nacimiento,
des_sexo_afiliado , 
coalesce(cod_departamento,999999) as cod_departamento , 
des_departamento_gestion_servicio_asegurado, 
coalesce(cod_provincia,999999) as cod_provincia, 
des_provincia_gestion_servicio_asegurado,--check
id_poliza, 
num_poliza, 
--des_producto_agrupado,
TIPO_AFILIACION,
coalesce(mnt_prima_neta_inducida_afiliado_sol,0) mnt_prima_neta_inducida_afiliado_sol , 
dx_cronicos_relacionados,
fecha_antiguedad_dx_afiliado,
from tablafin)
select COUNT(DISTINCT numero_afiliado) FROM FINN
WHERE 1=1
--and fecha_de_baja='1000-01-01 00:00:00'
  --and a.id_persona_afiliado='AX-2935332';
;

/*SELECT *numero_afiliado,id_poliza,num_poliza,periodo_afiliacion,DES_PRODUCTO_AGRUPADO,mnt_prima_neta_inducida_afiliado_sol,MIN(periodo_bitacora)MIN_PERIODO,
CASE WHEN fec_afiliacion_asegurado>MIN(periodo_bitacora) THEN MIN(periodo_bitacora) ELSE fec_afiliacion_asegurado END AFILIACION,
from `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.CMBD_1_histt`
--FROM `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas.CMBD1PO`
WHERE 1=1
--and fecha_de_baja='1000-01-01 00:00:00'
GROUP BY 1*/




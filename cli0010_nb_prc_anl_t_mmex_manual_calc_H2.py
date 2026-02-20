#!/usr/bin/env python
# coding: utf-8

# ## cli0010_nb_prc_anl_t_mmex_manual_calc_H2
# 
# 
# 

# In[43]:


val v_year = "2023"
val v_month  = "01"
val escenario = "REAL" // {ALL / PA-UPA / EST / REAL / PA / UPA}
val applicationName: String = ""
val parentUid:String = "N/A"
val uuid:String = "N/A"


# In[44]:


get_ipython().run_line_magic('run', 'cli0010/util/cli0010_nb_prc_app_obj_module_properties')


# In[45]:


get_ipython().run_line_magic('run', 'cli0010/util/cli0010_nb_prc_app_obj_module_library')


# In[46]:


var v_periodo = v_year.concat(v_month).concat("01").toInt

var pperiodo = v_year.concat(v_month).toInt

val anio = v_periodo.toString().substring(0, 4).toInt

var v_periodo_ytd = anio.toString().concat("00").concat("00").toInt


# In[47]:


spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")


# In[48]:


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import com.repsol.datalake.log._

implicit val uid_puid_app = (uuid, parentUid, applicationName)

LogHelper().logStart()

// Variables de salida copydata usando Pipelines
val pl_output = s"Generación de Modelo OK"

print(v_year)


# In[49]:


import java.io.IOException
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DecimalType

def look_column_else_zero0( tabla : DataFrame , columna : String) : DataFrame =
{
    if ( tabla.columns.map(_.toUpperCase).contains(columna.toUpperCase) ) {
     return tabla }
    else  {
        return tabla.withColumn(columna, lit(0).cast(DecimalType(24,10))) }
        
}

// def look_column_else_zero( tabla : DataFrame , columna : String) : DataFrame =
// {
//     if ( tabla.columns.map(_.toUpperCase).contains(columna.toUpperCase) ) {
//      return tabla }
//     else  {
//         return tabla.withColumn(columna, lit(null).cast(DecimalType(24,10))) }
        
// }

def look_column_else_zero(tabla: DataFrame, columna: String): DataFrame = {
  val colExpr = coalesce(tabla(columna), lit(null).cast(DecimalType(24, 10)).alias(columna))
  
  return tabla.withColumn(columna, colExpr)
}

def look_columns_else_null(tabla: DataFrame, columns: List[String]): DataFrame = {
  columns.foldLeft(tabla) { (df, column) =>
    if (df.columns.map(_.toUpperCase).contains(column.toUpperCase)) {
      df
    } else {
      df.withColumn(column, lit(null).cast(DecimalType(24, 10)))
    }
  }
}


# In[50]:


//Paths del LakeHouse
val lakehousePath = "CLI0010/trn"
val edwPath = "CLI0010/edw"
val container_output = "lakehouse"

val linked_service_name = "DL_COM"
val my_container = "processed"
val cont_lakehouse = "lakehouse"
val my_account = conexion("Endpoint").toString.substring(8)


# In[51]:


val business="AM_COM_Vista_Cliente"
val ds_output = s"cli0010_tb_fac_m_mod_mmex_icv/"
val pathWriteTemp = s"$edwPath/$business/$ds_output"
val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

var t_Pool_fac_SQLPOOL = spark.read.parquet(parquet_path_temp)


# In[52]:


if (escenario == "REAL"){
    t_Pool_fac_SQLPOOL = t_Pool_fac_SQLPOOL.where(col("num_periodo") === pperiodo && col("id_escenario") === 1).cache
}else if (escenario == "PA-UPA"){
    t_Pool_fac_SQLPOOL = t_Pool_fac_SQLPOOL.where(col("num_periodo") === pperiodo && col("id_escenario").isin(2,3)).cache
}else if (escenario == "EST"){
    t_Pool_fac_SQLPOOL = t_Pool_fac_SQLPOOL.where(col("num_periodo") === pperiodo && col("id_escenario") === 4).cache
}else if (escenario == "PA"){
    t_Pool_fac_SQLPOOL = t_Pool_fac_SQLPOOL.where(col("num_periodo") === pperiodo && col("id_escenario") === 2).cache
}else if (escenario == "UPA"){
    t_Pool_fac_SQLPOOL = t_Pool_fac_SQLPOOL.where(col("num_periodo") === pperiodo && col("id_escenario") === 3).cache
}else{
    t_Pool_fac_SQLPOOL = t_Pool_fac_SQLPOOL.where(col("num_periodo") === pperiodo).cache
}


# In[53]:


var t_Pool_kpi = readFromSQLPool("sch_anl","cli0010_tb_dim_m_kpi_mmex_h2", token).cache

var t_Pool_fac = t_Pool_fac_SQLPOOL.dropDuplicates("num_periodo", "id_escenario"
    , "val_unidadnegocio", "val_origen", "val_producto", "val_pais", "val_sociedad", "val_unidadmedida", "val_canal", 
    "id_kpi", "val_kpi", "cod_kpi", "num_periodo_mensacum", "val_flag_calculado")


# In[54]:


var df_resultados_generados2 = t_Pool_fac.join(t_Pool_kpi, t_Pool_fac("id_kpi") === t_Pool_kpi("id_kpi"))
    .select(t_Pool_fac("num_periodo"), 
        t_Pool_fac("id_escenario"), 
        t_Pool_fac("id_kpi"), 
        t_Pool_fac("val_pais"),
        t_Pool_fac("val_sociedad"),
        t_Pool_fac("val_canal"),
        t_Pool_fac("val_producto"),
        t_Pool_fac("val_origen"),
        t_Pool_kpi("des_kpi"), 
        t_Pool_fac("val_kpi")).withColumn("DES2", regexp_replace($"des_kpi", "Mov. Mex.", "")).drop("des_kpi").cache

//display(df_resultados_generados2.where( col("des_kpi").like("%Mon%")))


# In[55]:


var df_resultados_generados = df_resultados_generados2.drop(col("val_pais")).drop(col("val_sociedad")).drop(col("val_canal")).drop(col("val_producto")).drop(col("val_origen")).dropDuplicates


# In[56]:


val pivotea = df_resultados_generados2.groupBy("num_periodo", "id_escenario").pivot("DES2").sum("val_kpi").cache


# In[57]:


val columnsToEnsure = List("Central Movilidad Mexico_Amortizaciones"
,"Central Movilidad Mexico_Costes fijos de estructura##Comunicación y relaciones públicas"
,"Central Movilidad Mexico_Costes fijos de estructura##Otros servicios"
,"Central Movilidad Mexico_Costes fijos de estructura##Personal"
,"Central Movilidad Mexico_Costes fijos de estructura##Primas de seguros"
,"Central Movilidad Mexico_Costes fijos de estructura##Publicidad y relaciones públicas"
,"Central Movilidad Mexico_Costes fijos de estructura##Servicios bancarios y similares"
,"Central Movilidad Mexico_Costes fijos de estructura##Servicios profesionales"
,"Central Movilidad Mexico_Costes fijos de estructura##Suministros"
,"Central Movilidad Mexico_Margen de contribución"
,"Central Movilidad Mexico_Otros gastos"
,"Central Movilidad Mexico_Otros resultados"
,"Central Movilidad Mexico_Otros servicios DG"
,"Central Movilidad Mexico_Personal##Otros costes de personal"
,"Central Movilidad Mexico_Personal##Retribución"
,"Central Movilidad Mexico_Provisiones recurrentes"
,"Central Movilidad Mexico_Servicios corporativos"
,"Central Movilidad Mexico_Servicios externos##Arrendamientos y cánones"
,"Central Movilidad Mexico_Servicios externos##Mantenimiento y reparaciones"
,"Central Movilidad Mexico_Servicios externos##Otros servicios externos"
,"Central Movilidad Mexico_Servicios externos##Publicidad y relaciones públicas"
,"Central Movilidad Mexico_Servicios externos##Seguros"
,"Central Movilidad Mexico_Servicios externos##Servicios profesionales"
,"Central Movilidad Mexico_Servicios externos##Suministros"
,"Central Movilidad Mexico_Servicios externos##Tributos"
,"Central Movilidad Mexico_Servicios externos##Viajes"
,"Central Movilidad Mexico_Servicios transversales DG"
,"Central Movilidad Mexico_Tipo de Cambio MXN/EUR acumulado"
,"Central Movilidad Mexico_Tipo de Cambio MXN/EUR acumulado"
,"Central Movilidad Mexico_Tipo de Cambio MXN/EUR mensual"
," EES Mayorista_Amortizaciones"
," EES Mayorista_Corporativos"
," EES Mayorista_Costes fijos EES##Actividades promocionales"
," EES Mayorista_Costes fijos EES##Mantenimiento y reparaciones"
," EES Mayorista_Costes fijos EES##Otros costes fijos EES"
," EES Mayorista_Costes fijos EES##Sistemas de controles volumétricos"
," EES Mayorista_Costes fijos EES##Técnicos Neotech"
," EES Mayorista_EBIT CCS recurrente Filiales##JV Mar Cortés (50%)"
," EES Mayorista_Margen de Contribución EES##Efecto Reservas Estratégicas"
," EES Mayorista_Margen de Contribución EES##Extramargen Monterra EES"
," EES Mayorista_Margen de Contribución EES##Extramargen Olstor EES"
," EES Mayorista_Margen de Contribución EES##MC sin Reservas Estratégicas"
," EES Mayorista_Otros gastos"
," EES Mayorista_Otros resultados"
," EES Mayorista_Otros servicios DG"
," EES Mayorista_Personal##Otros costes de personal"
," EES Mayorista_Personal##Retribución"
," EES Mayorista_Provisiones recurrentes"
," EES Mayorista_Servicios corporativos"
," EES Mayorista_Servicios externos##Arrendamientos y cánones"
," EES Mayorista_Servicios externos##Mantenimiento y reparaciones"
," EES Mayorista_Servicios externos##Otros servicios externos"
," EES Mayorista_Servicios externos##Publicidad y relaciones públicas"
," EES Mayorista_Servicios externos##Seguros"
," EES Mayorista_Servicios externos##Servicios profesionales"
," EES Mayorista_Servicios externos##Suministros"
," EES Mayorista_Servicios externos##Tributos"
," EES Mayorista_Servicios externos##Viajes"
," EES Mayorista_Servicios transversales DG"
," EES Mayorista_Tipo de Cambio MXN/EUR acumulado"
," EES Mayorista_Tipo de Cambio MXN/EUR mensual"
," EES Mayorista_VentasGasóleo A regular: Diesel"
," EES Mayorista_VentasPremium: 92"
," EES Mayorista_VentasRegular: 87"
," EES Minorista_Amortizaciones"
," EES Minorista_Corporativos"
," EES Minorista_Costes fijos EES##Actividades promocionales"
," EES Minorista_Costes fijos EES##Mantenimiento y reparaciones"
," EES Minorista_Costes fijos EES##Otros costes fijos EES"
," EES Minorista_Costes fijos EES##Sistemas de controles volumétricos"
," EES Minorista_Costes fijos EES##Técnicos Neotech"
," EES Minorista_Otros gastos"
," EES Minorista_Otros márgenes de contribución"
," EES Minorista_Otros márgenes de contribución##Gestión Directa (Margen de contribución EES Minorista)"
," EES Minorista_Otros resultados"
," EES Minorista_Otros servicios DG"
," EES Minorista_Personal##Otros costes de personal"
," EES Minorista_Personal##Retribución"
," EES Minorista_Provisiones recurrentes"
," EES Minorista_Servicios corporativos"
," EES Minorista_Servicios externos##Arrendamientos y cánones"
," EES Minorista_Servicios externos##Mantenimiento y reparaciones"
," EES Minorista_Servicios externos##Otros servicios externos"
," EES Minorista_Servicios externos##Publicidad y relaciones públicas"
," EES Minorista_Servicios externos##Seguros"
," EES Minorista_Servicios externos##Servicios profesionales"
," EES Minorista_Servicios externos##Suministros"
," EES Minorista_Servicios externos##Tributos"
," EES Minorista_Servicios externos##Viajes"
," EES Minorista_Servicios transversales DG"
," EES Minorista_VentasGasóleo A regular: Diesel"
," EES Minorista_VentasPremium: 92"
," EES Minorista_VentasRegular: 87"
," EES_Amortizaciones"
," EES_Corporativos"
," EES_Coste Logística y distribución##Transporte"
," EES_Coste Producto##Aditivos"
," EES_Coste Producto##Descuentos en compras"
," EES_Coste Producto##Materia prima"
," EES_Coste Producto##Mermas"
," EES_Coste de canal"
," EES_Costes fijos EES##Actividades promocionales"
," EES_Costes fijos EES##Mantenimiento y reparaciones"
," EES_Costes fijos EES##Otros costes fijos EES"
," EES_Costes fijos EES##Sistemas de controles volumétricos"
," EES_Costes fijos EES##Técnicos Neotech"
," EES_EBIT CCS recurrente Filiales##JV Mar Cortés (50%)"
," EES_Extramargen Monterra"
," EES_Extramargen Olstor"
," EES_IEPS"
," EES_Ingresos brutos"
," EES_Margen bruto sin descuento/costes variables: Ingresos – coste de Materia prima"
," EES_Margen de Contribución EES##Efecto Reservas Estratégicas"
," EES_Margen de Contribución EES##Extramargen Monterra EES"
," EES_Margen de Contribución EES##Extramargen Olstor EES"
," EES_Margen de Contribución EES##MC sin Reservas Estratégicas"
," EES_Otros gastos"
," EES_Otros márgenes de contribución##Gestión Directa (Margen de contribución EES Minorista)"
," EES_Otros resultados"
," EES_Otros servicios DG"
," EES_Personal##Otros costes de personal"
," EES_Personal##Retribución"
," EES_Provisiones recurrentes"
," EES_Servicios corporativos"
," EES_Servicios externos##Arrendamientos y cánones"
," EES_Servicios externos##Mantenimiento y reparaciones"
," EES_Servicios externos##Otros servicios externos"
," EES_Servicios externos##Publicidad y relaciones públicas"
," EES_Servicios externos##Seguros"
," EES_Servicios externos##Servicios profesionales"
," EES_Servicios externos##Suministros"
," EES_Servicios externos##Tributos"
," EES_Servicios externos##Viajes"
," EES_Servicios transversales DG"
," EES_Tipo de Cambio MXN/EUR acumulado"
," EES_Tipo de Cambio MXN/EUR mensual"
," EES_VentasGasóleo A regular: Diesel"
," EES_VentasPremium: 92"
," EES_VentasRegular: 87"
," EES_Ventas##MonterraGasóleo A regular: Diesel"
," EES_Ventas##MonterraPremium: 92"
," EES_Ventas##MonterraRegular: 87"
," EES_Ventas##OlstorGasóleo A regular: Diesel"
," EES_Ventas##OlstorPremium: 92"
," EES_Ventas##OlstorRegular: 87"
," VVDD_Amortizaciones"
," VVDD_Coste Producto##Materia prima"
," VVDD_Coste Producto##Mermas"
," VVDD_Otros costes variables##Almacen"
," VVDD_Otros costes variables##Efecto almacen"
," VVDD_Ingresos netos"
," VVDD_Otros gastos"
," VVDD_Otros márgenes de contribución##Almacén"
," VVDD_Otros márgenes de contribución##Interterminales"
," VVDD_Otros márgenes de contribución##Ventas Directas"
," VVDD_Otros resultados"
," VVDD_Otros servicios DG"
," VVDD_Personal##Otros costes de personal"
," VVDD_Personal##Retribución"
," VVDD_Provisiones recurrentes"
," VVDD_Servicios corporativos"
," VVDD_Servicios externos##Arrendamientos y cánones"
," VVDD_Servicios externos##Mantenimiento y reparaciones"
," VVDD_Servicios externos##Otros servicios externos"
," VVDD_Servicios externos##Publicidad y relaciones públicas"
," VVDD_Servicios externos##Seguros"
," VVDD_Servicios externos##Servicios profesionales"
," VVDD_Servicios externos##Suministros"
," VVDD_Servicios externos##Tributos"
," VVDD_Servicios externos##Viajes"
," VVDD_Servicios transversales DG"
," VVDD_Tipo de Cambio MXN/EUR acumulado"
," VVDD_Tipo de Cambio MXN/EUR mensual"
," VVDD_VentasGasóleo A regular: Diesel"
," VVDD_VentasPremium: 92"
,"imp_ctral_cf_est_2_viaj"
," VVDD_VentasRegular: 87")
val df_177 = look_columns_else_null(pivotea, columnsToEnsure).cache


# In[58]:


var n = df_177.select(
    concat(col("num_periodo"), lit("01")).cast(IntegerType).as("cod_periodo"),
    col("id_escenario").cast(IntegerType),
    col(" EES_Amortizaciones").cast(DecimalType(24,10)).as("imp_ees_amort"),
    col(" EES_Corporativos").cast(DecimalType(24,10)).as("imp_ees_corp"),
    col(" EES_Coste Logística y distribución##Transporte").cast(DecimalType(24,10)).as("imp_ees_cost_log_dist_2_transp"),
    col(" EES_Coste Producto##Aditivos").cast(DecimalType(24,10)).as("imp_ees_coste_prod_2_aditivos"),
    col(" EES_Coste Producto##Descuentos en compras").cast(DecimalType(24,10)).as("imp_ees_coste_prod_2_desc_comp"),
    col(" EES_Coste Producto##Materia prima").cast(DecimalType(24,10)).as("imp_ees_coste_prod_2_mat_prima"),
    col(" EES_Coste Producto##Mermas").cast(DecimalType(24,10)).as("imp_ees_coste_prod_2_mermas"),
    col(" EES_Coste de canal").cast(DecimalType(24,10)).as("imp_ees_coste_canal"),
    col(" EES_Costes fijos EES##Actividades promocionales").cast(DecimalType(24,10)).as("imp_ees_cf_ees_2_act_promo"),
    col(" EES_Costes fijos EES##Mantenimiento y reparaciones").cast(DecimalType(24,10)).as("imp_ees_cf_ees_2_mant_rep"),
    col(" EES_Costes fijos EES##Otros costes fijos EES").cast(DecimalType(24,10)).as("imp_ees_cf_ees_2_otros_cf_ees"),
    col(" EES_Costes fijos EES##Sistemas de controles volumétricos").cast(DecimalType(24,10)).as("imp_ees_cf_ees_2_sis_ctrl_volu"),
    col(" EES_Costes fijos EES##Técnicos Neotech").cast(DecimalType(24,10)).as("imp_ees_cf_ees_2_tec_neotech"),
    col(" EES_EBIT CCS recurrente Filiales##JV Mar Cortés (50%)").cast(DecimalType(24,10)).as("imp_ees_EBIT_CCS_JV_mar_cortes"),
    col(" EES_Extramargen Monterra").cast(DecimalType(24,10)).as("imp_ees_extram_monterra"),
    col(" EES_Extramargen Olstor").cast(DecimalType(24,10)).as("imp_ees_extram_olstor"),
    col(" EES_IEPS").cast(DecimalType(24,10)).as("imp_ees_IEPS"),
    col(" EES_Ingresos brutos").cast(DecimalType(24,10)).as("imp_ees_ing_brut"),
    col(" EES_Margen bruto sin descuento/costes variables: Ingresos – coste de Materia prima").cast(DecimalType(24,10)).as("imp_ees_mb_sin_des_ing_c_prima"),
    col(" EES_Margen de Contribución EES##Efecto Reservas Estratégicas").cast(DecimalType(24,10)).as("imp_ees_mc_ees_2_ef_res_estrat"),
    col(" EES_Margen de Contribución EES##Extramargen Monterra EES").cast(DecimalType(24,10)).as("imp_ees_mc_ees_2_extram_mont"),
    col(" EES_Margen de Contribución EES##Extramargen Olstor EES").cast(DecimalType(24,10)).as("imp_ees_mc_ees_2_extram_olstor"),
    col(" EES_Margen de Contribución EES##MC sin Reservas Estratégicas").cast(DecimalType(24,10)).as("imp_ees_mc_ees_2_mc_sn_res_est"),
    col(" EES_Otros gastos").cast(DecimalType(24,10)).as("imp_ees_otros_gastos"),
    col(" EES_Otros márgenes de contribución##Gestión Directa (Margen de contribución EES Minorista)").cast(DecimalType(24,10)).as("imp_ees_otros_mc_2_gdirec_mc"),
    col(" EES_Otros resultados").cast(DecimalType(24,10)).as("imp_ees_otros_result"),
    col(" EES_Otros servicios DG").cast(DecimalType(24,10)).as("imp_ees_otros_serv_DG"),
    col(" EES_Personal##Otros costes de personal").cast(DecimalType(24,10)).as("imp_ees_pers_2_otros_cost_pers"),
    col(" EES_Personal##Retribución").cast(DecimalType(24,10)).as("imp_ees_pers_2_retrib"),
    col(" EES_Provisiones recurrentes").cast(DecimalType(24,10)).as("imp_ees_provis_recur"),
    col(" EES_Servicios corporativos").cast(DecimalType(24,10)).as("imp_ees_serv_corp"),
    col(" EES_Servicios externos##Arrendamientos y cánones").cast(DecimalType(24,10)).as("imp_ees_serv_ext_2_arrend_can"),
    col(" EES_Servicios externos##Mantenimiento y reparaciones").cast(DecimalType(24,10)).as("imp_ees_serv_ext_2_mant_rep"),
    col(" EES_Servicios externos##Otros servicios externos").cast(DecimalType(24,10)).as("imp_ees_sv_ext_2_otros_sv_ext"),
    col(" EES_Servicios externos##Publicidad y relaciones públicas").cast(DecimalType(24,10)).as("imp_ees_serv_ext_2_pub_rrpp"),
    col(" EES_Servicios externos##Seguros").cast(DecimalType(24,10)).as("imp_ees_serv_ext_2_seg"),
    col(" EES_Servicios externos##Servicios profesionales").cast(DecimalType(24,10)).as("imp_ees_serv_ext_2_serv_prof"),
    col(" EES_Servicios externos##Suministros").cast(DecimalType(24,10)).as("imp_ees_serv_ext_2_sumin"),
    col(" EES_Servicios externos##Tributos").cast(DecimalType(24,10)).as("imp_ees_serv_ext_2_trib"),
    col(" EES_Servicios externos##Viajes").cast(DecimalType(24,10)).as("imp_ees_serv_ext_2_viajes"),
    col(" EES_Servicios transversales DG").cast(DecimalType(24,10)).as("imp_ees_serv_transv_DG"),
    col(" EES_Tipo de Cambio MXN/EUR acumulado").cast(DecimalType(24,10)).as("imp_ees_tipo_cambio_MXN_EUR_ac"),
    col(" EES_Tipo de Cambio MXN/EUR mensual").cast(DecimalType(24,10)).as("imp_ees_tipo_cambio_MXN_EUR_m"),
    col(" EES_VentasGasóleo A regular: Diesel").cast(DecimalType(24,10)).as("imp_ees_ventas_go_a_reg_diesel"),
    col(" EES_VentasPremium: 92").cast(DecimalType(24,10)).as("imp_ees_ventas_prem_92"),
    col(" EES_VentasRegular: 87").cast(DecimalType(24,10)).as("imp_ees_ventas_reg_87"),
    col(" EES_Ventas##MonterraGasóleo A regular: Diesel").cast(DecimalType(24,10)).as("imp_ees_ventas_mont_reg_diesel"), // MOD
    col(" EES_Ventas##MonterraPremium: 92").cast(DecimalType(24,10)).as("imp_ees_ventas_mont_premium"), // MOD
    col(" EES_Ventas##MonterraRegular: 87").cast(DecimalType(24,10)).as("imp_ees_ventas_mont_regular"),// MOD
    col(" EES_Ventas##OlstorGasóleo A regular: Diesel").cast(DecimalType(24,10)).as("imp_ees_ventas_olstor_rg_diesl"),// MOD
    col(" EES_Ventas##OlstorPremium: 92").cast(DecimalType(24,10)).as("imp_ees_ventas_olstor_premium"),// MOD
    col(" EES_Ventas##OlstorRegular: 87").cast(DecimalType(24,10)).as("imp_ees_ventas_olstor_regular"),// MOD
    col(" VVDD_Amortizaciones").cast(DecimalType(24,10)).as("imp_vvdd_amort"),
    col(" VVDD_Coste Producto##Materia prima").cast(DecimalType(24,10)).as("imp_vvdd_coste_prod_2_mat_prim"),
    col(" VVDD_Coste Producto##Mermas").cast(DecimalType(24,10)).as("imp_vvdd_coste_prod_2_mermas"),
    col(" VVDD_Ingresos netos").cast(DecimalType(24,10)).as("imp_vvdd_ing_net"),
    col(" VVDD_Otros gastos").cast(DecimalType(24,10)).as("imp_vvdd_otros_gastos"),
    col(" VVDD_Otros márgenes de contribución##Almacén").cast(DecimalType(24,10)).as("imp_vvdd_otros_mc_2_almacen"),
    col(" VVDD_Otros márgenes de contribución##Interterminales").cast(DecimalType(24,10)).as("imp_vvdd_otros_mc_2_interterm"),
    col(" VVDD_Otros márgenes de contribución##Ventas Directas").cast(DecimalType(24,10)).as("imp_vvdd_otros_mc_2_vta_direc"),
    col(" VVDD_Otros resultados").cast(DecimalType(24,10)).as("imp_vvdd_otros_result"),
    col(" VVDD_Otros servicios DG").cast(DecimalType(24,10)).as("imp_vvdd_otros_serv_DG"),
    col(" VVDD_Personal##Otros costes de personal").cast(DecimalType(24,10)).as("imp_vvdd_pers_2_otro_cost_pers"),
    col(" VVDD_Personal##Retribución").cast(DecimalType(24,10)).as("imp_vvdd_pers_2_retrib"),
    col(" VVDD_Provisiones recurrentes").cast(DecimalType(24,10)).as("imp_vvdd_provis_recur"),
    col(" VVDD_Servicios corporativos").cast(DecimalType(24,10)).as("imp_vvdd_serv_corp"),
    col(" VVDD_Servicios externos##Arrendamientos y cánones").cast(DecimalType(24,10)).as("imp_vvdd_serv_ext_2_arrend_can"),
    col(" VVDD_Servicios externos##Mantenimiento y reparaciones").cast(DecimalType(24,10)).as("imp_vvdd_serv_ext_2_mant_rep"),
    col(" VVDD_Servicios externos##Otros servicios externos").cast(DecimalType(24,10)).as("imp_vvdd_sv_ext_2_otros_sv_ext"),
    col(" VVDD_Servicios externos##Publicidad y relaciones públicas").cast(DecimalType(24,10)).as("imp_vvdd_serv_ext_2_pub_rrpp"),
    col(" VVDD_Servicios externos##Seguros").cast(DecimalType(24,10)).as("imp_vvdd_serv_ext_2_seg"),
    col(" VVDD_Servicios externos##Servicios profesionales").cast(DecimalType(24,10)).as("imp_vvdd_serv_ext_2_serv_prof"),
    col(" VVDD_Servicios externos##Suministros").cast(DecimalType(24,10)).as("imp_vvdd_serv_ext_2_sumin"),
    col(" VVDD_Servicios externos##Tributos").cast(DecimalType(24,10)).as("imp_vvdd_serv_ext_2_trib"),
    col(" VVDD_Servicios externos##Viajes").cast(DecimalType(24,10)).as("imp_vvdd_serv_ext_2_viajes"),
    col(" VVDD_Servicios transversales DG").cast(DecimalType(24,10)).as("imp_vvdd_serv_transv_DG"),
    col(" VVDD_Tipo de Cambio MXN/EUR acumulado").cast(DecimalType(24,10)).as("imp_vvdd_Tcambio_MXN_EUR_ac"),
    col(" VVDD_Tipo de Cambio MXN/EUR mensual").cast(DecimalType(24,10)).as("imp_vvdd_tipo_cambio_MXN_EUR_m"),
    col(" VVDD_VentasGasóleo A regular: Diesel").cast(DecimalType(24,10)).as("imp_vvdd_ventas_go_a_reg_diesl"),
    col(" VVDD_VentasPremium: 92").cast(DecimalType(24,10)).as("imp_vvdd_ventas_prem_92"),
    col(" VVDD_VentasRegular: 87").cast(DecimalType(24,10)).as("imp_vvdd_ventas_reg_87"),
    col("Central Movilidad Mexico_Tipo de Cambio MXN/EUR acumulado").cast(DecimalType(24,10)).as("imp_ctral_Tcambio_MXN_EUR_ac"),
    col("Central Movilidad Mexico_Tipo de Cambio MXN/EUR mensual").cast(DecimalType(24,10)).as("imp_ctral_Tcambio_MXN_EUR_m"),
    col(" VVDD_Otros costes variables##Almacen").cast(DecimalType(24,10)).as("imp_vvdd_otros_costes_almacen"),
    col(" VVDD_Otros costes variables##Efecto almacen").cast(DecimalType(24,10)).as("imp_vvdd_otros_costes_efecto_almacen")
    ).cache


# ____________________________________________________________________________________________________________________________________________________

# In[59]:


case class r(des: String, newnomenc: String)

val dicc = Seq(new r(" EES_Amortizaciones", "imp_ees_amort"),
    new r(" EES_Corporativos", "imp_ees_corp"),
    new r(" EES_Coste Logística y distribución##Transporte", "imp_ees_cost_log_dist_2_transp"),
    new r(" EES_Coste Producto##Aditivos", "imp_ees_coste_prod_2_aditivos "),
    new r(" EES_Coste Producto##Descuentos en compras", "imp_ees_coste_prod_2_desc_comp"),
    new r(" EES_Coste Producto##Materia prima", "imp_ees_coste_prod_2_mat_prima"),
    new r(" EES_Coste Producto##Mermas", "imp_ees_coste_prod_2_mermas"),
    new r(" EES_Coste de canal", "imp_ees_coste_canal"),
    new r(" EES_Costes fijos EES##Actividades promocionales", "imp_ees_cf_ees_2_act_promo"),
    new r(" EES_Costes fijos EES##Mantenimiento y reparaciones", "imp_ees_cf_ees_2_mant_rep"),
    new r(" EES_Costes fijos EES##Otros costes fijos EES", "imp_ees_cf_ees_2_otros_cf_ees"),
    new r(" EES_Costes fijos EES##Sistemas de controles volumétricos", "imp_ees_cf_ees_2_sis_ctrl_volu"),
    new r(" EES_Costes fijos EES##Técnicos Neotech", "imp_ees_cf_ees_2_tec_neotech"),
    new r(" EES_EBIT CCS recurrente Filiales##JV Mar Cortés (50%)", "imp_ees_EBIT_CCS_JV_mar_cortes"),
    new r(" EES_Extramargen Monterra", "imp_ees_extram_monterra"),
    new r(" EES_Extramargen Olstor", "imp_ees_extram_olstor"),
    new r(" EES_IEPS", "imp_ees_IEPS"),
    new r(" EES_Ingresos brutos", "imp_ees_ing_brut"),
    new r(" EES_Margen bruto sin descuento/costes variables: Ingresos – coste de Materia prima", "imp_ees_mb_sin_des_ing_c_prima"),
    new r(" EES_Margen de Contribución EES##Efecto Reservas Estratégicas", "imp_ees_mc_ees_2_ef_res_estrat"),
    new r(" EES_Margen de Contribución EES##Extramargen Monterra EES", "imp_ees_mc_ees_2_extram_mont"),
    new r(" EES_Margen de Contribución EES##Extramargen Olstor EES", "imp_ees_mc_ees_2_extram_olstor"),
    new r(" EES_Margen de Contribución EES##MC sin Reservas Estratégicas", "imp_ees_mc_ees_2_mc_sn_res_est "),
    new r(" EES_Otros gastos", "imp_ees_otros_gastos"),
    new r(" EES_Otros márgenes de contribución##Gestión Directa (Margen de contribución EES Minorista)", "imp_ees_otros_mc_2_gdirec_mc"),
    new r(" EES_Otros resultados", "imp_ees_otros_result"),
    new r(" EES_Otros servicios DG", "imp_ees_otros_serv_DG"),
    new r(" EES_Personal##Otros costes de personal", "imp_ees_pers_2_otros_cost_pers"),
    new r(" EES_Personal##Retribución", "imp_ees_pers_2_retrib"),
    new r(" EES_Provisiones recurrentes", "imp_ees_provis_recur"),
    new r(" EES_Servicios corporativos", "imp_ees_serv_corp"),
    new r(" EES_Servicios externos##Arrendamientos y cánones", "imp_ees_serv_ext_2_arrend_can"),
    new r(" EES_Servicios externos##Mantenimiento y reparaciones", "imp_ees_serv_ext_2_mant_rep"),
    new r(" EES_Servicios externos##Otros servicios externos", "imp_ees_sv_ext_2_otros_sv_ext "),
    new r(" EES_Servicios externos##Publicidad y relaciones públicas", "imp_ees_serv_ext_2_pub_rrpp"),
    new r(" EES_Servicios externos##Seguros", "imp_ees_serv_ext_2_seg"),
    new r(" EES_Servicios externos##Servicios profesionales", "imp_ees_serv_ext_2_serv_prof"),
    new r(" EES_Servicios externos##Suministros", "imp_ees_serv_ext_2_sumin"),
    new r(" EES_Servicios externos##Tributos", "imp_ees_serv_ext_2_trib"),
    new r(" EES_Servicios externos##Viajes", "imp_ees_serv_ext_2_viajes"),
    new r(" EES_Servicios transversales DG", "imp_ees_serv_transv_DG"),
    new r(" EES_VentasGasóleo A regular: Diesel", "imp_ees_ventas_go_a_reg_diesel"),
    new r(" EES_VentasPremium: 92", "imp_ees_ventas_prem_92"),
    new r(" EES_VentasRegular: 87", "imp_ees_ventas_reg_87"),
    new r(" EES_Ventas##MonterraGasóleo A regular: Diesel", "imp_ees_ventas_mont_reg_diesel"),
    new r(" EES_Ventas##MonterraPremium: 92", "imp_ees_ventas_mont_premium"),
    new r(" EES_Ventas##MonterraRegular: 87", "imp_ees_ventas_mont_regular"),
    new r(" EES_Ventas##OlstorGasóleo A regular: Diesel", "imp_ees_ventas_olstor_rg_diesl"),
    new r(" EES_Ventas##OlstorPremium: 92", "imp_ees_ventas_olstor_premium"),
    new r(" EES_Ventas##OlstorRegular: 87", "imp_ees_ventas_olstor_regular"),
    new r(" VVDD_Amortizaciones", "imp_vvdd_amort"),
    new r(" VVDD_Coste Producto##Materia prima", "imp_vvdd_coste_prod_2_mat_prim"),
    new r(" VVDD_Coste Producto##Mermas", "imp_vvdd_coste_prod_2_mermas"),
    new r(" VVDD_Ingresos netos", "imp_vvdd_ing_net"),
    new r(" VVDD_Otros gastos", "imp_vvdd_otros_gastos"),
    new r(" VVDD_Otros márgenes de contribución##Almacén", "imp_vvdd_otros_mc_2_almacen"),
    new r(" VVDD_Otros márgenes de contribución##Interterminales", "imp_vvdd_otros_mc_2_interterm"),
    new r(" VVDD_Otros márgenes de contribución##Ventas Directas", "imp_vvdd_otros_mc_2_vta_direc"),
    new r(" VVDD_Otros resultados", "imp_vvdd_otros_result"),
    new r(" VVDD_Otros servicios DG", "imp_vvdd_otros_serv_DG"),
    new r(" VVDD_Personal##Otros costes de personal", "imp_vvdd_pers_2_otro_cost_pers"),
    new r(" VVDD_Personal##Retribución", "imp_vvdd_pers_2_retrib"),
    new r(" VVDD_Provisiones recurrentes", "imp_vvdd_provis_recur"),
    new r(" VVDD_Servicios corporativos", "imp_vvdd_serv_corp"),
    new r(" VVDD_Servicios externos##Arrendamientos y cánones", "imp_vvdd_serv_ext_2_arrend_can"),
    new r(" VVDD_Servicios externos##Mantenimiento y reparaciones", "imp_vvdd_serv_ext_2_mant_rep"),
    new r(" VVDD_Servicios externos##Otros servicios externos", "imp_vvdd_sv_ext_2_otros_sv_ext"),
    new r(" VVDD_Servicios externos##Publicidad y relaciones públicas", "imp_vvdd_serv_ext_2_pub_rrpp"),
    new r(" VVDD_Servicios externos##Seguros", "imp_vvdd_serv_ext_2_seg"),
    new r(" VVDD_Servicios externos##Servicios profesionales", "imp_vvdd_serv_ext_2_serv_prof "),
    new r(" VVDD_Servicios externos##Suministros", "imp_vvdd_serv_ext_2_sumin"),
    new r(" VVDD_Servicios externos##Tributos", "imp_vvdd_serv_ext_2_trib"),
    new r(" VVDD_Servicios externos##Viajes", "imp_vvdd_serv_ext_2_viajes"),
    new r(" VVDD_Servicios transversales DG", "imp_vvdd_serv_transv_DG"),
    new r(" VVDD_Tipo de Cambio MXN/EUR acumulado", "imp_vvdd_Tcambio_MXN_EUR_ac"),
    new r(" VVDD_Tipo de Cambio MXN/EUR mensual", "imp_vvdd_tipo_cambio_MXN_EUR_m"),
    new r(" VVDD_VentasGasóleo A regular: Diesel", "imp_vvdd_ventas_go_a_reg_diesl "),
    new r(" VVDD_VentasPremium: 92", "imp_vvdd_ventas_prem_92"),
    new r(" VVDD_VentasRegular: 87", "imp_vvdd_ventas_reg_87"),
    new r("Central Movilidad Mexico_Tipo de Cambio MXN/EUR acumulado", "imp_ctral_Tcambio_MXN_EUR_ac"),
    new r("Central Movilidad Mexico_Tipo de Cambio MXN/EUR mensual", "imp_ctral_Tcambio_MXN_EUR_m"),
    new r(" VVDD_Otros costes variables##Almacen", "imp_vvdd_otros_costes_almacen"),
    new r(" VVDD_Otros costes variables##Efecto almacen", "imp_vvdd_otros_costes_efecto_almacen")
    ).toDF.cache


# In[60]:


var dicc2 = dicc.join(df_resultados_generados2, df_resultados_generados2("DES2") === dicc("des"), "left").dropDuplicates.cache


# In[61]:


var dicc3 = dicc2.drop(col("DES2")).drop(col("des")).drop(col("id_kpi"))


# In[62]:


var dicc4 = dicc3.select(
    col("newnomenc").cast(VarcharType(100)),
    col("num_periodo").cast(IntegerType),
    when(col("id_escenario") === 2, 2).when(col("id_escenario") === 1, 1).when(col("id_escenario") === 4, 4).when(col("id_escenario") === 3, 3).cast(IntegerType).as("id_escenarioDic"),
    col("val_pais").cast(VarcharType(100)),
    col("val_canal").cast(VarcharType(100)),
    col("val_producto").cast(VarcharType(100)),
    col("val_origen").cast(VarcharType(100)),
    col("val_kpi").cast(DecimalType(24,10))
).where(col("num_periodo") === pperiodo).cache


# **MOV. MEX. EES MEXICO**

# In[63]:


var imp_ees_cv_unit = n.withColumn("imp_ees_result_otras_soc", when(isnull(col("imp_ees_EBIT_CCS_JV_mar_cortes")), 0).otherwise(col("imp_ees_EBIT_CCS_JV_mar_cortes")))
.withColumn("imp_ees_ventas_monterra", when(isnull(col("imp_ees_ventas_mont_reg_diesel")), 0).otherwise(col("imp_ees_ventas_mont_reg_diesel"))
    + when(isnull(col("imp_ees_ventas_mont_premium")), 0).otherwise(col("imp_ees_ventas_mont_premium"))
    + when(isnull(col("imp_ees_ventas_mont_regular")), 0).otherwise(col("imp_ees_ventas_mont_regular")))
.withColumn("imp_ees_ventas_olstor", when(isnull(col("imp_ees_ventas_olstor_rg_diesl")), 0).otherwise(col("imp_ees_ventas_olstor_rg_diesl"))
    + when(isnull(col("imp_ees_ventas_olstor_premium")), 0).otherwise(col("imp_ees_ventas_olstor_premium"))
    + when(isnull(col("imp_ees_ventas_olstor_regular")), 0).otherwise(col("imp_ees_ventas_olstor_regular")))
.withColumn("imp_ees_ventas", when(isnull(col("imp_ees_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_ees_ventas_go_a_reg_diesel"))
    + when(isnull(col("imp_ees_ventas_prem_92")), 0).otherwise(col("imp_ees_ventas_prem_92"))
    + when(isnull(col("imp_ees_ventas_reg_87")), 0).otherwise(col("imp_ees_ventas_reg_87")))
.withColumn("imp_ees_mc_unit_ees_2_s_res_et", when(isnull(col("imp_ees_mc_ees_2_mc_sn_res_est")), 0).otherwise(col("imp_ees_mc_ees_2_mc_sn_res_est"))
    / when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas")))
.withColumn("imp_ees_mc_unit_ees_2_efres_et", when(isnull(col("imp_ees_mc_ees_2_ef_res_estrat")), 0).otherwise(col("imp_ees_mc_ees_2_ef_res_estrat"))
    / when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas")))
.withColumn("imp_ees_mc_ees", when(isnull(col("imp_ees_mc_ees_2_mc_sn_res_est")), 0).otherwise(col("imp_ees_mc_ees_2_mc_sn_res_est"))
    + when(isnull(col("imp_ees_mc_ees_2_ef_res_estrat")), 0).otherwise(col("imp_ees_mc_ees_2_ef_res_estrat"))
    + when(isnull(col("imp_ees_mc_ees_2_extram_olstor")), 0).otherwise(col("imp_ees_mc_ees_2_extram_olstor"))
    + when(isnull(col("imp_ees_mc_ees_2_extram_mont")), 0).otherwise(col("imp_ees_mc_ees_2_extram_mont")))
.withColumn("imp_ees_mc_unit_ees", when(isnull(col("imp_ees_mc_ees")), 0).otherwise(col("imp_ees_mc_ees"))
    / when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas")))
.withColumn("imp_ees_mc_unit_ees_2_extr_ols", when(isnull(col("imp_ees_mc_ees_2_extram_olstor")), 0).otherwise(col("imp_ees_mc_ees_2_extram_olstor"))
    / when(isnull(col("imp_ees_ventas_olstor")), 0).otherwise(col("imp_ees_ventas_olstor")))
.withColumn("imp_ees_mc_unit_ees_2_extr_mon", when(isnull(col("imp_ees_mc_ees_2_extram_mont")), 0).otherwise(col("imp_ees_mc_ees_2_extram_mont"))
    / when(isnull(col("imp_ees_ventas_monterra")), 0).otherwise(col("imp_ees_ventas_monterra")))
.withColumn("imp_ees_pers", when(isnull(col("imp_ees_pers_2_retrib")), 0).otherwise(col("imp_ees_pers_2_retrib"))
    + when(isnull(col("imp_ees_pers_2_otros_cost_pers")), 0).otherwise(col("imp_ees_pers_2_otros_cost_pers")))
.withColumn("imp_ees_otros_mc", when(isnull(col("imp_ees_otros_mc_2_gdirec_mc")), 0).otherwise(col("imp_ees_otros_mc_2_gdirec_mc")))
.withColumn("imp_ees_serv_ext", when(isnull(col("imp_ees_serv_ext_2_arrend_can")), 0).otherwise(col("imp_ees_serv_ext_2_arrend_can"))
    + when(isnull(col("imp_ees_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_ees_serv_ext_2_pub_rrpp"))
    + when(isnull(col("imp_ees_serv_ext_2_sumin")), 0).otherwise(col("imp_ees_serv_ext_2_sumin"))
    + when(isnull(col("imp_ees_serv_ext_2_mant_rep")), 0).otherwise(col("imp_ees_serv_ext_2_mant_rep"))
    + when(isnull(col("imp_ees_serv_ext_2_serv_prof")), 0).otherwise(col("imp_ees_serv_ext_2_serv_prof"))
    + when(isnull(col("imp_ees_serv_ext_2_viajes")), 0).otherwise(col("imp_ees_serv_ext_2_viajes"))
    + when(isnull(col("imp_ees_serv_ext_2_seg")), 0).otherwise(col("imp_ees_serv_ext_2_seg"))
    + when(isnull(col("imp_ees_serv_ext_2_trib")), 0).otherwise(col("imp_ees_serv_ext_2_trib"))
    + when(isnull(col("imp_ees_sv_ext_2_otros_sv_ext")), 0).otherwise(col("imp_ees_sv_ext_2_otros_sv_ext")))
.withColumn("imp_ees_tot_cf_concep", when(isnull(col("imp_ees_pers")), 0).otherwise(col("imp_ees_pers"))
    + when(isnull(col("imp_ees_serv_ext")), 0).otherwise(col("imp_ees_serv_ext"))
    + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp"))
    + when(isnull(col("imp_ees_serv_transv_DG")), 0).otherwise(col("imp_ees_serv_transv_DG"))
    + when(isnull(col("imp_ees_otros_serv_DG")), 0).otherwise(col("imp_ees_otros_serv_DG")))
.withColumn("imp_ees_cf_ees", when(isnull(col("imp_ees_cf_ees_2_mant_rep")), 0).otherwise(col("imp_ees_cf_ees_2_mant_rep"))
    + when(isnull(col("imp_ees_cf_ees_2_tec_neotech")), 0).otherwise(col("imp_ees_cf_ees_2_tec_neotech"))
    + when(isnull(col("imp_ees_cf_ees_2_sis_ctrl_volu")), 0).otherwise(col("imp_ees_cf_ees_2_sis_ctrl_volu"))
    + when(isnull(col("imp_ees_cf_ees_2_act_promo")), 0).otherwise(col("imp_ees_cf_ees_2_act_promo"))
    + when(isnull(col("imp_ees_cf_ees_2_otros_cf_ees")), 0).otherwise(col("imp_ees_cf_ees_2_otros_cf_ees")))
.withColumn("imp_ees_tot_cf_analit", when(isnull(col("imp_ees_cf_ees")), 0).otherwise(col("imp_ees_cf_ees"))
    + when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos"))
    + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp")))
.withColumn("imp_ees_cf", when(isnull(col("imp_ees_tot_cf_analit")), 0).otherwise(col("imp_ees_tot_cf_analit")))
.withColumn("imp_ees_result_op_ppal", when(isnull(col("imp_ees_mc_ees")), 0).otherwise(col("imp_ees_mc_ees"))
    + when(isnull(col("imp_ees_otros_mc")), 0).otherwise(col("imp_ees_otros_mc"))
    - when(isnull(col("imp_ees_cf")), 0).otherwise(col("imp_ees_cf"))
    - when(isnull(col("imp_ees_amort")), 0).otherwise(col("imp_ees_amort"))
    - when(isnull(col("imp_ees_provis_recur")), 0).otherwise(col("imp_ees_provis_recur"))
    + when(isnull(col("imp_ees_otros_result")), 0).otherwise(col("imp_ees_otros_result")))
.withColumn("imp_ees_result_op", when(isnull(col("imp_ees_result_op_ppal")), 0).otherwise(col("imp_ees_result_op_ppal"))
    + when(isnull(col("imp_ees_result_otras_soc")), 0).otherwise(col("imp_ees_result_otras_soc")))
.withColumn("imp_ees_ing_net", when(isnull(col("imp_ees_ing_brut")), 0).otherwise(col("imp_ees_ing_brut"))
    - when(isnull(col("imp_ees_IEPS")), 0).otherwise(col("imp_ees_IEPS")))
.withColumn("imp_ees_coste_prod", when(isnull(col("imp_ees_coste_prod_2_mat_prima")), 0).otherwise(col("imp_ees_coste_prod_2_mat_prima"))
    + when(isnull(col("imp_ees_coste_prod_2_aditivos")), 0).otherwise(col("imp_ees_coste_prod_2_aditivos"))
    + when(isnull(col("imp_ees_coste_prod_2_mermas")), 0).otherwise(col("imp_ees_coste_prod_2_mermas"))
    + when(isnull(col("imp_ees_coste_prod_2_desc_comp")), 0).otherwise(col("imp_ees_coste_prod_2_desc_comp")))
.withColumn("imp_ees_mb", when(isnull(col("imp_ees_ing_net")), 0).otherwise(col("imp_ees_ing_net"))
    - when(isnull(col("imp_ees_coste_prod")), 0).otherwise(col("imp_ees_coste_prod")))
.withColumn("imp_ees_coste_logist_dist", when(isnull(col("imp_ees_cost_log_dist_2_transp")), 0).otherwise(col("imp_ees_cost_log_dist_2_transp")))
.withColumn("imp_ees_mc", when(isnull(col("imp_ees_mb")), 0).otherwise(col("imp_ees_mb"))
    - when(isnull(col("imp_ees_coste_logist_dist")), 0).otherwise(col("imp_ees_coste_logist_dist"))
    - when(isnull(col("imp_ees_coste_canal")), 0).otherwise(col("imp_ees_coste_canal"))
   + when(isnull(col("imp_ees_extram_olstor")), 0).otherwise(col("imp_ees_extram_olstor"))
   + when(isnull(col("imp_ees_extram_monterra")), 0).otherwise(col("imp_ees_extram_monterra"))
   + when(isnull(col("imp_ees_otros_mc_2_gdirec_mc")), 0).otherwise(col("imp_ees_otros_mc_2_gdirec_mc"))
   + when(isnull(col("imp_ees_mc_ees_2_ef_res_estrat")), 0).otherwise(col("imp_ees_mc_ees_2_ef_res_estrat")))
.withColumn("imp_ees_cv_tot", when(isnull(col("imp_ees_coste_prod")), 0).otherwise(col("imp_ees_coste_prod"))
    + when(isnull(col("imp_ees_coste_logist_dist")), 0).otherwise(col("imp_ees_coste_logist_dist"))
    + when(isnull(col("imp_ees_coste_canal")), 0).otherwise(col("imp_ees_coste_canal")))
.withColumn("imp_ees_cv_unit", when(isnull(col("imp_ees_cv_tot")), 0).otherwise(col("imp_ees_cv_tot"))
    / when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas"))).cache

// var imp_ees_result_otras_soc = n.withColumn("imp_ees_result_otras_soc", when(isnull(col("imp_ees_EBIT_CCS_JV_mar_cortes")), 0).otherwise(col("imp_ees_EBIT_CCS_JV_mar_cortes")))

// var imp_ees_ventas_monterra = imp_ees_result_otras_soc.withColumn("imp_ees_ventas_monterra", when(isnull(col("imp_ees_ventas_mont_reg_diesel")), 0).otherwise(col("imp_ees_ventas_mont_reg_diesel"))
//     + when(isnull(col("imp_ees_ventas_mont_premium")), 0).otherwise(col("imp_ees_ventas_mont_premium"))
//     + when(isnull(col("imp_ees_ventas_mont_regular")), 0).otherwise(col("imp_ees_ventas_mont_regular")))

// var imp_ees_ventas_olstor = imp_ees_ventas_monterra.withColumn("imp_ees_ventas_olstor", when(isnull(col("imp_ees_ventas_olstor_rg_diesl")), 0).otherwise(col("imp_ees_ventas_olstor_rg_diesl"))
//     + when(isnull(col("imp_ees_ventas_olstor_premium")), 0).otherwise(col("imp_ees_ventas_olstor_premium"))
//     + when(isnull(col("imp_ees_ventas_olstor_regular")), 0).otherwise(col("imp_ees_ventas_olstor_regular")))

// var EESVentas = imp_ees_ventas_olstor.withColumn("imp_ees_ventas", when(isnull(col("imp_ees_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_ees_ventas_go_a_reg_diesel"))
//     + when(isnull(col("imp_ees_ventas_prem_92")), 0).otherwise(col("imp_ees_ventas_prem_92"))
//     + when(isnull(col("imp_ees_ventas_reg_87")), 0).otherwise(col("imp_ees_ventas_reg_87")))

// var imp_ees_mc_unit_ees_2_s_res_et =  EESVentas.withColumn("imp_ees_mc_unit_ees_2_s_res_et", when(isnull(col("imp_ees_mc_ees_2_mc_sn_res_est")), 0).otherwise(col("imp_ees_mc_ees_2_mc_sn_res_est"))
//     / when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas")))


// var imp_ees_mc_unit_ees_2_efres_et =  imp_ees_mc_unit_ees_2_s_res_et.withColumn("imp_ees_mc_unit_ees_2_efres_et", when(isnull(col("imp_ees_mc_ees_2_ef_res_estrat")), 0).otherwise(col("imp_ees_mc_ees_2_ef_res_estrat"))
//     / when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas")))


// var EESMargenContriEES = imp_ees_mc_unit_ees_2_efres_et.withColumn("imp_ees_mc_ees", when(isnull(col("imp_ees_mc_ees_2_mc_sn_res_est")), 0).otherwise(col("imp_ees_mc_ees_2_mc_sn_res_est"))
//     + when(isnull(col("imp_ees_mc_ees_2_ef_res_estrat")), 0).otherwise(col("imp_ees_mc_ees_2_ef_res_estrat"))
//     + when(isnull(col("imp_ees_mc_ees_2_extram_olstor")), 0).otherwise(col("imp_ees_mc_ees_2_extram_olstor"))
//     + when(isnull(col("imp_ees_mc_ees_2_extram_mont")), 0).otherwise(col("imp_ees_mc_ees_2_extram_mont")))

// var imp_ees_mc_unit_ees =  EESMargenContriEES.withColumn("imp_ees_mc_unit_ees", when(isnull(col("imp_ees_mc_ees")), 0).otherwise(col("imp_ees_mc_ees"))
//     / when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas")))

// var imp_ees_mc_unit_ees_2_extr_ols	=  imp_ees_mc_unit_ees.withColumn("imp_ees_mc_unit_ees_2_extr_ols", when(isnull(col("imp_ees_mc_ees_2_extram_olstor")), 0).otherwise(col("imp_ees_mc_ees_2_extram_olstor"))
//     / when(isnull(col("imp_ees_ventas_olstor")), 0).otherwise(col("imp_ees_ventas_olstor")))

// var imp_ees_mc_unit_ees_2_extr_mon = imp_ees_mc_unit_ees_2_extr_ols.withColumn("imp_ees_mc_unit_ees_2_extr_mon", when(isnull(col("imp_ees_mc_ees_2_extram_mont")), 0).otherwise(col("imp_ees_mc_ees_2_extram_mont"))
//     / when(isnull(col("imp_ees_ventas_monterra")), 0).otherwise(col("imp_ees_ventas_monterra")))


// var EESResPersonal = imp_ees_mc_unit_ees_2_extr_mon.withColumn("imp_ees_pers", when(isnull(col("imp_ees_pers_2_retrib")), 0).otherwise(col("imp_ees_pers_2_retrib"))
//     + when(isnull(col("imp_ees_pers_2_otros_cost_pers")), 0).otherwise(col("imp_ees_pers_2_otros_cost_pers")))

// var EESOtrosMargenesContri = EESResPersonal.withColumn("imp_ees_otros_mc", when(isnull(col("imp_ees_otros_mc_2_gdirec_mc")), 0).otherwise(col("imp_ees_otros_mc_2_gdirec_mc")))

// var EESServExternos = EESOtrosMargenesContri.withColumn("imp_ees_serv_ext", when(isnull(col("imp_ees_serv_ext_2_arrend_can")), 0).otherwise(col("imp_ees_serv_ext_2_arrend_can"))
//     + when(isnull(col("imp_ees_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_ees_serv_ext_2_pub_rrpp"))
//     + when(isnull(col("imp_ees_serv_ext_2_sumin")), 0).otherwise(col("imp_ees_serv_ext_2_sumin"))
//     + when(isnull(col("imp_ees_serv_ext_2_mant_rep")), 0).otherwise(col("imp_ees_serv_ext_2_mant_rep"))
//     + when(isnull(col("imp_ees_serv_ext_2_serv_prof")), 0).otherwise(col("imp_ees_serv_ext_2_serv_prof"))
//     + when(isnull(col("imp_ees_serv_ext_2_viajes")), 0).otherwise(col("imp_ees_serv_ext_2_viajes"))
//     + when(isnull(col("imp_ees_serv_ext_2_seg")), 0).otherwise(col("imp_ees_serv_ext_2_seg"))
//     + when(isnull(col("imp_ees_serv_ext_2_trib")), 0).otherwise(col("imp_ees_serv_ext_2_trib"))
//     + when(isnull(col("imp_ees_sv_ext_2_otros_sv_ext")), 0).otherwise(col("imp_ees_sv_ext_2_otros_sv_ext")))

// var EESTotalCostesFijosConcepto = EESServExternos.withColumn("imp_ees_tot_cf_concep", when(isnull(col("imp_ees_pers")), 0).otherwise(col("imp_ees_pers"))
//     + when(isnull(col("imp_ees_serv_ext")), 0).otherwise(col("imp_ees_serv_ext"))
//     + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp"))
//     + when(isnull(col("imp_ees_serv_transv_DG")), 0).otherwise(col("imp_ees_serv_transv_DG"))
//     + when(isnull(col("imp_ees_otros_serv_DG")), 0).otherwise(col("imp_ees_otros_serv_DG")))



// var EESCostesFijosESS = EESTotalCostesFijosConcepto.withColumn("imp_ees_cf_ees", when(isnull(col("imp_ees_cf_ees_2_mant_rep")), 0).otherwise(col("imp_ees_cf_ees_2_mant_rep"))
//     + when(isnull(col("imp_ees_cf_ees_2_tec_neotech")), 0).otherwise(col("imp_ees_cf_ees_2_tec_neotech"))
//     + when(isnull(col("imp_ees_cf_ees_2_sis_ctrl_volu")), 0).otherwise(col("imp_ees_cf_ees_2_sis_ctrl_volu"))
//     + when(isnull(col("imp_ees_cf_ees_2_act_promo")), 0).otherwise(col("imp_ees_cf_ees_2_act_promo"))
//     + when(isnull(col("imp_ees_cf_ees_2_otros_cf_ees")), 0).otherwise(col("imp_ees_cf_ees_2_otros_cf_ees")))

// var EESTotalCostesFijosAnalitica = EESCostesFijosESS.withColumn("imp_ees_tot_cf_analit", when(isnull(col("imp_ees_cf_ees")), 0).otherwise(col("imp_ees_cf_ees"))
//     + when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos"))
//     + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp")))

// var EESCostesFijos = EESTotalCostesFijosAnalitica.withColumn("imp_ees_cf", when(isnull(col("imp_ees_tot_cf_analit")), 0).otherwise(col("imp_ees_tot_cf_analit")))

// var EESResOpePrin = EESCostesFijos.withColumn("imp_ees_result_op_ppal", when(isnull(col("imp_ees_mc_ees")), 0).otherwise(col("imp_ees_mc_ees"))
//     + when(isnull(col("imp_ees_otros_mc")), 0).otherwise(col("imp_ees_otros_mc"))
//     - when(isnull(col("imp_ees_cf")), 0).otherwise(col("imp_ees_cf"))
//     - when(isnull(col("imp_ees_amort")), 0).otherwise(col("imp_ees_amort"))
//     - when(isnull(col("imp_ees_provis_recur")), 0).otherwise(col("imp_ees_provis_recur"))
//     + when(isnull(col("imp_ees_otros_result")), 0).otherwise(col("imp_ees_otros_result")))

// var EESResOpe = EESResOpePrin.withColumn("imp_ees_result_op", when(isnull(col("imp_ees_result_op_ppal")), 0).otherwise(col("imp_ees_result_op_ppal"))
//     + when(isnull(col("imp_ees_result_otras_soc")), 0).otherwise(col("imp_ees_result_otras_soc")))


// var EESIngNet = EESResOpe.withColumn("imp_ees_ing_net", when(isnull(col("imp_ees_ing_brut")), 0).otherwise(col("imp_ees_ing_brut"))
//     - when(isnull(col("imp_ees_IEPS")), 0).otherwise(col("imp_ees_IEPS")))

// var EESCosteDeProducto = EESIngNet.withColumn("imp_ees_coste_prod", when(isnull(col("imp_ees_coste_prod_2_mat_prima")), 0).otherwise(col("imp_ees_coste_prod_2_mat_prima"))
//     + when(isnull(col("imp_ees_coste_prod_2_aditivos")), 0).otherwise(col("imp_ees_coste_prod_2_aditivos"))
//     + when(isnull(col("imp_ees_coste_prod_2_mermas")), 0).otherwise(col("imp_ees_coste_prod_2_mermas"))
//     + when(isnull(col("imp_ees_coste_prod_2_desc_comp")), 0).otherwise(col("imp_ees_coste_prod_2_desc_comp")))

// var EESMargenBruto = EESCosteDeProducto.withColumn("imp_ees_mb", when(isnull(col("imp_ees_ing_net")), 0).otherwise(col("imp_ees_ing_net"))
//     - when(isnull(col("imp_ees_coste_prod")), 0).otherwise(col("imp_ees_coste_prod")))

// var EESCosteDeLogisticayDist = EESMargenBruto.withColumn("imp_ees_coste_logist_dist", when(isnull(col("imp_ees_cost_log_dist_2_transp")), 0).otherwise(col("imp_ees_cost_log_dist_2_transp")))

// var EESMargenDeContribucion = EESCosteDeLogisticayDist.withColumn("imp_ees_mc", when(isnull(col("imp_ees_mb")), 0).otherwise(col("imp_ees_mb"))
//     - when(isnull(col("imp_ees_coste_logist_dist")), 0).otherwise(col("imp_ees_coste_logist_dist"))
//     - when(isnull(col("imp_ees_coste_canal")), 0).otherwise(col("imp_ees_coste_canal"))
//    + when(isnull(col("imp_ees_extram_olstor")), 0).otherwise(col("imp_ees_extram_olstor"))
//    + when(isnull(col("imp_ees_extram_monterra")), 0).otherwise(col("imp_ees_extram_monterra")))

// var EESCostesVariablesTotales = EESMargenDeContribucion.withColumn("imp_ees_cv_tot", when(isnull(col("imp_ees_coste_prod")), 0).otherwise(col("imp_ees_coste_prod"))
//     + when(isnull(col("imp_ees_coste_logist_dist")), 0).otherwise(col("imp_ees_coste_logist_dist"))
//     + when(isnull(col("imp_ees_coste_canal")), 0).otherwise(col("imp_ees_coste_canal")))

// var imp_ees_cv_unit =  EESCostesVariablesTotales.withColumn("imp_ees_cv_unit", when(isnull(col("imp_ees_cv_tot")), 0).otherwise(col("imp_ees_cv_tot"))
//     / when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas"))).cache


# In[64]:


// display(imp_ees_cv_unit.select("imp_vvdd_otros_costes_almacen","imp_vvdd_otros_costes_efecto_almacen"))


# **Mov. Mex. VVDD**

# In[65]:


var imp_vvdd_tot_cf_analit = imp_ees_cv_unit.withColumn("imp_vvdd_ventas", when(isnull(col("imp_vvdd_ventas_go_a_reg_diesl")), 0).otherwise(col("imp_vvdd_ventas_go_a_reg_diesl"))
    + when(isnull(col("imp_vvdd_ventas_prem_92")), 0).otherwise(col("imp_vvdd_ventas_prem_92"))
    + when(isnull(col("imp_vvdd_ventas_reg_87")), 0).otherwise(col("imp_vvdd_ventas_reg_87")))
.withColumn("imp_vvdd_otros_mc", when(isnull(col("imp_vvdd_otros_mc_2_vta_direc")), 0).otherwise(col("imp_vvdd_otros_mc_2_vta_direc"))
    + when(isnull(col("imp_vvdd_otros_mc_2_interterm")), 0).otherwise(col("imp_vvdd_otros_mc_2_interterm"))
    + when(isnull(col("imp_vvdd_otros_mc_2_almacen")), 0).otherwise(col("imp_vvdd_otros_mc_2_almacen")))
.withColumn("imp_vvdd_pers", when(isnull(col("imp_vvdd_pers_2_retrib")), 0).otherwise(col("imp_vvdd_pers_2_retrib"))
    + when(isnull(col("imp_vvdd_pers_2_otro_cost_pers")), 0).otherwise(col("imp_vvdd_pers_2_otro_cost_pers")))
.withColumn("imp_vvdd_serv_ext", when(isnull(col("imp_vvdd_serv_ext_2_arrend_can")), 0).otherwise(col("imp_vvdd_serv_ext_2_arrend_can"))
    + when(isnull(col("imp_vvdd_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_vvdd_serv_ext_2_pub_rrpp"))
    + when(isnull(col("imp_vvdd_serv_ext_2_sumin")), 0).otherwise(col("imp_vvdd_serv_ext_2_sumin"))
    + when(isnull(col("imp_vvdd_serv_ext_2_mant_rep")), 0).otherwise(col("imp_vvdd_serv_ext_2_mant_rep"))
    + when(isnull(col("imp_vvdd_serv_ext_2_serv_prof")), 0).otherwise(col("imp_vvdd_serv_ext_2_serv_prof"))
    + when(isnull(col("imp_vvdd_serv_ext_2_viajes")), 0).otherwise(col("imp_vvdd_serv_ext_2_viajes"))
    + when(isnull(col("imp_vvdd_serv_ext_2_seg")), 0).otherwise(col("imp_vvdd_serv_ext_2_seg"))
    + when(isnull(col("imp_vvdd_serv_ext_2_trib")), 0).otherwise(col("imp_vvdd_serv_ext_2_trib"))
    + when(isnull(col("imp_vvdd_sv_ext_2_otros_sv_ext")), 0).otherwise(col("imp_vvdd_sv_ext_2_otros_sv_ext")))
.withColumn("imp_vvdd_tot_cf_concep", when(isnull(col("imp_vvdd_pers")), 0).otherwise(col("imp_vvdd_pers"))
    + when(isnull(col("imp_vvdd_serv_ext")), 0).otherwise(col("imp_vvdd_serv_ext"))
    + when(isnull(col("imp_vvdd_serv_corp")), 0).otherwise(col("imp_vvdd_serv_corp"))
    + when(isnull(col("imp_vvdd_serv_transv_DG")), 0).otherwise(col("imp_vvdd_serv_transv_DG"))
    + when(isnull(col("imp_vvdd_otros_serv_DG")), 0).otherwise(col("imp_vvdd_otros_serv_DG")))
.withColumn("imp_vvdd_tot_cf_analit", when(isnull(col("imp_vvdd_otros_gastos")), 0).otherwise(col("imp_vvdd_otros_gastos")))
.withColumn("imp_vvdd_cf", when(isnull(col("imp_vvdd_tot_cf_analit")), 0).otherwise(col("imp_vvdd_tot_cf_analit")))
.withColumn("imp_vvdd_result_op", when(isnull(col("imp_vvdd_otros_mc")), 0).otherwise(col("imp_vvdd_otros_mc"))
    - when(isnull(col("imp_vvdd_cf")), 0).otherwise(col("imp_vvdd_cf"))
    - when(isnull(col("imp_vvdd_amort")), 0).otherwise(col("imp_vvdd_amort"))
    - when(isnull(col("imp_vvdd_provis_recur")), 0).otherwise(col("imp_vvdd_provis_recur"))
    + when(isnull(col("imp_vvdd_otros_result")), 0).otherwise(col("imp_vvdd_otros_result")))
.withColumn("imp_vvdd_coste_prod", when(isnull(col("imp_vvdd_coste_prod_2_mat_prim")), 0).otherwise(col("imp_vvdd_coste_prod_2_mat_prim"))
    + when(isnull(col("imp_vvdd_coste_prod_2_mermas")), 0).otherwise(col("imp_vvdd_coste_prod_2_mermas")))
.withColumn("imp_vvdd_mb", when(isnull(col("imp_vvdd_ing_net")), 0).otherwise(col("imp_vvdd_ing_net"))
    - when(isnull(col("imp_vvdd_coste_prod")), 0).otherwise(col("imp_vvdd_coste_prod")))
.withColumn("imp_vvdd_otros_costes_var", when(isnull(col("imp_vvdd_otros_costes_almacen")), 0).otherwise(col("imp_vvdd_otros_costes_almacen"))
    + when(isnull(col("imp_vvdd_otros_costes_efecto_almacen")), 0).otherwise(col("imp_vvdd_otros_costes_efecto_almacen"))) 
.withColumn("imp_vvdd_mc", when(isnull(col("imp_vvdd_mb")), 0).otherwise(col("imp_vvdd_mb"))
- when(isnull(col("imp_vvdd_otros_costes_var")), 0).otherwise(col("imp_vvdd_otros_costes_var")))
.withColumn("imp_vvdd_cv_tot", when(isnull(col("imp_vvdd_coste_prod")), 0).otherwise(col("imp_vvdd_coste_prod")))
.withColumn("imp_vvdd_cv_unit", when(isnull(col("imp_vvdd_cv_tot")), 0).otherwise(col("imp_vvdd_cv_tot"))
    / when(isnull(col("imp_vvdd_ventas")), 0).otherwise(col("imp_vvdd_ventas")))
.withColumn("imp_vvdd_tot_cf_analit", when(isnull(col("imp_vvdd_otros_gastos")), 0).otherwise(col("imp_vvdd_otros_gastos"))).cache

// var VVDDVentas = imp_ees_cv_unit.withColumn("imp_vvdd_ventas", when(isnull(col("imp_vvdd_ventas_go_a_reg_diesl")), 0).otherwise(col("imp_vvdd_ventas_go_a_reg_diesl"))
//     + when(isnull(col("imp_vvdd_ventas_prem_92")), 0).otherwise(col("imp_vvdd_ventas_prem_92"))
//     + when(isnull(col("imp_vvdd_ventas_reg_87")), 0).otherwise(col("imp_vvdd_ventas_reg_87")))

// var VVDDOtrosMargenes = VVDDVentas.withColumn("imp_vvdd_otros_mc", when(isnull(col("imp_vvdd_otros_mc_2_vta_direc")), 0).otherwise(col("imp_vvdd_otros_mc_2_vta_direc"))
//     + when(isnull(col("imp_vvdd_otros_mc_2_interterm")), 0).otherwise(col("imp_vvdd_otros_mc_2_interterm"))
//     + when(isnull(col("imp_vvdd_otros_mc_2_almacen")), 0).otherwise(col("imp_vvdd_otros_mc_2_almacen")))

// var VVDDPersonal = VVDDOtrosMargenes.withColumn("imp_vvdd_pers", when(isnull(col("imp_vvdd_pers_2_retrib")), 0).otherwise(col("imp_vvdd_pers_2_retrib"))
//     + when(isnull(col("imp_vvdd_pers_2_otro_cost_pers")), 0).otherwise(col("imp_vvdd_pers_2_otro_cost_pers")))

// var VVDDServExt = VVDDPersonal.withColumn("imp_vvdd_serv_ext", when(isnull(col("imp_vvdd_serv_ext_2_arrend_can")), 0).otherwise(col("imp_vvdd_serv_ext_2_arrend_can"))
//     + when(isnull(col("imp_vvdd_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_vvdd_serv_ext_2_pub_rrpp"))
//     + when(isnull(col("imp_vvdd_serv_ext_2_sumin")), 0).otherwise(col("imp_vvdd_serv_ext_2_sumin"))
//     + when(isnull(col("imp_vvdd_serv_ext_2_mant_rep")), 0).otherwise(col("imp_vvdd_serv_ext_2_mant_rep"))
//     + when(isnull(col("imp_vvdd_serv_ext_2_serv_prof")), 0).otherwise(col("imp_vvdd_serv_ext_2_serv_prof"))
//     + when(isnull(col("imp_vvdd_serv_ext_2_viajes")), 0).otherwise(col("imp_vvdd_serv_ext_2_viajes"))
//     + when(isnull(col("imp_vvdd_serv_ext_2_seg")), 0).otherwise(col("imp_vvdd_serv_ext_2_seg"))
//     + when(isnull(col("imp_vvdd_serv_ext_2_trib")), 0).otherwise(col("imp_vvdd_serv_ext_2_trib"))
//     + when(isnull(col("imp_vvdd_sv_ext_2_otros_sv_ext")), 0).otherwise(col("imp_vvdd_sv_ext_2_otros_sv_ext")))

// var VVDDTotCFConcep = VVDDServExt.withColumn("imp_vvdd_tot_cf_concep", when(isnull(col("imp_vvdd_pers")), 0).otherwise(col("imp_vvdd_pers"))
//     + when(isnull(col("imp_vvdd_serv_ext")), 0).otherwise(col("imp_vvdd_serv_ext"))
//     + when(isnull(col("imp_vvdd_serv_corp")), 0).otherwise(col("imp_vvdd_serv_corp"))
//     + when(isnull(col("imp_vvdd_serv_transv_DG")), 0).otherwise(col("imp_vvdd_serv_transv_DG"))
//     + when(isnull(col("imp_vvdd_otros_serv_DG")), 0).otherwise(col("imp_vvdd_otros_serv_DG")))

// var VVDDTotCFAnalit = VVDDTotCFConcep.withColumn("imp_vvdd_tot_cf_analit", when(isnull(col("imp_vvdd_otros_gastos")), 0).otherwise(col("imp_vvdd_otros_gastos")))

// var VVDDCostesFijos = VVDDTotCFAnalit.withColumn("imp_vvdd_cf", when(isnull(col("imp_vvdd_tot_cf_analit")), 0).otherwise(col("imp_vvdd_tot_cf_analit")))

// var VVDDResuOpe = VVDDCostesFijos.withColumn("imp_vvdd_result_op", when(isnull(col("imp_vvdd_otros_mc")), 0).otherwise(col("imp_vvdd_otros_mc"))
//     - when(isnull(col("imp_vvdd_cf")), 0).otherwise(col("imp_vvdd_cf"))
//     - when(isnull(col("imp_vvdd_amort")), 0).otherwise(col("imp_vvdd_amort"))
//     - when(isnull(col("imp_vvdd_provis_recur")), 0).otherwise(col("imp_vvdd_provis_recur"))
//     + when(isnull(col("imp_vvdd_otros_result")), 0).otherwise(col("imp_vvdd_otros_result")))

// var VVDDCosteProd = VVDDResuOpe.withColumn("imp_vvdd_coste_prod", when(isnull(col("imp_vvdd_coste_prod_2_mat_prim")), 0).otherwise(col("imp_vvdd_coste_prod_2_mat_prim"))
//     + when(isnull(col("imp_vvdd_coste_prod_2_mermas")), 0).otherwise(col("imp_vvdd_coste_prod_2_mermas")))

// var VVDDMargenBruto = VVDDCosteProd.withColumn("imp_vvdd_mb", when(isnull(col("imp_vvdd_ing_net")), 0).otherwise(col("imp_vvdd_ing_net"))
//     - when(isnull(col("imp_vvdd_coste_prod")), 0).otherwise(col("imp_vvdd_coste_prod")))

// var VVDDMargenContri = VVDDMargenBruto.withColumn("imp_vvdd_mc", when(isnull(col("imp_vvdd_mb")), 0).otherwise(col("imp_vvdd_mb")))

// var imp_vvdd_cv_tot =  VVDDMargenContri.withColumn("imp_vvdd_cv_tot", when(isnull(col("imp_vvdd_coste_prod")), 0).otherwise(col("imp_vvdd_coste_prod"))
//     // + when(isnull(col("")), 0).otherwise(col(""))
//     // + when(isnull(col("")), 0).otherwise(col(""))
//     )

// var VVDDCVUnit = imp_vvdd_cv_tot.withColumn("imp_vvdd_cv_unit", when(isnull(col("imp_vvdd_cv_tot")), 0).otherwise(col("imp_vvdd_cv_tot"))
//     / when(isnull(col("imp_vvdd_ventas")), 0).otherwise(col("imp_vvdd_ventas")))

// var imp_vvdd_tot_cf_analit =  VVDDCVUnit.withColumn("imp_vvdd_tot_cf_analit", when(isnull(col("imp_vvdd_otros_gastos")), 0).otherwise(col("imp_vvdd_otros_gastos"))).cache


# **Mov. Mex. Total**

# In[66]:


var t_Pool_mm =  imp_vvdd_tot_cf_analit.withColumn("imp_tot_ventas_go_a_reg_diesel", when(isnull(col("imp_vvdd_ventas_go_a_reg_diesl")), 0).otherwise(col("imp_vvdd_ventas_go_a_reg_diesl"))
    + when(isnull(col("imp_ees_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_ees_ventas_go_a_reg_diesel")))
.withColumn("imp_tot_ventas_prem_92", when(isnull(col("imp_vvdd_ventas_prem_92")), 0).otherwise(col("imp_vvdd_ventas_prem_92"))
    + when(isnull(col("imp_ees_ventas_prem_92")), 0).otherwise(col("imp_ees_ventas_prem_92")))
.withColumn("imp_tot_otros_costes_efecto_almacen",col("imp_vvdd_otros_costes_efecto_almacen"))
.withColumn("imp_tot_otros_costes_almacen",col("imp_vvdd_otros_costes_almacen"))
.withColumn("imp_tot_mc_ees_2_ef_res_estrat",col("imp_ees_mc_ees_2_ef_res_estrat"))
.withColumn("imp_tot_ventas_reg_87", when(isnull(col("imp_vvdd_ventas_reg_87")), 0).otherwise(col("imp_vvdd_ventas_reg_87"))
    + when(isnull(col("imp_ees_ventas_reg_87")), 0).otherwise(col("imp_ees_ventas_reg_87")))
.withColumn("imp_total_ventas", when(isnull(col("imp_tot_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_tot_ventas_go_a_reg_diesel"))
    + when(isnull(col("imp_tot_ventas_prem_92")), 0).otherwise(col("imp_tot_ventas_prem_92"))
    + when(isnull(col("imp_tot_ventas_reg_87")), 0).otherwise(col("imp_tot_ventas_reg_87")))
.withColumn("imp_tot_mm_ing_net", when(isnull(col("imp_ees_ing_net")), 0).otherwise(col("imp_ees_ing_net"))
    + when(isnull(col("imp_vvdd_ing_net")), 0).otherwise(col("imp_vvdd_ing_net")))
.withColumn("imp_tot_mm_coste_prod_2_m_prim", when(isnull(col("imp_ees_coste_prod_2_mat_prima")), 0).otherwise(col("imp_ees_coste_prod_2_mat_prima"))
    + when(isnull(col("imp_vvdd_coste_prod_2_mat_prim")), 0).otherwise(col("imp_vvdd_coste_prod_2_mat_prim")))
.withColumn("imp_tot_mm_coste_prod_2_adt", when(isnull(col("imp_ees_coste_prod_2_aditivos")), 0).otherwise(col("imp_ees_coste_prod_2_aditivos")))
.withColumn("imp_tot_mm_coste_prod_2_mermas", when(isnull(col("imp_ees_coste_prod_2_mermas")), 0).otherwise(col("imp_ees_coste_prod_2_mermas"))
    + when(isnull(col("imp_vvdd_coste_prod_2_mermas")), 0).otherwise(col("imp_vvdd_coste_prod_2_mermas")))
.withColumn("imp_tot_mm_coste_prod_2_desc", when(isnull(col("imp_ees_coste_prod_2_desc_comp")), 0).otherwise(col("imp_ees_coste_prod_2_desc_comp")))
.withColumn("imp_tot_mm_coste_prod", when(isnull(col("imp_tot_mm_coste_prod_2_m_prim")), 0).otherwise(col("imp_tot_mm_coste_prod_2_m_prim"))
    + when(isnull(col("imp_tot_mm_coste_prod_2_adt")), 0).otherwise(col("imp_tot_mm_coste_prod_2_adt"))
    + when(isnull(col("imp_tot_mm_coste_prod_2_mermas")), 0).otherwise(col("imp_tot_mm_coste_prod_2_mermas"))
    + when(isnull(col("imp_tot_mm_coste_prod_2_desc")), 0).otherwise(col("imp_tot_mm_coste_prod_2_desc")))
.withColumn("imp_tot_mm_mb", when(isnull(col("imp_tot_mm_ing_net")), 0).otherwise(col("imp_tot_mm_ing_net"))
    - when(isnull(col("imp_tot_mm_coste_prod")), 0).otherwise(col("imp_tot_mm_coste_prod")))
.withColumn("imp_tot_mm_cost_log_dist_2_tsp", when(isnull(col("imp_ees_cost_log_dist_2_transp")), 0).otherwise(col("imp_ees_cost_log_dist_2_transp")))
.withColumn("imp_tot_mm_coste_logist_dist", when(isnull(col("imp_tot_mm_cost_log_dist_2_tsp")), 0).otherwise(col("imp_tot_mm_cost_log_dist_2_tsp")))
.withColumn("imp_tot_mm_extram_olstor", when(isnull(col("imp_ees_extram_olstor")), 0).otherwise(col("imp_ees_extram_olstor")))
.withColumn("imp_tot_mm_extram_monterra", when(isnull(col("imp_ees_extram_monterra")), 0).otherwise(col("imp_ees_extram_monterra")))
.withColumn("imp_tot_mm_coste_canal", when(isnull(col("imp_tot_mm_extram_olstor")), 0).otherwise(col("imp_tot_mm_extram_olstor"))
    + when(isnull(col("imp_tot_mm_extram_monterra")), 0).otherwise(col("imp_tot_mm_extram_monterra")))
.withColumn("imp_tot_mm_mc", when(isnull(col("imp_tot_mm_mb")), 0).otherwise(col("imp_tot_mm_mb"))
    - when(isnull(col("imp_tot_mm_coste_logist_dist")), 0).otherwise(col("imp_tot_mm_coste_logist_dist"))
    - when(isnull(col("imp_tot_mm_coste_canal")), 0).otherwise(col("imp_tot_mm_coste_canal"))
    + when(isnull(col("imp_tot_mm_extram_olstor")), 0).otherwise(col("imp_tot_mm_extram_olstor"))
    + when(isnull(col("imp_tot_mm_extram_monterra")), 0).otherwise(col("imp_tot_mm_extram_monterra"))
    - when(isnull(col("imp_tot_otros_costes_almacen")), 0).otherwise(col("imp_tot_otros_costes_almacen"))
    + when(isnull(col("imp_ees_otros_mc")), 0).otherwise(col("imp_ees_otros_mc")))
.withColumn("imp_tot_mm_mb_sin_desc_cv", when(isnull(col("imp_ees_mb_sin_des_ing_c_prima")), 0).otherwise(col("imp_ees_mb_sin_des_ing_c_prima")))
.withColumn("imp_tot_mm_cv_tot", when(isnull(col("imp_tot_mm_coste_prod")), 0).otherwise(col("imp_tot_mm_coste_prod"))
    + when(isnull(col("imp_tot_mm_coste_logist_dist")), 0).otherwise(col("imp_tot_mm_coste_logist_dist"))
    + when(isnull(col("imp_ees_coste_canal")), 0).otherwise(col("imp_ees_coste_canal")))
.withColumn("imp_tot_mm_cv_unit", when(isnull(col("imp_tot_mm_cv_tot")), 0).otherwise(col("imp_tot_mm_cv_tot"))
    / when(isnull(col("imp_total_ventas")), 0).otherwise(col("imp_total_ventas")))
.withColumn("imp_tot_mm_ventas_mvcv", when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas"))
    + when(isnull(col("imp_vvdd_ventas")), 0).otherwise(col("imp_vvdd_ventas")))
.withColumn("imp_tot_mm_ventas_cr", when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas"))
    + when(isnull(col("imp_vvdd_ventas")), 0).otherwise(col("imp_vvdd_ventas")))
.withColumn("imp_tot_mm_mc_ees_2_ef_res_est", when(isnull(col("imp_ees_mc_ees_2_ef_res_estrat")), 0).otherwise(col("imp_ees_mc_ees_2_ef_res_estrat")))
.withColumn("imp_tot_mm_mc_ees_2_extram_ols", when(isnull(col("imp_ees_mc_ees_2_extram_olstor")), 0).otherwise(col("imp_ees_mc_ees_2_extram_olstor")))
.withColumn("imp_tot_mm_mc_ees_2_extram_mon", when(isnull(col("imp_ees_mc_ees_2_extram_mont")), 0).otherwise(col("imp_ees_mc_ees_2_extram_mont")))
.withColumn("imp_tot_mm_mc_ees_2_s_res_est", when(isnull(col("imp_ees_mc_ees_2_mc_sn_res_est")), 0).otherwise(col("imp_ees_mc_ees_2_mc_sn_res_est")))
.withColumn("imp_tot_mm_mc_ees", when(isnull(col("imp_tot_mm_mc_ees_2_s_res_est")), 0).otherwise(col("imp_tot_mm_mc_ees_2_s_res_est"))
    + when(isnull(col("imp_tot_mm_mc_ees_2_ef_res_est")), 0).otherwise(col("imp_tot_mm_mc_ees_2_ef_res_est"))
    + when(isnull(col("imp_tot_mm_mc_ees_2_extram_ols")), 0).otherwise(col("imp_tot_mm_mc_ees_2_extram_ols"))
    + when(isnull(col("imp_tot_mm_mc_ees_2_extram_mon")), 0).otherwise(col("imp_tot_mm_mc_ees_2_extram_mon")))
.withColumn("imp_tot_mm_mc_uni_ees_2_resest", when(isnull(col("imp_tot_mm_mc_ees_2_ef_res_est")), 0).otherwise(col("imp_tot_mm_mc_ees_2_ef_res_est"))
    / when(isnull(col("imp_tot_mm_ventas_cr")), 0).otherwise(col("imp_tot_mm_ventas_cr")))
.withColumn("imp_tot_mm_mc_uni_ees_2_s_res", when(isnull(col("imp_tot_mm_mc_ees_2_s_res_est")), 0).otherwise(col("imp_tot_mm_mc_ees_2_s_res_est"))
    / when(isnull(col("imp_tot_mm_ventas_cr")), 0).otherwise(col("imp_tot_mm_ventas_cr")))
.withColumn("imp_tot_mm_mc_uni_ees_2_olstor", when(isnull(col("imp_tot_mm_mc_ees_2_extram_ols")), 0).otherwise(col("imp_tot_mm_mc_ees_2_extram_ols"))
    / when(isnull(col("imp_tot_mm_ventas_cr")), 0).otherwise(col("imp_tot_mm_ventas_cr")))
.withColumn("imp_tot_mm_mc_unit_ees_2_mont", when(isnull(col("imp_tot_mm_mc_ees_2_extram_mon")), 0).otherwise(col("imp_tot_mm_mc_ees_2_extram_mon"))
    / when(isnull(col("imp_tot_mm_ventas_cr")), 0).otherwise(col("imp_tot_mm_ventas_cr")))
.withColumn("imp_tot_mm_mc_unit_ees", when(isnull(col("imp_tot_mm_mc_ees")), 0).otherwise(col("imp_tot_mm_mc_ees"))
    / when(isnull(col("imp_tot_mm_ventas_cr")), 0).otherwise(col("imp_tot_mm_ventas_cr")))
.withColumn("imp_tot_mm_otro_mc_2_g_direc", when(isnull(col("imp_ees_otros_mc_2_gdirec_mc")), 0).otherwise(col("imp_ees_otros_mc_2_gdirec_mc")))
.withColumn("imp_tot_mm_otro_mc_2_t_vta_dir", when(isnull(col("imp_vvdd_otros_mc_2_vta_direc")), 0).otherwise(col("imp_vvdd_otros_mc_2_vta_direc")))
.withColumn("imp_tot_mm_otro_mc_2_t_inter", when(isnull(col("imp_vvdd_otros_mc_2_interterm")), 0).otherwise(col("imp_vvdd_otros_mc_2_interterm")))
.withColumn("imp_tot_mm_otro_mc_2_t_almacen", when(isnull(col("imp_vvdd_otros_mc_2_almacen")), 0).otherwise(col("imp_vvdd_otros_mc_2_almacen")))
.withColumn("imp_tot_mm_otros_mc", when(isnull(col("imp_tot_mm_otro_mc_2_g_direc")), 0).otherwise(col("imp_tot_mm_otro_mc_2_g_direc"))
    + when(isnull(col("imp_tot_mm_otro_mc_2_t_vta_dir")), 0).otherwise(col("imp_tot_mm_otro_mc_2_t_vta_dir"))
    + when(isnull(col("imp_tot_mm_otro_mc_2_t_inter")), 0).otherwise(col("imp_tot_mm_otro_mc_2_t_inter"))
    + when(isnull(col("imp_tot_mm_otro_mc_2_t_almacen")), 0).otherwise(col("imp_tot_mm_otro_mc_2_t_almacen")))
.withColumn("imp_tot_mm_provis_recur", when(isnull(col("imp_ees_provis_recur")), 0).otherwise(col("imp_ees_provis_recur")))
.withColumn("imp_tot_mm_otros_result", when(isnull(col("imp_ees_otros_result")), 0).otherwise(col("imp_ees_otros_result")))
.withColumn("imp_tot_mm_cf_ees_2_mant_rep", when(isnull(col("imp_ees_cf_ees_2_mant_rep")), 0).otherwise(col("imp_ees_cf_ees_2_mant_rep")))
.withColumn("imp_tot_mm_cf_ees_2_neotech", when(isnull(col("imp_ees_cf_ees_2_tec_neotech")), 0).otherwise(col("imp_ees_cf_ees_2_tec_neotech")))
.withColumn("imp_tot_mm_cf_ees_2_ctrl_volu", when(isnull(col("imp_ees_cf_ees_2_sis_ctrl_volu")), 0).otherwise(col("imp_ees_cf_ees_2_sis_ctrl_volu")))
.withColumn("imp_tot_mm_cf_ees_2_act_promo", when(isnull(col("imp_ees_cf_ees_2_act_promo")), 0).otherwise(col("imp_ees_cf_ees_2_act_promo")))
.withColumn("imp_tot_mm_cf_ees_2_otro_cf", when(isnull(col("imp_ees_cf_ees_2_otros_cf_ees")), 0).otherwise(col("imp_ees_cf_ees_2_otros_cf_ees")))
.withColumn("imp_tot_mm_otros_gastos", when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos"))
    + when(isnull(col("imp_vvdd_otros_gastos")), 0).otherwise(col("imp_vvdd_otros_gastos"))).cache


// var TotMMVentasDiesel =  imp_vvdd_tot_cf_analit.withColumn("imp_tot_ventas_go_a_reg_diesel", when(isnull(col("imp_vvdd_ventas_go_a_reg_diesl")), 0).otherwise(col("imp_vvdd_ventas_go_a_reg_diesl"))
//     + when(isnull(col("imp_ees_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_ees_ventas_go_a_reg_diesel")))

// var TotMMVentasPremium =  TotMMVentasDiesel.withColumn("imp_tot_ventas_prem_92", when(isnull(col("imp_vvdd_ventas_prem_92")), 0).otherwise(col("imp_vvdd_ventas_prem_92"))
//     + when(isnull(col("imp_ees_ventas_prem_92")), 0).otherwise(col("imp_ees_ventas_prem_92")))


// var TotMMVentasReg =  TotMMVentasPremium.withColumn("imp_tot_ventas_reg_87", when(isnull(col("imp_vvdd_ventas_reg_87")), 0).otherwise(col("imp_vvdd_ventas_reg_87"))
//     + when(isnull(col("imp_ees_ventas_reg_87")), 0).otherwise(col("imp_ees_ventas_reg_87")))

// var imp_total_ventas = TotMMVentasReg.withColumn("imp_total_ventas", when(isnull(col("imp_tot_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_tot_ventas_go_a_reg_diesel"))
//     + when(isnull(col("imp_tot_ventas_prem_92")), 0).otherwise(col("imp_tot_ventas_prem_92"))
//     + when(isnull(col("imp_tot_ventas_reg_87")), 0).otherwise(col("imp_tot_ventas_reg_87")))

// var TotMMIngNeto =  imp_total_ventas.withColumn("imp_tot_mm_ing_net", when(isnull(col("imp_ees_ing_net")), 0).otherwise(col("imp_ees_ing_net"))
//     + when(isnull(col("imp_vvdd_ing_net")), 0).otherwise(col("imp_vvdd_ing_net")))

// var TotMMCosteProdMP =  TotMMIngNeto.withColumn("imp_tot_mm_coste_prod_2_m_prim", when(isnull(col("imp_ees_coste_prod_2_mat_prima")), 0).otherwise(col("imp_ees_coste_prod_2_mat_prima"))
//     + when(isnull(col("imp_vvdd_coste_prod_2_mat_prim")), 0).otherwise(col("imp_vvdd_coste_prod_2_mat_prim")))

// var TotMMCosteProdAdit =  TotMMCosteProdMP.withColumn("imp_tot_mm_coste_prod_2_adt", when(isnull(col("imp_ees_coste_prod_2_aditivos")), 0).otherwise(col("imp_ees_coste_prod_2_aditivos")))

// var TotMMCosteProdMermas =  TotMMCosteProdAdit.withColumn("imp_tot_mm_coste_prod_2_mermas", when(isnull(col("imp_ees_coste_prod_2_mermas")), 0).otherwise(col("imp_ees_coste_prod_2_mermas"))
//     + when(isnull(col("imp_vvdd_coste_prod_2_mermas")), 0).otherwise(col("imp_vvdd_coste_prod_2_mermas")))

// var TotMMCosteProdDesc =  TotMMCosteProdMermas.withColumn("imp_tot_mm_coste_prod_2_desc", when(isnull(col("imp_ees_coste_prod_2_desc_comp")), 0).otherwise(col("imp_ees_coste_prod_2_desc_comp")))

// var TotMMCosteProd =  TotMMCosteProdDesc.withColumn("imp_tot_mm_coste_prod", when(isnull(col("imp_tot_mm_coste_prod_2_m_prim")), 0).otherwise(col("imp_tot_mm_coste_prod_2_m_prim"))
//     + when(isnull(col("imp_tot_mm_coste_prod_2_adt")), 0).otherwise(col("imp_tot_mm_coste_prod_2_adt"))
//     + when(isnull(col("imp_tot_mm_coste_prod_2_mermas")), 0).otherwise(col("imp_tot_mm_coste_prod_2_mermas"))
//     + when(isnull(col("imp_tot_mm_coste_prod_2_desc")), 0).otherwise(col("imp_tot_mm_coste_prod_2_desc")))

// var TotMMMB =  TotMMCosteProd.withColumn("imp_tot_mm_mb", when(isnull(col("imp_tot_mm_ing_net")), 0).otherwise(col("imp_tot_mm_ing_net"))
//     - when(isnull(col("imp_tot_mm_coste_prod")), 0).otherwise(col("imp_tot_mm_coste_prod")))

// var TotMMCLTransp =  TotMMMB.withColumn("imp_tot_mm_cost_log_dist_2_tsp", when(isnull(col("imp_ees_cost_log_dist_2_transp")), 0).otherwise(col("imp_ees_cost_log_dist_2_transp")))

// var TotMMCosteLogistDist =  TotMMCLTransp.withColumn("imp_tot_mm_coste_logist_dist", when(isnull(col("imp_tot_mm_cost_log_dist_2_tsp")), 0).otherwise(col("imp_tot_mm_cost_log_dist_2_tsp")))

// var TotMMExtramOlstor =  TotMMCosteLogistDist.withColumn("imp_tot_mm_extram_olstor", when(isnull(col("imp_ees_extram_olstor")), 0).otherwise(col("imp_ees_extram_olstor")))

// var TotMMExtramMonterra =  TotMMExtramOlstor.withColumn("imp_tot_mm_extram_monterra", when(isnull(col("imp_ees_extram_monterra")), 0).otherwise(col("imp_ees_extram_monterra")))

// var TotMMCC =  TotMMExtramMonterra.withColumn("imp_tot_mm_coste_canal", when(isnull(col("imp_tot_mm_extram_olstor")), 0).otherwise(col("imp_tot_mm_extram_olstor"))
//     + when(isnull(col("imp_tot_mm_extram_monterra")), 0).otherwise(col("imp_tot_mm_extram_monterra")))

// var TotMMMC =  TotMMCC.withColumn("imp_tot_mm_mc", when(isnull(col("imp_tot_mm_mb")), 0).otherwise(col("imp_tot_mm_mb"))
//     - when(isnull(col("imp_tot_mm_coste_logist_dist")), 0).otherwise(col("imp_tot_mm_coste_logist_dist"))
//     - when(isnull(col("imp_ees_coste_canal")), 0).otherwise(col("imp_ees_coste_canal"))
//     + when(isnull(col("imp_ees_extram_olstor")), 0).otherwise(col("imp_ees_extram_olstor"))
//     + when(isnull(col("imp_ees_extram_monterra")), 0).otherwise(col("imp_ees_extram_monterra")))

// var TotMMMBSinDesc_cv =  TotMMMC.withColumn("imp_tot_mm_mb_sin_desc_cv", when(isnull(col("imp_ees_mb_sin_des_ing_c_prima")), 0).otherwise(col("imp_ees_mb_sin_des_ing_c_prima")))

// var TotMMCVTot =  TotMMMBSinDesc_cv.withColumn("imp_tot_mm_cv_tot", when(isnull(col("imp_tot_mm_coste_prod")), 0).otherwise(col("imp_tot_mm_coste_prod"))
//     + when(isnull(col("imp_tot_mm_coste_logist_dist")), 0).otherwise(col("imp_tot_mm_coste_logist_dist"))
//     + when(isnull(col("imp_ees_coste_canal")), 0).otherwise(col("imp_ees_coste_canal")))

// var TotMMCVunit =  TotMMCVTot.withColumn("imp_tot_mm_cv_unit", when(isnull(col("imp_tot_mm_cv_tot")), 0).otherwise(col("imp_tot_mm_cv_tot"))
//     / when(isnull(col("imp_total_ventas")), 0).otherwise(col("imp_total_ventas")))

// var TotMMVentasMcVc =  TotMMCVunit.withColumn("imp_tot_mm_ventas_mvcv", when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas"))
//     + when(isnull(col("imp_vvdd_ventas")), 0).otherwise(col("imp_vvdd_ventas")))


// var TotMMVentarCR =  TotMMVentasMcVc.withColumn("imp_tot_mm_ventas_cr", when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas"))
//     + when(isnull(col("imp_vvdd_ventas")), 0).otherwise(col("imp_vvdd_ventas")))


// var imp_tot_mm_mc_ees_2_ef_res_est =  TotMMVentarCR.withColumn("imp_tot_mm_mc_ees_2_ef_res_est", when(isnull(col("imp_ees_mc_ees_2_ef_res_estrat")), 0).otherwise(col("imp_ees_mc_ees_2_ef_res_estrat")))

// var imp_tot_mm_mc_ees_2_extram_ols =  imp_tot_mm_mc_ees_2_ef_res_est.withColumn("imp_tot_mm_mc_ees_2_extram_ols", when(isnull(col("imp_ees_mc_ees_2_extram_olstor")), 0).otherwise(col("imp_ees_mc_ees_2_extram_olstor")))

// var imp_tot_mm_mc_ees_2_extram_mon =  imp_tot_mm_mc_ees_2_extram_ols.withColumn("imp_tot_mm_mc_ees_2_extram_mon", when(isnull(col("imp_ees_mc_ees_2_extram_mont")), 0).otherwise(col("imp_ees_mc_ees_2_extram_mont")))

// var TotMMSMCSinResEstrat =  imp_tot_mm_mc_ees_2_extram_mon.withColumn("imp_tot_mm_mc_ees_2_s_res_est", when(isnull(col("imp_ees_mc_ees_2_mc_sn_res_est")), 0).otherwise(col("imp_ees_mc_ees_2_mc_sn_res_est")))

// var TotMMMCEES =  TotMMSMCSinResEstrat.withColumn("imp_tot_mm_mc_ees", when(isnull(col("imp_tot_mm_mc_ees_2_s_res_est")), 0).otherwise(col("imp_tot_mm_mc_ees_2_s_res_est"))
//     + when(isnull(col("imp_tot_mm_mc_ees_2_ef_res_est")), 0).otherwise(col("imp_tot_mm_mc_ees_2_ef_res_est"))
//     + when(isnull(col("imp_tot_mm_mc_ees_2_extram_ols")), 0).otherwise(col("imp_tot_mm_mc_ees_2_extram_ols"))
//     + when(isnull(col("imp_tot_mm_mc_ees_2_extram_mon")), 0).otherwise(col("imp_tot_mm_mc_ees_2_extram_mon")))

// var TotMMMCUnitres =  TotMMMCEES.withColumn("imp_tot_mm_mc_uni_ees_2_resest", when(isnull(col("imp_tot_mm_mc_ees_2_ef_res_est")), 0).otherwise(col("imp_tot_mm_mc_ees_2_ef_res_est"))
//     / when(isnull(col("imp_tot_mm_ventas_cr")), 0).otherwise(col("imp_tot_mm_ventas_cr")))


// var TotMMMCSinResEstrat =  TotMMMCUnitres.withColumn("imp_tot_mm_mc_uni_ees_2_s_res", when(isnull(col("imp_tot_mm_mc_ees_2_s_res_est")), 0).otherwise(col("imp_tot_mm_mc_ees_2_s_res_est"))
//     / when(isnull(col("imp_tot_mm_ventas_cr")), 0).otherwise(col("imp_tot_mm_ventas_cr")))


// var TotMMMCUnitolster =  TotMMMCSinResEstrat.withColumn("imp_tot_mm_mc_uni_ees_2_olstor", when(isnull(col("imp_tot_mm_mc_ees_2_extram_ols")), 0).otherwise(col("imp_tot_mm_mc_ees_2_extram_ols"))
//     / when(isnull(col("imp_tot_mm_ventas_cr")), 0).otherwise(col("imp_tot_mm_ventas_cr")))
// //imp_tot_mm_ventas_cr filtrado por origen: Olstor

// var TotMMMCUnitMonterra =  TotMMMCUnitolster.withColumn("imp_tot_mm_mc_unit_ees_2_mont", when(isnull(col("imp_tot_mm_mc_ees_2_extram_mon")), 0).otherwise(col("imp_tot_mm_mc_ees_2_extram_mon"))
//     / when(isnull(col("imp_tot_mm_ventas_cr")), 0).otherwise(col("imp_tot_mm_ventas_cr")))
// //imp_tot_mm_ventas_cr filtrado por origen: Monterra

// var TotMMMCUnit =  TotMMMCUnitMonterra.withColumn("imp_tot_mm_mc_unit_ees", when(isnull(col("imp_tot_mm_mc_ees")), 0).otherwise(col("imp_tot_mm_mc_ees"))
//     / when(isnull(col("imp_tot_mm_ventas_cr")), 0).otherwise(col("imp_tot_mm_ventas_cr")))

// var imp_tot_mm_otro_mc_2_g_direc =  TotMMMCUnit.withColumn("imp_tot_mm_otro_mc_2_g_direc", when(isnull(col("imp_ees_otros_mc_2_gdirec_mc")), 0).otherwise(col("imp_ees_otros_mc_2_gdirec_mc")))

// var imp_tot_mm_otro_mc_2_t_vta_dir =  imp_tot_mm_otro_mc_2_g_direc.withColumn("imp_tot_mm_otro_mc_2_t_vta_dir", when(isnull(col("imp_vvdd_otros_mc_2_vta_direc")), 0).otherwise(col("imp_vvdd_otros_mc_2_vta_direc")))

// var imp_tot_mm_otro_mc_2_t_inter =  imp_tot_mm_otro_mc_2_t_vta_dir.withColumn("imp_tot_mm_otro_mc_2_t_inter", when(isnull(col("imp_vvdd_otros_mc_2_interterm")), 0).otherwise(col("imp_vvdd_otros_mc_2_interterm")))

// var imp_tot_mm_otro_mc_2_t_almacen =  imp_tot_mm_otro_mc_2_t_inter.withColumn("imp_tot_mm_otro_mc_2_t_almacen", when(isnull(col("imp_vvdd_otros_mc_2_almacen")), 0).otherwise(col("imp_vvdd_otros_mc_2_almacen")))

// var imp_tot_mm_otros_mc =  imp_tot_mm_otro_mc_2_t_almacen.withColumn("imp_tot_mm_otros_mc", when(isnull(col("imp_tot_mm_otro_mc_2_g_direc")), 0).otherwise(col("imp_tot_mm_otro_mc_2_g_direc"))
//     + when(isnull(col("imp_tot_mm_otro_mc_2_t_vta_dir")), 0).otherwise(col("imp_tot_mm_otro_mc_2_t_vta_dir"))
//     + when(isnull(col("imp_tot_mm_otro_mc_2_t_inter")), 0).otherwise(col("imp_tot_mm_otro_mc_2_t_inter"))
//     + when(isnull(col("imp_tot_mm_otro_mc_2_t_almacen")), 0).otherwise(col("imp_tot_mm_otro_mc_2_t_almacen")))


// var imp_tot_mm_provis_recur =  imp_tot_mm_otros_mc.withColumn("imp_tot_mm_provis_recur", when(isnull(col("imp_ees_provis_recur")), 0).otherwise(col("imp_ees_provis_recur")))

// var imp_tot_mm_otros_result =  imp_tot_mm_provis_recur.withColumn("imp_tot_mm_otros_result", when(isnull(col("imp_ees_otros_result")), 0).otherwise(col("imp_ees_otros_result")))


// var imp_tot_mm_cf_ees_2_mant_rep =  imp_tot_mm_otros_result.withColumn("imp_tot_mm_cf_ees_2_mant_rep", when(isnull(col("imp_ees_cf_ees_2_mant_rep")), 0).otherwise(col("imp_ees_cf_ees_2_mant_rep")))

// var imp_tot_mm_cf_ees_2_neotech =  imp_tot_mm_cf_ees_2_mant_rep.withColumn("imp_tot_mm_cf_ees_2_neotech", when(isnull(col("imp_ees_cf_ees_2_tec_neotech")), 0).otherwise(col("imp_ees_cf_ees_2_tec_neotech")))

// var imp_tot_mm_cf_ees_2_ctrl_volu =  imp_tot_mm_cf_ees_2_neotech.withColumn("imp_tot_mm_cf_ees_2_ctrl_volu", when(isnull(col("imp_ees_cf_ees_2_sis_ctrl_volu")), 0).otherwise(col("imp_ees_cf_ees_2_sis_ctrl_volu")))

// var imp_tot_mm_cf_ees_2_act_promo =  imp_tot_mm_cf_ees_2_ctrl_volu.withColumn("imp_tot_mm_cf_ees_2_act_promo", when(isnull(col("imp_ees_cf_ees_2_act_promo")), 0).otherwise(col("imp_ees_cf_ees_2_act_promo")))

// var imp_tot_mm_cf_ees_2_otro_cf =  imp_tot_mm_cf_ees_2_act_promo.withColumn("imp_tot_mm_cf_ees_2_otro_cf", when(isnull(col("imp_ees_cf_ees_2_otros_cf_ees")), 0).otherwise(col("imp_ees_cf_ees_2_otros_cf_ees")))

// var imp_tot_mm_otros_gastos =  imp_tot_mm_cf_ees_2_otro_cf.withColumn("imp_tot_mm_otros_gastos", when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos"))
//     + when(isnull(col("imp_vvdd_otros_gastos")), 0).otherwise(col("imp_vvdd_otros_gastos")))

// // var imp_tot_mm_corp =  imp_tot_mm_otros_gastos.withColumn("imp_tot_mm_corp", when(isnull(col("imp_ees_corp")), 0).otherwise(col("imp_ees_corp"))
// //     + when(isnull(col("imp_ees_minor_corp")), 0).otherwise(col("imp_ees_minor_corp"))
// //     + when(isnull(col("imp_ees_mayor_corp")), 0).otherwise(col("imp_ees_mayor_corp")))

// // imp_tot_mm_result_net_ajust = imp_tot_mm_result_op + imp_tot_mm_result_op_2_particip_minor - imp_tot_mm_result_op_2_imp

// // imp_tot_mm_result_net = imp_tot_mm_result_net_ajust + imp_tot_mm_result_net_ajust_2_result_especif_ddi + imp_tot_mm_result_net_ajust_2_ef_patrim_ddi

// var t_Pool_mm = imp_tot_mm_otros_gastos.cache


# In[67]:


// display(t_Pool_mm.select("imp_tot_mm_mc","imp_tot_mm_mb",
// "imp_tot_mm_coste_logist_dist","imp_tot_mm_coste_canal",
// "imp_tot_mm_extram_olstor","imp_tot_mm_extram_monterra",
// "imp_tot_otros_costes_almacen","imp_ees_otros_mc"))


# In[68]:


var df_kpis_write = t_Pool_mm.where(col("cod_periodo") === v_periodo)


# In[69]:


spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

val ds_output = s"cli0010_tb_m_pivkpi_mmex_icv/"
// val ds_output_temp = ds_output.concat("temp")
val pathWrite = s"$edwPath/$business/$ds_output"
val parquet_path_ft = f"abfss://$container_output@$my_account/$pathWrite"

df_kpis_write.coalesce(1).write.partitionBy("id_escenario","cod_periodo").mode("overwrite").format("parquet").save(parquet_path_ft)


# In[70]:


for ((k,v) <- sc.getPersistentRDDs) {
   v.unpersist()
}

var df_kpis = spark.read.parquet(parquet_path_ft).where(col("cod_periodo") === v_periodo)

if (escenario == "REAL"){
    df_kpis = df_kpis.where(col("id_escenario") === 1).cache
}else if (escenario == "PA-UPA"){
    df_kpis = df_kpis.where(col("id_escenario").isin(2,3)).cache
}else if (escenario == "EST"){
    df_kpis = df_kpis.where(col("id_escenario") === 4).cache
}else if (escenario == "PA"){
    df_kpis = df_kpis.where(col("id_escenario") === 2).cache
}else if (escenario == "UPA"){
    df_kpis = df_kpis.where(col("id_escenario") === 3).cache
}else{
    df_kpis.cache
}


# In[71]:


var vc_pa = spark.emptyDataFrame

if (escenario == "PA-UPA" || escenario == "ALL" || escenario == "PA"){
    var df_pa = df_kpis.select(
        col("cod_periodo").cast(DecimalType(24,10)),
        col("imp_ees_ventas").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_ing_brut").cast(DecimalType(24,10)),
        col("imp_ees_IEPS").cast(DecimalType(24,10)),
        col("imp_ees_ing_net").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mat_prima").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_aditivos").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_desc_comp").cast(DecimalType(24,10)),
        col("imp_ees_mb").cast(DecimalType(24,10)),
        col("imp_ees_cost_log_dist_2_transp").cast(DecimalType(24,10)),
        col("imp_ees_coste_canal").cast(DecimalType(24,10)),
        col("imp_ees_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_extram_monterra").cast(DecimalType(24,10)),
        col("imp_ees_mb_sin_des_ing_c_prima").cast(DecimalType(24,10)),
        col("imp_ees_cv_tot").cast(DecimalType(24,10)),
        col("imp_ees_cv_unit").cast(DecimalType(24,10)),
        col("imp_ees_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas").cast(DecimalType(24,10)),
        col("imp_vvdd_ing_net").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mat_prim").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_vvdd_mb").cast(DecimalType(24,10)),
        col("imp_vvdd_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_unit").cast(DecimalType(24,10)),
        col("imp_total_ventas").cast(DecimalType(24,10)),
        col("imp_tot_mm_ing_net").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_m_prim").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_adt").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_desc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb").cast(DecimalType(24,10)),
        col("imp_tot_mm_cost_log_dist_2_tsp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_unit").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_costes_almacen").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_costes_efecto_almacen").cast(DecimalType(24,10)),
        col("imp_tot_otros_costes_almacen").cast(DecimalType(24,10)),
        col("imp_tot_otros_costes_efecto_almacen").cast(DecimalType(24,10)),
        col("imp_tot_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc_2_gdirec_mc").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_g_direc").cast(DecimalType(24,10))
        ).where(col("id_escenario") === "2")

    val stack = "stack(52,".concat(
        "'13', '2', imp_ees_ventas, 'imp_ees_ventas', '20492049_01', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m','20492049_01', '', '', ''," +
        "'13', '6', imp_ees_mc, 'imp_ees_mc','20492049_01', '', '', ''," +
        "'13', '34', imp_ees_ing_brut, 'imp_ees_ing_brut','20492049_01', '', '', ''," +
        "'13', '35', imp_ees_IEPS, 'imp_ees_IEPS','20492049_01', '', '', ''," +
        "'13', '36', imp_ees_ing_net, 'imp_ees_ing_net','20492049_01', '', '', ''," +
        "'13', '37', imp_ees_coste_prod_2_mat_prima, 'imp_ees_coste_prod_2_mat_prima', '20492049_01', '', '', '1'," +
        "'13', '37', imp_ees_coste_prod_2_aditivos, 'imp_ees_coste_prod_2_aditivos', '20492049_01', '', '', '2'," +
        "'13', '37', imp_ees_coste_prod_2_mermas, 'imp_ees_coste_prod_2_mermas', '20492049_01', '', '', '3'," +
        "'13', '37', imp_ees_coste_prod_2_desc_comp, 'imp_ees_coste_prod_2_desc_comp', '20492049_01', '', '', '4'," +
        "'13', '38', imp_ees_mb, 'imp_ees_mb', '20492049_01', '', '', ''," +
        "'13', '39', imp_ees_cost_log_dist_2_transp, 'imp_ees_cost_log_dist_2_transp', '20492049_01', '', '', '5'," +
        "'13', '40', imp_ees_coste_canal, 'imp_ees_coste_canal', '20492049_01', '', '', ''," +
        "'13', '41', imp_ees_extram_olstor, 'imp_ees_extram_olstor', '20492049_01', '', '', ''," +
        "'13', '42', imp_ees_extram_monterra, 'imp_ees_extram_monterra', '20492049_01', '', '', ''," +
        "'13', '43', imp_ees_mb_sin_des_ing_c_prima, 'imp_ees_mb_sin_des_ing_c_prima', '20492049_01', '', '', ''," +
        "'13', '44', imp_ees_cv_tot, 'imp_ees_cv_tot', '20492049_01', '', '', ''," +
        "'13', '45', imp_ees_cv_unit, 'imp_ees_cv_unit', '20492049_01', '', '', ''," +
        "'13', '2', imp_vvdd_ventas, 'imp_vvdd_ventas', '20492049_02', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m', '20492049_02', '', '', ''," +
        "'13', '36', imp_vvdd_ing_net, 'imp_vvdd_ing_net', '20492049_02', '', '', ''," +
        "'13', '37', imp_vvdd_coste_prod_2_mat_prim, 'imp_vvdd_coste_prod_2_mat_prim', '20492049_02', '', '', '1'," +
        "'13', '37', imp_vvdd_coste_prod_2_mermas, 'imp_vvdd_coste_prod_2_mermas', '20492049_02', '', '', '3'," +
        "'13', '38', imp_vvdd_mb, 'imp_vvdd_mb', '20492049_02', '', '', ''," +
        "'13', '6', imp_vvdd_mc, 'imp_vvdd_mc', '20492049_02', '', '', ''," +
        "'13', '44', imp_vvdd_cv_tot, 'imp_vvdd_cv_tot', '20492049_02', '', '', ''," +
        "'13', '45', imp_vvdd_cv_unit, 'imp_vvdd_cv_unit', '20492049_02', '', '', ''," +
        "'13', '2', imp_total_ventas, 'imp_total_ventas', '20502050_01', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m', '20502050_01', '', '', ''," +
        "'13', '6', imp_tot_mm_mc, 'imp_tot_mm_mc', '20502050_01', '', '', ''," +
        "'13', '36', imp_tot_mm_ing_net, 'imp_tot_mm_ing_net', '20502050_01', '', '', ''," +
        "'13', '37', imp_tot_mm_coste_prod_2_m_prim, 'imp_tot_mm_coste_prod_2_m_prim', '20502050_01', '', '', '1'," +
        "'13', '37', imp_tot_mm_coste_prod_2_adt, 'imp_tot_mm_coste_prod_2_adt', '20502050_01', '', '', '2'," +
        "'13', '37', imp_tot_mm_coste_prod_2_mermas, 'imp_tot_mm_coste_prod_2_mermas', '20502050_01', '', '', '3'," +
        "'13', '37', imp_tot_mm_coste_prod_2_desc, 'imp_tot_mm_coste_prod_2_desc', '20502050_01', '', '', '4'," +
        "'13', '38', imp_tot_mm_mb, 'imp_tot_mm_mb', '20502050_01', '', '', ''," +
        "'13', '39', imp_tot_mm_cost_log_dist_2_tsp, 'imp_tot_mm_cost_log_dist_2_tsp', '20502050_01', '', '', '5'," +
        "'13', '40', imp_ees_coste_canal, 'imp_ees_coste_canal', '20502050_01', '', '', ''," +
        "'13', '41', imp_ees_extram_olstor, 'imp_ees_extram_olstor', '20502050_01', '', '', ''," +
        "'13', '42', imp_ees_extram_monterra, 'imp_ees_extram_monterra', '20502050_01', '', '', ''," +
        "'13', '43', imp_ees_mb_sin_des_ing_c_prima, 'imp_ees_mb_sin_des_ing_c_prima', '20502050_01', '', '', ''," +
        "'13', '44', imp_tot_mm_cv_tot, 'imp_tot_mm_cv_tot', '20502050_01', '', '', ''," +
        "'13', '45', imp_tot_mm_cv_unit, 'imp_tot_mm_cv_unit', '20502050_01', '', '', ''," +
        "'13', '46', imp_vvdd_otros_costes_almacen, 'imp_vvdd_otros_costes_almacen', '20492049_02', '', '', '6'," +
        "'13', '46', imp_vvdd_otros_costes_efecto_almacen, 'imp_vvdd_otros_costes_efecto_almacen', '20492049_02', '', '', '7'," +
        "'13', '46', imp_ees_mc_ees_2_ef_res_estrat, 'imp_ees_mc_ees_2_ef_res_estrat', '20492049_01', '', '', '8'," +
        "'13', '46', imp_tot_otros_costes_almacen, 'imp_tot_otros_costes_almacen', '20502050_01', '', '', '6'," +
        "'13', '46', imp_tot_otros_costes_efecto_almacen, 'imp_tot_otros_costes_efecto_almacen', '20502050_01', '', '', '7'," +

        "'13', '47', imp_ees_otros_mc, 'imp_ees_otros_mc', '20492049_01_02', '', '', ''," +
        "'13', '47', imp_ees_otros_mc_2_gdirec_mc, 'imp_ees_otros_mc_2_gdirec_mc', '20492049_01', '', '', ''," +
        "'13', '47', imp_tot_mm_otro_mc_2_g_direc, 'imp_tot_mm_otro_mc_2_g_direc', '20502050_01', '', '', ''," +

        "'13', '46', imp_tot_mc_ees_2_ef_res_estrat, 'imp_tot_mc_ees_2_ef_res_estrat', '20502050_01', '', '', '8')" +
        " as (ID_Informe, ID_concepto, importe, nombre, id_negocio, id_dim1, id_dim2, id_tipocoste)")

    vc_pa = df_pa
        .select(
            expr(stack)
            ,lit("").as("id_sociedad") //vacia
            ,col("cod_periodo").as("Fecha")
            ,lit("2").as("ID_Agregacion") // todos 2
            ,col("imp_ctral_Tcambio_MXN_EUR_ac").as("TasaAC")
            ,col("imp_ctral_Tcambio_MXN_EUR_m").as("TasaM")
            ,lit("2").as("ID_Escenario")
        ).select("ID_Informe", "ID_concepto", "ID_Negocio", "ID_Sociedad","ID_Agregacion","ID_Dim1" , "ID_Dim2", "id_tipocoste", "ID_Escenario", "Importe","nombre", "Fecha", "TasaAC", "TasaM").filter($"Importe".isNotNull).cache
}


# In[72]:


var vc_real = spark.emptyDataFrame

if (escenario == "REAL" || escenario == "ALL"){
    var df_real = df_kpis.select(    col("cod_periodo").cast(DecimalType(24,10)),
        col("imp_ees_ventas").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_ing_brut").cast(DecimalType(24,10)),
        col("imp_ees_IEPS").cast(DecimalType(24,10)),
        col("imp_ees_ing_net").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mat_prima").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_aditivos").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_desc_comp").cast(DecimalType(24,10)),
        col("imp_ees_mb").cast(DecimalType(24,10)),
        col("imp_ees_cost_log_dist_2_transp").cast(DecimalType(24,10)),
        col("imp_ees_coste_canal").cast(DecimalType(24,10)),
        col("imp_ees_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_extram_monterra").cast(DecimalType(24,10)),
        col("imp_ees_mb_sin_des_ing_c_prima").cast(DecimalType(24,10)),
        col("imp_ees_cv_tot").cast(DecimalType(24,10)),
        col("imp_ees_cv_unit").cast(DecimalType(24,10)),
        col("imp_ees_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas").cast(DecimalType(24,10)),
        col("imp_vvdd_ing_net").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mat_prim").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_vvdd_mb").cast(DecimalType(24,10)),
        col("imp_vvdd_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_unit").cast(DecimalType(24,10)),
        col("imp_total_ventas").cast(DecimalType(24,10)),
        col("imp_tot_mm_ing_net").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_m_prim").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_adt").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_desc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb").cast(DecimalType(24,10)),
        col("imp_tot_mm_cost_log_dist_2_tsp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_unit").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_costes_almacen").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_costes_efecto_almacen").cast(DecimalType(24,10)),
        col("imp_tot_otros_costes_almacen").cast(DecimalType(24,10)),
        col("imp_tot_otros_costes_efecto_almacen").cast(DecimalType(24,10)),
        col("imp_tot_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc_2_gdirec_mc").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_g_direc").cast(DecimalType(24,10))      
        ).where(col("id_escenario") === "1")



    val stack = "stack(52,".concat(
        "'13', '2', imp_ees_ventas, 'imp_ees_ventas', '20492049_01', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m','20492049_01', '', '', ''," +
        "'13', '6', imp_ees_mc, 'imp_ees_mc','20492049_01', '', '', ''," +
        "'13', '34', imp_ees_ing_brut, 'imp_ees_ing_brut','20492049_01', '', '', ''," +
        "'13', '35', imp_ees_IEPS, 'imp_ees_IEPS','20492049_01', '', '', ''," +
        "'13', '36', imp_ees_ing_net, 'imp_ees_ing_net','20492049_01', '', '', ''," +
        "'13', '37', imp_ees_coste_prod_2_mat_prima, 'imp_ees_coste_prod_2_mat_prima', '20492049_01', '', '', '1'," +
        "'13', '37', imp_ees_coste_prod_2_aditivos, 'imp_ees_coste_prod_2_aditivos', '20492049_01', '', '', '2'," +
        "'13', '37', imp_ees_coste_prod_2_mermas, 'imp_ees_coste_prod_2_mermas', '20492049_01', '', '', '3'," +
        "'13', '37', imp_ees_coste_prod_2_desc_comp, 'imp_ees_coste_prod_2_desc_comp', '20492049_01', '', '', '4'," +
        "'13', '38', imp_ees_mb, 'imp_ees_mb', '20492049_01', '', '', ''," +
        "'13', '39', imp_ees_cost_log_dist_2_transp, 'imp_ees_cost_log_dist_2_transp', '20492049_01', '', '', '5'," +
        "'13', '40', imp_ees_coste_canal, 'imp_ees_coste_canal', '20492049_01', '', '', ''," +
        "'13', '41', imp_ees_extram_olstor, 'imp_ees_extram_olstor', '20492049_01', '', '', ''," +
        "'13', '42', imp_ees_extram_monterra, 'imp_ees_extram_monterra', '20492049_01', '', '', ''," +
        "'13', '43', imp_ees_mb_sin_des_ing_c_prima, 'imp_ees_mb_sin_des_ing_c_prima', '20492049_01', '', '', ''," +
        "'13', '44', imp_ees_cv_tot, 'imp_ees_cv_tot', '20492049_01', '', '', ''," +
        "'13', '45', imp_ees_cv_unit, 'imp_ees_cv_unit', '20492049_01', '', '', ''," +
        "'13', '2', imp_vvdd_ventas, 'imp_vvdd_ventas', '20492049_02', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m', '20492049_02', '', '', ''," +
        "'13', '36', imp_vvdd_ing_net, 'imp_vvdd_ing_net', '20492049_02', '', '', ''," +
        "'13', '37', imp_vvdd_coste_prod_2_mat_prim, 'imp_vvdd_coste_prod_2_mat_prim', '20492049_02', '', '', '1'," +
        "'13', '37', imp_vvdd_coste_prod_2_mermas, 'imp_vvdd_coste_prod_2_mermas', '20492049_02', '', '', '3'," +
        "'13', '38', imp_vvdd_mb, 'imp_vvdd_mb', '20492049_02', '', '', ''," +
        "'13', '6', imp_vvdd_mc, 'imp_vvdd_mc', '20492049_02', '', '', ''," +
        "'13', '44', imp_vvdd_cv_tot, 'imp_vvdd_cv_tot', '20492049_02', '', '', ''," +
        "'13', '45', imp_vvdd_cv_unit, 'imp_vvdd_cv_unit', '20492049_02', '', '', ''," +
        "'13', '2', imp_total_ventas, 'imp_total_ventas', '20502050_01', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m', '20502050_01', '', '', ''," +
        "'13', '6', imp_tot_mm_mc, 'imp_tot_mm_mc', '20502050_01', '', '', ''," +
        "'13', '36', imp_tot_mm_ing_net, 'imp_tot_mm_ing_net', '20502050_01', '', '', ''," +
        "'13', '37', imp_tot_mm_coste_prod_2_m_prim, 'imp_tot_mm_coste_prod_2_m_prim', '20502050_01', '', '', '1'," +
        "'13', '37', imp_tot_mm_coste_prod_2_adt, 'imp_tot_mm_coste_prod_2_adt', '20502050_01', '', '', '2'," +
        "'13', '37', imp_tot_mm_coste_prod_2_mermas, 'imp_tot_mm_coste_prod_2_mermas', '20502050_01', '', '', '3'," +
        "'13', '37', imp_tot_mm_coste_prod_2_desc, 'imp_tot_mm_coste_prod_2_desc', '20502050_01', '', '', '4'," +
        "'13', '38', imp_tot_mm_mb, 'imp_tot_mm_mb', '20502050_01', '', '', ''," +
        "'13', '39', imp_tot_mm_cost_log_dist_2_tsp, 'imp_tot_mm_cost_log_dist_2_tsp', '20502050_01', '', '', '5'," +
        "'13', '40', imp_ees_coste_canal, 'imp_ees_coste_canal', '20502050_01', '', '', ''," +
        "'13', '41', imp_ees_extram_olstor, 'imp_ees_extram_olstor', '20502050_01', '', '', ''," +
        "'13', '42', imp_ees_extram_monterra, 'imp_ees_extram_monterra', '20502050_01', '', '', ''," +
        "'13', '43', imp_ees_mb_sin_des_ing_c_prima, 'imp_ees_mb_sin_des_ing_c_prima', '20502050_01', '', '', ''," +
        "'13', '44', imp_tot_mm_cv_tot, 'imp_tot_mm_cv_tot', '20502050_01', '', '', ''," +
        "'13', '45', imp_tot_mm_cv_unit, 'imp_tot_mm_cv_unit', '20502050_01', '', '', ''," +
        "'13', '46', imp_vvdd_otros_costes_almacen, 'imp_vvdd_otros_costes_almacen', '20492049_02', '', '', '6'," +
        "'13', '46', imp_vvdd_otros_costes_efecto_almacen, 'imp_vvdd_otros_costes_efecto_almacen', '20492049_02', '', '', '7'," +
        "'13', '46', imp_ees_mc_ees_2_ef_res_estrat, 'imp_ees_mc_ees_2_ef_res_estrat', '20492049_01', '', '', '8'," +
        "'13', '46', imp_tot_otros_costes_almacen, 'imp_tot_otros_costes_almacen', '20502050_01', '', '', '6'," +
        "'13', '46', imp_tot_otros_costes_efecto_almacen, 'imp_tot_otros_costes_efecto_almacen', '20502050_01', '', '', '7'," +
        "'13', '47', imp_ees_otros_mc, 'imp_ees_otros_mc', '20492049_01_02', '', '', ''," +
        "'13', '47', imp_ees_otros_mc_2_gdirec_mc, 'imp_ees_otros_mc_2_gdirec_mc', '20492049_01', '', '', ''," +
        "'13', '47', imp_tot_mm_otro_mc_2_g_direc, 'imp_tot_mm_otro_mc_2_g_direc', '20502050_01', '', '', ''," +
        "'13', '46', imp_tot_mc_ees_2_ef_res_estrat, 'imp_tot_mc_ees_2_ef_res_estrat', '20502050_01', '', '', '8')" +
        " as (ID_Informe, ID_concepto, importe, nombre, id_negocio, id_dim1, id_dim2, id_tipocoste)")

    vc_real = df_real
        .select(
            expr(stack)
            ,lit("").as("id_sociedad") //vacia
            ,col("cod_periodo").as("Fecha")
            ,lit("2").as("ID_Agregacion") // todos 2
            ,col("imp_ctral_Tcambio_MXN_EUR_ac").as("TasaAC")
            ,col("imp_ctral_Tcambio_MXN_EUR_m").as("TasaM")
            ,lit("1").as("ID_Escenario")
        ).select("ID_Informe", "ID_concepto", "ID_Negocio", "ID_Sociedad","ID_Agregacion","ID_Dim1" , "ID_Dim2", "id_tipocoste", "ID_Escenario", "Importe", "nombre", "Fecha", "TasaAC", "TasaM").filter($"Importe".isNotNull).cache
}


# In[73]:


var vc_est = spark.emptyDataFrame

if (escenario == "EST" || escenario == "ALL"){
    var df_est = df_kpis.select(
            col("cod_periodo").cast(DecimalType(24,10)),
        col("imp_ees_ventas").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_ing_brut").cast(DecimalType(24,10)),
        col("imp_ees_IEPS").cast(DecimalType(24,10)),
        col("imp_ees_ing_net").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mat_prima").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_aditivos").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_desc_comp").cast(DecimalType(24,10)),
        col("imp_ees_mb").cast(DecimalType(24,10)),
        col("imp_ees_cost_log_dist_2_transp").cast(DecimalType(24,10)),
        col("imp_ees_coste_canal").cast(DecimalType(24,10)),
        col("imp_ees_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_extram_monterra").cast(DecimalType(24,10)),
        col("imp_ees_mb_sin_des_ing_c_prima").cast(DecimalType(24,10)),
        col("imp_ees_cv_tot").cast(DecimalType(24,10)),
        col("imp_ees_cv_unit").cast(DecimalType(24,10)),
        col("imp_ees_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas").cast(DecimalType(24,10)),
        col("imp_vvdd_ing_net").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mat_prim").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_vvdd_mb").cast(DecimalType(24,10)),
        col("imp_vvdd_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_unit").cast(DecimalType(24,10)),
        col("imp_total_ventas").cast(DecimalType(24,10)),
        col("imp_tot_mm_ing_net").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_m_prim").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_adt").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_desc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb").cast(DecimalType(24,10)),
        col("imp_tot_mm_cost_log_dist_2_tsp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_unit").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_costes_almacen").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_costes_efecto_almacen").cast(DecimalType(24,10)),
        col("imp_tot_otros_costes_almacen").cast(DecimalType(24,10)),
        col("imp_tot_otros_costes_efecto_almacen").cast(DecimalType(24,10)),
        col("imp_tot_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc_2_gdirec_mc").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_g_direc").cast(DecimalType(24,10))      
        ).where(col("id_escenario") === "4")

    val stack = "stack(52,".concat(
        "'13', '2', imp_ees_ventas, 'imp_ees_ventas', '20492049_01', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m','20492049_01', '', '', ''," +
        "'13', '6', imp_ees_mc, 'imp_ees_mc','20492049_01', '', '', ''," +
        "'13', '34', imp_ees_ing_brut, 'imp_ees_ing_brut','20492049_01', '', '', ''," +
        "'13', '35', imp_ees_IEPS, 'imp_ees_IEPS','20492049_01', '', '', ''," +
        "'13', '36', imp_ees_ing_net, 'imp_ees_ing_net','20492049_01', '', '', ''," +
        "'13', '37', imp_ees_coste_prod_2_mat_prima, 'imp_ees_coste_prod_2_mat_prima', '20492049_01', '', '', '1'," +
        "'13', '37', imp_ees_coste_prod_2_aditivos, 'imp_ees_coste_prod_2_aditivos', '20492049_01', '', '', '2'," +
        "'13', '37', imp_ees_coste_prod_2_mermas, 'imp_ees_coste_prod_2_mermas', '20492049_01', '', '', '3'," +
        "'13', '37', imp_ees_coste_prod_2_desc_comp, 'imp_ees_coste_prod_2_desc_comp', '20492049_01', '', '', '4'," +
        "'13', '38', imp_ees_mb, 'imp_ees_mb', '20492049_01', '', '', ''," +
        "'13', '39', imp_ees_cost_log_dist_2_transp, 'imp_ees_cost_log_dist_2_transp', '20492049_01', '', '', '5'," +
        "'13', '40', imp_ees_coste_canal, 'imp_ees_coste_canal', '20492049_01', '', '', ''," +
        "'13', '41', imp_ees_extram_olstor, 'imp_ees_extram_olstor', '20492049_01', '', '', ''," +
        "'13', '42', imp_ees_extram_monterra, 'imp_ees_extram_monterra', '20492049_01', '', '', ''," +
        "'13', '43', imp_ees_mb_sin_des_ing_c_prima, 'imp_ees_mb_sin_des_ing_c_prima', '20492049_01', '', '', ''," +
        "'13', '44', imp_ees_cv_tot, 'imp_ees_cv_tot', '20492049_01', '', '', ''," +
        "'13', '45', imp_ees_cv_unit, 'imp_ees_cv_unit', '20492049_01', '', '', ''," +
        "'13', '2', imp_vvdd_ventas, 'imp_vvdd_ventas', '20492049_02', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m', '20492049_02', '', '', ''," +
        "'13', '36', imp_vvdd_ing_net, 'imp_vvdd_ing_net', '20492049_02', '', '', ''," +
        "'13', '37', imp_vvdd_coste_prod_2_mat_prim, 'imp_vvdd_coste_prod_2_mat_prim', '20492049_02', '', '', '1'," +
        "'13', '37', imp_vvdd_coste_prod_2_mermas, 'imp_vvdd_coste_prod_2_mermas', '20492049_02', '', '', '3'," +
        "'13', '38', imp_vvdd_mb, 'imp_vvdd_mb', '20492049_02', '', '', ''," +
        "'13', '6', imp_vvdd_mc, 'imp_vvdd_mc', '20492049_02', '', '', ''," +
        "'13', '44', imp_vvdd_cv_tot, 'imp_vvdd_cv_tot', '20492049_02', '', '', ''," +
        "'13', '45', imp_vvdd_cv_unit, 'imp_vvdd_cv_unit', '20492049_02', '', '', ''," +
        "'13', '2', imp_total_ventas, 'imp_total_ventas', '20502050_01', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m', '20502050_01', '', '', ''," +
        "'13', '6', imp_tot_mm_mc, 'imp_tot_mm_mc', '20502050_01', '', '', ''," +
        "'13', '36', imp_tot_mm_ing_net, 'imp_tot_mm_ing_net', '20502050_01', '', '', ''," +
        "'13', '37', imp_tot_mm_coste_prod_2_m_prim, 'imp_tot_mm_coste_prod_2_m_prim', '20502050_01', '', '', '1'," +
        "'13', '37', imp_tot_mm_coste_prod_2_adt, 'imp_tot_mm_coste_prod_2_adt', '20502050_01', '', '', '2'," +
        "'13', '37', imp_tot_mm_coste_prod_2_mermas, 'imp_tot_mm_coste_prod_2_mermas', '20502050_01', '', '', '3'," +
        "'13', '37', imp_tot_mm_coste_prod_2_desc, 'imp_tot_mm_coste_prod_2_desc', '20502050_01', '', '', '4'," +
        "'13', '38', imp_tot_mm_mb, 'imp_tot_mm_mb', '20502050_01', '', '', ''," +
        "'13', '39', imp_tot_mm_cost_log_dist_2_tsp, 'imp_tot_mm_cost_log_dist_2_tsp', '20502050_01', '', '', '5'," +
        "'13', '40', imp_ees_coste_canal, 'imp_ees_coste_canal', '20502050_01', '', '', ''," +
        "'13', '41', imp_ees_extram_olstor, 'imp_ees_extram_olstor', '20502050_01', '', '', ''," +
        "'13', '42', imp_ees_extram_monterra, 'imp_ees_extram_monterra', '20502050_01', '', '', ''," +
        "'13', '43', imp_ees_mb_sin_des_ing_c_prima, 'imp_ees_mb_sin_des_ing_c_prima', '20502050_01', '', '', ''," +
        "'13', '44', imp_tot_mm_cv_tot, 'imp_tot_mm_cv_tot', '20502050_01', '', '', ''," +
        "'13', '45', imp_tot_mm_cv_unit, 'imp_tot_mm_cv_unit', '20502050_01', '', '', ''," +
        "'13', '46', imp_vvdd_otros_costes_almacen, 'imp_vvdd_otros_costes_almacen', '20492049_02', '', '', '6'," +
        "'13', '46', imp_vvdd_otros_costes_efecto_almacen, 'imp_vvdd_otros_costes_efecto_almacen', '20492049_02', '', '', '7'," +
        "'13', '46', imp_ees_mc_ees_2_ef_res_estrat, 'imp_ees_mc_ees_2_ef_res_estrat', '20492049_01', '', '', '8'," +
        "'13', '46', imp_tot_otros_costes_almacen, 'imp_tot_otros_costes_almacen', '20502050_01', '', '', '6'," +
        "'13', '46', imp_tot_otros_costes_efecto_almacen, 'imp_tot_otros_costes_efecto_almacen', '20502050_01', '', '', '7'," +
        "'13', '47', imp_ees_otros_mc, 'imp_ees_otros_mc', '20492049_01_02', '', '', ''," +
        "'13', '47', imp_ees_otros_mc_2_gdirec_mc, 'imp_ees_otros_mc_2_gdirec_mc', '20492049_01', '', '', ''," +
        "'13', '47', imp_tot_mm_otro_mc_2_g_direc, 'imp_tot_mm_otro_mc_2_g_direc', '20502050_01', '', '', ''," +   
        "'13', '46', imp_tot_mc_ees_2_ef_res_estrat, 'imp_tot_mc_ees_2_ef_res_estrat', '20502050_01', '', '', '8')" +        
        " as (ID_Informe, ID_concepto, importe, nombre, id_negocio, id_dim1, id_dim2, id_tipocoste)")

    vc_est = df_est
        .select(
            expr(stack)
            ,lit("").as("id_sociedad") //vacia
            ,col("cod_periodo").as("Fecha")
            ,lit("2").as("ID_Agregacion") // todos 2
            ,col("imp_ctral_Tcambio_MXN_EUR_ac").as("TasaAC")
            ,col("imp_ctral_Tcambio_MXN_EUR_m").as("TasaM")
            ,lit("4").as("ID_Escenario")
        ).select("ID_Informe", "ID_concepto", "ID_Negocio", "ID_Sociedad","ID_Agregacion","ID_Dim1" , "ID_Dim2", "id_tipocoste", "ID_Escenario", "Importe", "nombre", "Fecha", "TasaAC", "TasaM").filter($"Importe".isNotNull).cache
}


# In[74]:


var vc_upa = spark.emptyDataFrame

if (escenario == "PA-UPA" || escenario == "ALL" || escenario == "UPA"){
    var df_upa = df_kpis.select(
            col("cod_periodo").cast(DecimalType(24,10)),
        col("imp_ees_ventas").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_ing_brut").cast(DecimalType(24,10)),
        col("imp_ees_IEPS").cast(DecimalType(24,10)),
        col("imp_ees_ing_net").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mat_prima").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_aditivos").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_desc_comp").cast(DecimalType(24,10)),
        col("imp_ees_mb").cast(DecimalType(24,10)),
        col("imp_ees_cost_log_dist_2_transp").cast(DecimalType(24,10)),
        col("imp_ees_coste_canal").cast(DecimalType(24,10)),
        col("imp_ees_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_extram_monterra").cast(DecimalType(24,10)),
        col("imp_ees_mb_sin_des_ing_c_prima").cast(DecimalType(24,10)),
        col("imp_ees_cv_tot").cast(DecimalType(24,10)),
        col("imp_ees_cv_unit").cast(DecimalType(24,10)),
        col("imp_ees_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas").cast(DecimalType(24,10)),
        col("imp_vvdd_ing_net").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mat_prim").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_vvdd_mb").cast(DecimalType(24,10)),
        col("imp_vvdd_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_unit").cast(DecimalType(24,10)),
        col("imp_total_ventas").cast(DecimalType(24,10)),
        col("imp_tot_mm_ing_net").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_m_prim").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_adt").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_desc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb").cast(DecimalType(24,10)),
        col("imp_tot_mm_cost_log_dist_2_tsp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_unit").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_costes_almacen").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_tot_otros_costes_almacen").cast(DecimalType(24,10)),
        col("imp_tot_otros_costes_efecto_almacen").cast(DecimalType(24,10)),
        col("imp_tot_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),        
        col("imp_vvdd_otros_costes_efecto_almacen").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc_2_gdirec_mc").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_g_direc").cast(DecimalType(24,10)) 
        )
        .where(col("id_escenario") === "3")



    val stack = "stack(52,".concat(
        "'13', '2', imp_ees_ventas, 'imp_ees_ventas', '20492049_01', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m','20492049_01', '', '', ''," +
        "'13', '6', imp_ees_mc, 'imp_ees_mc','20492049_01', '', '', ''," +
        "'13', '34', imp_ees_ing_brut, 'imp_ees_ing_brut','20492049_01', '', '', ''," +
        "'13', '35', imp_ees_IEPS, 'imp_ees_IEPS','20492049_01', '', '', ''," +
        "'13', '36', imp_ees_ing_net, 'imp_ees_ing_net','20492049_01', '', '', ''," +
        "'13', '37', imp_ees_coste_prod_2_mat_prima, 'imp_ees_coste_prod_2_mat_prima', '20492049_01', '', '', '1'," +
        "'13', '37', imp_ees_coste_prod_2_aditivos, 'imp_ees_coste_prod_2_aditivos', '20492049_01', '', '', '2'," +
        "'13', '37', imp_ees_coste_prod_2_mermas, 'imp_ees_coste_prod_2_mermas', '20492049_01', '', '', '3'," +
        "'13', '37', imp_ees_coste_prod_2_desc_comp, 'imp_ees_coste_prod_2_desc_comp', '20492049_01', '', '', '4'," +
        "'13', '38', imp_ees_mb, 'imp_ees_mb', '20492049_01', '', '', ''," +
        "'13', '39', imp_ees_cost_log_dist_2_transp, 'imp_ees_cost_log_dist_2_transp', '20492049_01', '', '', '5'," +
        "'13', '40', imp_ees_coste_canal, 'imp_ees_coste_canal', '20492049_01', '', '', ''," +
        "'13', '41', imp_ees_extram_olstor, 'imp_ees_extram_olstor', '20492049_01', '', '', ''," +
        "'13', '42', imp_ees_extram_monterra, 'imp_ees_extram_monterra', '20492049_01', '', '', ''," +
        "'13', '43', imp_ees_mb_sin_des_ing_c_prima, 'imp_ees_mb_sin_des_ing_c_prima', '20492049_01', '', '', ''," +
        "'13', '44', imp_ees_cv_tot, 'imp_ees_cv_tot', '20492049_01', '', '', ''," +
        "'13', '45', imp_ees_cv_unit, 'imp_ees_cv_unit', '20492049_01', '', '', ''," +
        "'13', '2', imp_vvdd_ventas, 'imp_vvdd_ventas', '20492049_02', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m', '20492049_02', '', '', ''," +
        "'13', '36', imp_vvdd_ing_net, 'imp_vvdd_ing_net', '20492049_02', '', '', ''," +
        "'13', '37', imp_vvdd_coste_prod_2_mat_prim, 'imp_vvdd_coste_prod_2_mat_prim', '20492049_02', '', '', '1'," +
        "'13', '37', imp_vvdd_coste_prod_2_mermas, 'imp_vvdd_coste_prod_2_mermas', '20492049_02', '', '', '3'," +
        "'13', '38', imp_vvdd_mb, 'imp_vvdd_mb', '20492049_02', '', '', ''," +
        "'13', '6', imp_vvdd_mc, 'imp_vvdd_mc', '20492049_02', '', '', ''," +
        "'13', '44', imp_vvdd_cv_tot, 'imp_vvdd_cv_tot', '20492049_02', '', '', ''," +
        "'13', '45', imp_vvdd_cv_unit, 'imp_vvdd_cv_unit', '20492049_02', '', '', ''," +
        "'13', '2', imp_total_ventas, 'imp_total_ventas', '20502050_01', '', '', ''," +
        "'13', '1', imp_ctral_Tcambio_MXN_EUR_m, 'imp_ctral_Tcambio_MXN_EUR_m', '20502050_01', '', '', ''," +
        "'13', '6', imp_tot_mm_mc, 'imp_tot_mm_mc', '20502050_01', '', '', ''," +
        "'13', '36', imp_tot_mm_ing_net, 'imp_tot_mm_ing_net', '20502050_01', '', '', ''," +
        "'13', '37', imp_tot_mm_coste_prod_2_m_prim, 'imp_tot_mm_coste_prod_2_m_prim', '20502050_01', '', '', '1'," +
        "'13', '37', imp_tot_mm_coste_prod_2_adt, 'imp_tot_mm_coste_prod_2_adt', '20502050_01', '', '', '2'," +
        "'13', '37', imp_tot_mm_coste_prod_2_mermas, 'imp_tot_mm_coste_prod_2_mermas', '20502050_01', '', '', '3'," +
        "'13', '37', imp_tot_mm_coste_prod_2_desc, 'imp_tot_mm_coste_prod_2_desc', '20502050_01', '', '', '4'," +
        "'13', '38', imp_tot_mm_mb, 'imp_tot_mm_mb', '20502050_01', '', '', ''," +
        "'13', '39', imp_tot_mm_cost_log_dist_2_tsp, 'imp_tot_mm_cost_log_dist_2_tsp', '20502050_01', '', '', '5'," +
        "'13', '40', imp_ees_coste_canal, 'imp_ees_coste_canal', '20502050_01', '', '', ''," +
        "'13', '41', imp_ees_extram_olstor, 'imp_ees_extram_olstor', '20502050_01', '', '', ''," +
        "'13', '42', imp_ees_extram_monterra, 'imp_ees_extram_monterra', '20502050_01', '', '', ''," +
        "'13', '43', imp_ees_mb_sin_des_ing_c_prima, 'imp_ees_mb_sin_des_ing_c_prima', '20502050_01', '', '', ''," +
        "'13', '44', imp_tot_mm_cv_tot, 'imp_tot_mm_cv_tot', '20502050_01', '', '', ''," +
        "'13', '45', imp_tot_mm_cv_unit, 'imp_tot_mm_cv_unit', '20502050_01', '', '', ''," +
        "'13', '46', imp_vvdd_otros_costes_almacen, 'imp_vvdd_otros_costes_almacen', '20492049_02', '', '', '6'," +
        "'13', '46', imp_vvdd_otros_costes_efecto_almacen, 'imp_vvdd_otros_costes_efecto_almacen', '20492049_02', '', '', '7'," +
        "'13', '46', imp_ees_mc_ees_2_ef_res_estrat, 'imp_ees_mc_ees_2_ef_res_estrat', '20492049_01', '', '', '8'," +
        "'13', '46', imp_tot_otros_costes_almacen, 'imp_tot_otros_costes_almacen', '20502050_01', '', '', '6'," +
        "'13', '46', imp_tot_otros_costes_efecto_almacen, 'imp_tot_otros_costes_efecto_almacen', '20502050_01', '', '', '7'," +
        "'13', '47', imp_ees_otros_mc, 'imp_ees_otros_mc', '20492049_01_02', '', '', ''," +
        "'13', '47', imp_ees_otros_mc_2_gdirec_mc, 'imp_ees_otros_mc_2_gdirec_mc', '20492049_01', '', '', ''," +
        "'13', '47', imp_tot_mm_otro_mc_2_g_direc, 'imp_tot_mm_otro_mc_2_g_direc', '20502050_01', '', '', ''," +
        "'13', '46', imp_tot_mc_ees_2_ef_res_estrat, 'imp_tot_mc_ees_2_ef_res_estrat', '20502050_01', '', '', '8')" +        
        " as (ID_Informe, ID_concepto, importe, nombre, id_negocio, id_dim1, id_dim2, id_tipocoste)")

    vc_upa = df_upa
        .select(
            expr(stack)
            ,lit("").as("id_sociedad") //vacia
            ,col("cod_periodo").as("Fecha")
            ,lit("2").as("ID_Agregacion") // todos 2
            ,col("imp_ctral_Tcambio_MXN_EUR_m").as("TasaAC")
            ,col("imp_ctral_Tcambio_MXN_EUR_m").as("TasaM")
            ,lit("3").as("ID_Escenario")
        ).select("ID_Informe", "ID_concepto", "ID_Negocio", "ID_Sociedad","ID_Agregacion","ID_Dim1" , "ID_Dim2", "id_tipocoste", "ID_Escenario", "Importe", "nombre", "Fecha", "TasaAC", "TasaM").filter($"Importe".isNotNull).cache
}


# In[75]:


var vc_final = vc_est.unionByName(vc_upa,true).unionByName(vc_pa,true).unionByName(vc_real,true).cache


# In[76]:


var vc_final1 = vc_final.withColumn("val_divisa", lit("Pesos"))


# In[77]:


var b = vc_final1.select(
    col("ID_Informe").cast(IntegerType).as("id_informe"),
    col("ID_concepto").cast(IntegerType).as("id_concepto"),
    col("ID_Negocio").cast(VarcharType(30)).as("id_negocio"),
    col("ID_Sociedad").cast(IntegerType).as("id_sociedad"),
    col("ID_Agregacion").cast(IntegerType).as("id_tipodato"),
    col("ID_Dim1").cast(IntegerType).as("id_dim1"),
    col("ID_Dim2").cast(IntegerType).as("id_dim2"),
    col("ID_Escenario").cast(IntegerType).as("id_escenario"),
    col("Importe").cast(DecimalType(24,3)).as("imp_importe"),
    col("nombre").cast(VarcharType(100)),
    col("TasaAC").cast(DecimalType(24,10)),
    col("TasaM").cast(DecimalType(24,10)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType).as("id_tipocoste"),
    col("Fecha").cast(IntegerType).as("cod_fecha")).filter($"Importe".isNotNull).cache

b.limit(1).count()


# ## ACUMULADO

# In[78]:


// ACUMULADO
var t_Pool_mex = readFromSQLPool("sch_anl","cli0010_tb_fac_m_pbi_mmexh2", token).where(col("val_divisa") === "Pesos" and col("cod_fecha") > v_periodo_ytd).withColumn("nombre", lit(0)).cache

var G_PA_UPA = spark.emptyDataFrame
var G_Real = spark.emptyDataFrame

if (escenario == "PA-UPA" || escenario == "ALL"){
    G_PA_UPA = t_Pool_mex.where(col("id_escenario").isin("2","3") && col("id_tipodato") === 2 && col("cod_fecha").lt(v_periodo)).withColumn("TasaAC", lit(0)).withColumn("TasaM", lit(0))
}else if (escenario == "PA"){
    G_PA_UPA = t_Pool_mex.where(col("id_escenario").isin("2") && col("id_tipodato") === 2 && col("cod_fecha").lt(v_periodo)).withColumn("TasaAC", lit(0)).withColumn("TasaM", lit(0))
}else if (escenario == "UPA"){
    G_PA_UPA = t_Pool_mex.where(col("id_escenario").isin("3") && col("id_tipodato") === 2 && col("cod_fecha").lt(v_periodo)).withColumn("TasaAC", lit(0)).withColumn("TasaM", lit(0))
}

if (escenario == "REAL" || escenario == "ALL"){
    G_Real = t_Pool_mex.where(col("id_escenario") === 1 && col("id_tipodato") === 2 && col("cod_fecha").lt(v_periodo))
}


# ## YTD ESTIMADOS
# 

# In[79]:


var DFSinAcumuladoEST = spark.emptyDataFrame
var DFRealLastMonth = spark.emptyDataFrame
var DFEstLastMonth = spark.emptyDataFrame

if (escenario == "EST" || escenario == "ALL"){
//Filtramos NUEVODF (TODO MENOS ACUM DE EST): Me quedo con todo - (el acumulado de estimado de la fecha actual)
    DFSinAcumuladoEST = b.filter(!($"id_escenario" === 4 and $"id_tipodato" === 1 and $"cod_fecha" === v_periodo)).cache
    DFRealLastMonth = b.filter($"id_escenario" === 1 and $"id_tipodato" === 2 and $"cod_fecha" === v_periodo).cache
    DFEstLastMonth = b.filter($"id_escenario" === 4 and $"id_tipodato" === 2 and $"cod_fecha" === v_periodo).cache
}


# In[80]:


import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

var G_EST = b.limit(0)

// SI HAY DATO DE ESTIMADO HACEMOS ACUMULADO
if(!(DFEstLastMonth.rdd.isEmpty() || DFEstLastMonth.agg(sum("imp_importe")).collectAsList().get(0).get(0).equals(0) || DFEstLastMonth.agg(sum("imp_importe")).collectAsList().get(0).isNullAt(0))){

    //YTD DE ESTIMADO
    //cargamos la tabla de la vista en un df
    var df_vista = readFromSQLPool("sch_anl","cli0010_tb_fac_m_pbi_mmexh2",token).where(col("cod_fecha") > v_periodo_ytd).cache

    //Recuperamos de la vista los datos de real hasta la fecha que tenemos de estimado y pasamos el id_escenario a estimado
    // df_pbi_real_to_estimados --> DF con todo REAL PERIODO hasta estimado y cambia el id_escenario de 1 a 4
    val string_fecha_estimados = v_periodo.toString

    //DF con la parte de real del mismo año hasta la fecha que estamos ejecutando, cambio id_escenario a 4
    val df_pbi_real_to_estimados = 
    if(!string_fecha_estimados.equals("0")){
        df_vista
        .where(col("id_escenario")===lit("1") &&
        col("id_tipodato")===lit("2") &&
        col("val_divisa") === lit("Pesos") &&
        col("cod_fecha").contains(string_fecha_estimados.substring(0,4)) && //Del mismo año
        col("cod_fecha")<string_fecha_estimados) //Hasta la fecha que estoy ejecutando (sin incluir)
        .withColumn("id_escenario",lit("4").cast(IntegerType)).cache
    }else{df_vista.limit(0)}

    //recumeramos la fecha max del df de real por si no tuvieramos todas la fechas, recuperar las que faltan de estimado
    val maxFechaReal = df_pbi_real_to_estimados.select(max("cod_fecha")).collectAsList()

    val string_max_fecha_estimados:String =
    if (!maxFechaReal.isEmpty && !maxFechaReal.get(0).isNullAt(0)) {
    maxFechaReal.get(0).get(0).toString
    } else {
    "0"
    }

    val df_pbi_estimados_auxiliar = 
    if(!string_fecha_estimados.equals("0")){
        df_vista
        .where(col("id_escenario")===lit("4") &&
        col("id_tipodato")===lit("2") &&
        col("val_divisa") === lit("Pesos") &&
        col("cod_fecha").contains(string_fecha_estimados.substring(0,4)) &&
        col("cod_fecha")> string_max_fecha_estimados && col("cod_fecha") < string_fecha_estimados ).cache //.between(string_max_fecha_estimados.toInt+100,string_fecha_estimados.toInt-100))
    }else{df_vista.limit(0)}

    G_EST = 
    if(!(DFRealLastMonth.rdd.isEmpty() || DFRealLastMonth.agg(sum("imp_importe")).collectAsList().get(0).get(0).equals(0) || DFRealLastMonth.agg(sum("imp_importe")).collectAsList().get(0).isNullAt(0))){
        df_pbi_real_to_estimados //Parte de estimado
        .unionByName(df_pbi_estimados_auxiliar,true) //Parte de real
        // .unionByName(DFRealLastMonth.withColumn("id_escenario",lit("4").cast(IntegerType)),true).cache //último mes real
    }else{
        df_pbi_real_to_estimados //Parte de estimado
        .unionByName(df_pbi_estimados_auxiliar,true) //Parte de real
        // .unionByName(DFEstLastMonth,true).cache //último mes estimado
    }

}
G_EST = G_EST.withColumn("nombre", lit(0)).cache


# ## TODO YTD
# 

# In[81]:


import org.apache.spark.sql.expressions.Window

var t_DFYTD = spark.emptyDataFrame

var t_final = spark.emptyDataFrame
if(escenario == "REAL"){
    t_final = b.select("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2","id_tipocoste","cod_fecha", "imp_importe", "val_divisa", "id_tipodato", "id_sociedad", "TasaAC", "TasaM","nombre").where(col("cod_fecha") === v_periodo)
      .unionByName(G_Real.withColumn("TasaAC", lit(0)).withColumn("TasaM", lit(0)).select("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2","id_tipocoste","cod_fecha", "imp_importe", "val_divisa", "id_tipodato", "id_sociedad", "TasaAC", "TasaM","nombre"), true)
}else if(escenario =="PA-UPA" || escenario == "PA" || escenario == "UPA"){
    t_final = b.select("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2","id_tipocoste","cod_fecha", "imp_importe", "val_divisa", "id_tipodato", "id_sociedad", "TasaAC", "TasaM","nombre").where(col("cod_fecha") === v_periodo)
      .unionByName(G_PA_UPA.withColumn("TasaAC", lit(0)).withColumn("TasaM", lit(0)).select("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2","id_tipocoste","cod_fecha", "imp_importe", "val_divisa", "id_tipodato", "id_sociedad", "TasaAC", "TasaM","nombre"), true)
}else if(escenario == "EST"){
    t_final = b.select("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2","id_tipocoste","cod_fecha", "imp_importe", "val_divisa", "id_tipodato", "id_sociedad", "TasaAC", "TasaM","nombre").where(col("cod_fecha") === v_periodo)
      .unionByName(G_EST.withColumn("TasaAC", lit(0)).withColumn("TasaM", lit(0)).select("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2","id_tipocoste","cod_fecha", "imp_importe", "val_divisa", "id_tipodato", "id_sociedad", "TasaAC", "TasaM","nombre"), true)
}else{
    t_final = b.select("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2","id_tipocoste","cod_fecha", "imp_importe", "val_divisa", "id_tipodato", "id_sociedad", "TasaAC", "TasaM","nombre").where(col("cod_fecha") === v_periodo)
      .unionByName(G_PA_UPA.withColumn("TasaAC", lit(0)).withColumn("TasaM", lit(0)).select("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2","id_tipocoste","cod_fecha", "imp_importe", "val_divisa", "id_tipodato", "id_sociedad", "TasaAC", "TasaM","nombre"), true)
      .unionByName(G_Real.withColumn("TasaAC", lit(0)).withColumn("TasaM", lit(0)).select("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2","id_tipocoste","cod_fecha", "imp_importe", "val_divisa", "id_tipodato", "id_sociedad", "TasaAC", "TasaM","nombre"), true)
      .unionByName(G_EST.withColumn("TasaAC", lit(0)).withColumn("TasaM", lit(0)).select("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2","id_tipocoste","cod_fecha", "imp_importe", "val_divisa", "id_tipodato", "id_sociedad", "TasaAC", "TasaM","nombre"), true)
}

val DFwithYear = t_final.withColumn("anio",col("cod_fecha").substr(0, 4))//.drop("nombre").drop("id_tipocoste")

val windowSpecfAgg = Window.partitionBy("anio", "id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2", "id_tipocoste").orderBy("cod_fecha")
val windowSpecfAnioAgg = Window.partitionBy("anio").orderBy("cod_fecha")

val aggDF_L = DFwithYear.withColumn("row_number",row_number.over(windowSpecfAnioAgg)).
    withColumn("imp_ytd", sum(col("imp_importe")).over(windowSpecfAgg))

t_DFYTD = aggDF_L.dropDuplicates


# In[82]:


import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

var t_DFYTD2 = t_DFYTD.select(
col("id_informe").cast(IntegerType),
col("id_concepto").cast(IntegerType),
col("id_negocio").cast(VarcharType(30)),
col("id_sociedad").cast(IntegerType),
col("id_tipodato").cast(IntegerType),
col("id_dim1").cast(IntegerType),
col("id_dim2").cast(IntegerType),
col("id_escenario").cast(IntegerType),
col("imp_importe").cast(DecimalType(24,3)),
col("nombre").cast(VarcharType(100)),
col("TasaAC").cast(DecimalType(24,10)),
col("TasaM").cast(DecimalType(24,10)),
col("val_divisa").cast(VarcharType(30)),
col("imp_ytd").cast(DecimalType(24,3)),
col("anio").cast(IntegerType),
col("row_number").cast(IntegerType),
col("id_tipocoste").cast(IntegerType),
col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull)

val unPivotDF = t_DFYTD2.selectExpr("id_sociedad","id_escenario", "cod_fecha", "id_tipodato", "id_negocio", "id_dim1", "id_dim2", "nombre", "id_tipocoste","val_divisa", "id_informe", "id_concepto", "TasaAC", "TasaM", "anio ", "row_number",
        "stack(2, 'imp_importe', imp_importe,'imp_ytd', imp_ytd) as (id_tipo_dato, imp_importe)")

val idagreColDF = unPivotDF.withColumn("id_tipodato",when(col("id_tipo_dato") === "imp_ytd", 1).otherwise(2))

var DF_ALL_YTD = idagreColDF.drop("row_number", "anio", "id_tipo_dato")
.select(
        col("id_informe").cast(IntegerType),
        col("id_concepto").cast(IntegerType),
        col("id_negocio").cast(VarcharType(30)),
        lit("").cast(IntegerType).as("id_sociedad"),
        col("id_tipodato").cast(IntegerType),
        col("id_dim1").cast(IntegerType),
        col("id_dim2").cast(IntegerType),
        col("id_escenario").cast(IntegerType),
        col("imp_importe").cast(DecimalType(24,3)),
        col("nombre").cast(VarcharType(100)),
        col("TasaAC").cast(DecimalType(24,10)),
        col("TasaM").cast(DecimalType(24,10)),
        col("val_divisa").cast(VarcharType(30)),
        col("cod_fecha").cast(IntegerType),
        col("id_tipocoste").cast(IntegerType)
).where(col("cod_fecha") === v_periodo ).cache

DF_ALL_YTD.limit(1).count()


# In[83]:


spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

val ds_output = s"cli0010_tb_aux_m_pivkpi_mmex_h2/"
val ds_output_temp = ds_output.concat("temp")
val pathWriteTemp = s"$edwPath/$business/$ds_output_temp"
val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

DF_ALL_YTD.coalesce(1).write.partitionBy("cod_fecha").mode("overwrite").format("parquet").save(parquet_path_temp)
DF_ALL_YTD.count


# In[84]:


for ((k,v) <- sc.getPersistentRDDs) {
   v.unpersist()
}

val DF_ALL = spark.read.parquet(parquet_path_temp).where(col("cod_fecha") === v_periodo ).cache


# ## DIVISAS

# In[85]:


var acumuladoEuros = DF_ALL.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    (col("imp_importe").cast(DecimalType(24,3)) / col("TasaAC")).as("imp_importe"),
    col("nombre").cast(VarcharType(100)),
    col("TasaAC").cast(DecimalType(24,10)),
    col("TasaM").cast(DecimalType(24,10)),
    lit("Euros").as("val_divisa"),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("id_tipodato") === 1)

var facPesos = DF_ALL.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,3)),
    col("nombre").cast(VarcharType(100)),
    col("TasaAC").cast(DecimalType(24,10)),
    col("TasaM").cast(DecimalType(24,10)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("val_divisa") === "Pesos")

var pesosEurosAcum = facPesos.unionByName(acumuladoEuros, true).dropDuplicates.cache


# **Euros a KEuros**

# In[86]:


var a = pesosEurosAcum.select(
    col("ID_Informe").cast(IntegerType),
    col("ID_concepto").cast(IntegerType),
    col("ID_Negocio").cast(VarcharType(30)),
    col("ID_Sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("ID_Dim1").cast(IntegerType),
    col("ID_Dim2").cast(IntegerType),
    col("ID_Escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,10)),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("val_divisa") === "Pesos").cache

var c = pesosEurosAcum.select(
    col("ID_Informe").cast(IntegerType),
    col("ID_concepto").cast(IntegerType),
    col("ID_Negocio").cast(VarcharType(30)),
    col("ID_Sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("ID_Dim1").cast(IntegerType),
    col("ID_Dim2").cast(IntegerType),
    col("ID_Escenario").cast(IntegerType),
    lit(col("imp_importe").cast(DecimalType(24,10))/1000).as("imp_importe"),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("ID_concepto") <= 8 && col("val_divisa") === "Euros").cache

var d = pesosEurosAcum.select(
    col("ID_Informe").cast(IntegerType),
    col("ID_concepto").cast(IntegerType),
    col("ID_Negocio").cast(VarcharType(30)),
    col("ID_Sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("ID_Dim1").cast(IntegerType),
    col("ID_Dim2").cast(IntegerType),
    col("ID_Escenario").cast(IntegerType),
    lit(col("imp_importe").cast(DecimalType(24,10))/1000).as("imp_importe"),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("ID_concepto") > 8 && col("val_divisa") === "Euros").cache

var h = a.union(c).union(d).select(
    col("ID_Informe").cast(IntegerType),
    col("ID_concepto").cast(IntegerType),
    col("ID_Negocio").cast(VarcharType(30)),
    col("ID_Sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("ID_Dim1").cast(IntegerType),
    col("ID_Dim2").cast(IntegerType),
    col("ID_Escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,10)),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)
).coalesce(100).cache

h.limit(1).count()


# In[87]:


var t_Pool_glp22 = spark.emptyDataFrame

if (escenario == "REAL"){
    t_Pool_glp22 = readFromSQLPool("sch_anl","cli0010_tb_fac_m_pbi_mmexh2", token).where(col("id_escenario") === 1).withColumn("nombre", lit(0)).cache
}else if(escenario == "PA-UPA"){
    t_Pool_glp22 = readFromSQLPool("sch_anl","cli0010_tb_fac_m_pbi_mmexh2", token).where(col("id_escenario").isin(2,3)).withColumn("nombre", lit(0)).cache
}else if(escenario == "EST"){
    t_Pool_glp22 = readFromSQLPool("sch_anl","cli0010_tb_fac_m_pbi_mmexh2", token).where(col("id_escenario") === 4).withColumn("nombre", lit(0)).cache
}else if(escenario == "PA"){
    t_Pool_glp22 = readFromSQLPool("sch_anl","cli0010_tb_fac_m_pbi_mmexh2", token).where(col("id_escenario").isin(2)).withColumn("nombre", lit(0)).cache
}else if(escenario == "UPA"){
    t_Pool_glp22 = readFromSQLPool("sch_anl","cli0010_tb_fac_m_pbi_mmexh2", token).where(col("id_escenario").isin(3)).withColumn("nombre", lit(0)).cache
}else{
    t_Pool_glp22 = readFromSQLPool("sch_anl","cli0010_tb_fac_m_pbi_mmexh2", token).withColumn("nombre", lit(0)).cache
}

var anios = anio * 10000
var FacEurosAcum1 = t_Pool_glp22.where(col("id_tipodato") === 1 && col("val_divisa") === "Euros" && col("cod_fecha").lt(v_periodo) && col("cod_fecha") > anios)


# In[88]:


var FacEurosAcum = FacEurosAcum1.select(
    col("ID_Informe").cast(IntegerType),
    col("ID_concepto").cast(IntegerType),
    col("ID_Negocio").cast(VarcharType(30)),
    col("ID_Sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("ID_Dim1").cast(IntegerType),
    col("ID_Dim2").cast(IntegerType),
    col("ID_Escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,10)),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)
)
var FacPesosEurosAcum = FacEurosAcum.unionByName(h, true).dropDuplicates.cache


# In[89]:


var  FacEurosAcum = t_Pool_glp22.filter( $"cod_fecha" >= v_year.toInt*10000+100+1 && $"cod_fecha" < v_periodo )
    .filter( $"id_escenario" === 1 )
    .filter( $"id_tipodato"=== 1 )
    .filter( $"val_divisa"=== "Euros" )
    .select(
        lit(4).as("ID_Escenario"),
        col("ID_Informe"),
        col("ID_concepto"),
        col("ID_Dim1"),
        col("ID_Dim2"),
        col("val_divisa"),
        col("ID_Negocio"),
        col("cod_fecha"),
        col("imp_importe"),
        col("id_tipodato"),
        col("id_tipocoste"),
        col("nombre"),
        col("ID_Sociedad")
    ).coalesce(2)

var FacEurosResto = t_Pool_glp22.filter( $"cod_fecha" >= v_year.toInt*10000+100+1 && $"cod_fecha" < v_periodo )
    .filter( $"id_escenario".notEqual(4) )
    .filter( $"id_tipodato"=== 1 )
    .filter( $"val_divisa"=== "Euros" )
    .select(
        col("ID_Escenario"),
        col("ID_Informe"),
        col("ID_concepto"),
        col("ID_Dim1"),
        col("ID_Dim2"),
        col("val_divisa"),
        col("ID_Negocio"),
        col("cod_fecha"),
        col("imp_importe"),
        col("id_tipodato"),
        col("id_tipocoste"),
        col("nombre"),
        col("ID_Sociedad")
    ).coalesce(2)

var PeriodoActual = h
    .filter( $"id_tipodato"=== 1 )
    .filter( $"val_divisa"=== "Euros" )
    .select(
        col("ID_Escenario"),
        col("ID_Informe"),
        col("ID_concepto"),
        col("ID_Dim1"),
        col("ID_Dim2"),
        col("val_divisa"),
        col("ID_Negocio"),
        col("cod_fecha"),
        col("imp_importe"),
        col("id_tipodato"),
        col("id_tipocoste"),
        col("nombre"),
        col("ID_Sociedad")
    ).coalesce(2)

var FacFinalEuros = FacEurosAcum.union(FacEurosResto).union(PeriodoActual).cache

val windowSpecfAgg = Window.partitionBy("id_escenario","id_informe","id_negocio", "id_concepto", "id_dim1","id_dim2", "val_divisa","id_tipocoste").orderBy("cod_fecha")
val resultDF = FacFinalEuros.withColumn("prevValue", lag("imp_importe", 1, 0).over(windowSpecfAgg)).cache
resultDF.limit(1).count()


# In[90]:


var resultDFF = resultDF.where(col("val_divisa") === "Euros" && col("cod_fecha") === v_periodo)

var EurosPorDiferencia = resultDFF.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    lit(2).cast(IntegerType).as("id_tipodato"),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    (col("imp_importe").cast(DecimalType(24,3)) - col("prevValue").cast(DecimalType(24,3))).as("imp_importe"),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).coalesce(2)

var FacPesosEurosAcumUnion = FacPesosEurosAcum.where(col("cod_fecha") === v_periodo)

var DFfinal = FacPesosEurosAcumUnion.unionByName(EurosPorDiferencia, true).cache


# In[91]:


var limpiezaTipoDeCambio = DFfinal.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,3)),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("id_concepto") =!= 1 && col("id_tipodato") === 1).cache

var limpiezaTipoDeCambio2 = DFfinal.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,3)),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("id_tipodato") === 2).cache

var factTipoDeCambio = limpiezaTipoDeCambio2.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    lit("1").as("id_tipodato").cast(IntegerType),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,3)),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("id_concepto") === 1).cache

var factConCambios = factTipoDeCambio.unionByName(limpiezaTipoDeCambio, true).unionByName(limpiezaTipoDeCambio2, true).cache
factConCambios.limit(1).count()


# In[92]:


var m3Pesos = factConCambios.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,3)),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("val_divisa") === "Pesos").cache

var m3Euros = factConCambios.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,3)),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("val_divisa") === "Euros" && col("id_concepto") =!= 2 && col("id_concepto") =!= 3 && col("id_concepto") =!= 4 && col("id_concepto") =!= 28 && col("id_concepto") =!= 1).cache

var m3Euros2 = m3Pesos.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,3)),
    col("nombre").cast(VarcharType(100)),
    lit("Euros").as("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("id_concepto") === 2 || col("id_concepto") === 3 || col("id_concepto") === 4 || col("id_concepto") === 28 || col("id_concepto") === 1).cache

var factConCambios7 = m3Euros.unionByName(m3Euros2, true).unionByName(m3Pesos, true).cache


# In[93]:


var factConCambios2 = factConCambios7.select(
    col("ID_Informe").cast(IntegerType),
    col("ID_concepto").cast(IntegerType),
    col("ID_Negocio").cast(VarcharType(30)),
    col("ID_Sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("ID_Dim1").cast(IntegerType),
    col("ID_Dim2").cast(IntegerType),
    col("ID_Escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,10)),
    col("nombre").cast(VarcharType(100)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)
)


# In[94]:


var finalizaConColumnas = factConCambios2.join(dicc4, factConCambios2("ID_Escenario") === dicc4("id_escenarioDic") && factConCambios2("nombre") === dicc4("newnomenc"), "left").dropDuplicates.cache
var finalHito = finalizaConColumnas.drop(col("nombre")).drop(col("newnomenc")).drop(("id_escenarioDic")).drop(col("num_periodo")).drop(col("val_kpi"))

var finalHito2 = finalHito.select(
    col("ID_Informe").cast(IntegerType),
    col("ID_concepto").cast(IntegerType),
    col("ID_Negocio").cast(VarcharType(30)),
    col("ID_Sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("ID_Dim1").cast(IntegerType),
    col("ID_Dim2").cast(IntegerType),
    col("ID_Escenario").cast(IntegerType),
    col("val_pais").cast(VarcharType(30)),
    col("val_canal").cast(IntegerType).as("ID_canal"),
    col("val_producto").cast(IntegerType).as("ID_producto"),
    col("val_origen").cast(IntegerType).as("id_origen"),
    col("imp_importe").cast(DecimalType(24,3)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).dropDuplicates

if (escenario == "REAL"){
    finalHito2 = finalHito2.where(col("id_escenario") === 1).cache
}else if (escenario == "PA-UPA"){
    finalHito2 = finalHito2.where(col("id_escenario").isin(2,3)).cache
}else if (escenario == "EST"){
    finalHito2 = finalHito2.where(col("id_escenario") === 4).cache
}else if (escenario == "PA"){
    finalHito2 = finalHito2.where(col("id_escenario") === 2).cache
}else if (escenario == "UPA"){
    finalHito2 = finalHito2.where(col("id_escenario") === 3).cache
}else{
    finalHito2.cache
}
finalHito2.limit(1).count()


# In[95]:


spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

val ds_output = s"cli0010_tb_fac_m_pbi_mmexh2/"
val pathWriteFinal = s"$edwPath/$business/$ds_output"
val parquet_path_final = f"abfss://$container_output@$my_account/$pathWriteFinal"

finalHito2.coalesce(1).write.partitionBy("cod_fecha").mode("overwrite").format("parquet").save(parquet_path_final)


# In[96]:


for ((k,v) <- sc.getPersistentRDDs) {
   v.unpersist()
}

var finaliza_pbi = spark.read.parquet(parquet_path_final).where(col("cod_fecha") === v_periodo)

if (escenario == "REAL"){
    finaliza_pbi = finaliza_pbi.where(col("id_escenario") === 1).cache
}else if (escenario == "PA-UPA"){
    finaliza_pbi = finaliza_pbi.where(col("id_escenario").isin(2,3)).cache
}else if (escenario == "EST"){
    finaliza_pbi = finaliza_pbi.where(col("id_escenario") === 4).cache
}else if (escenario == "PA"){
    finaliza_pbi = finaliza_pbi.where(col("id_escenario") === 2).cache
}else if (escenario == "UPA"){
    finaliza_pbi = finaliza_pbi.where(col("id_escenario") === 3).cache
}else{
    finaliza_pbi.cache
}


# In[97]:


import com.microsoft.azure.synapse.tokenlibrary.TokenLibrary

import java.sql.Connection 
import java.sql.ResultSet 
import java.sql.Statement 
import com.microsoft.sqlserver.jdbc.SQLServerDataSource

var schemaName = "sch_anl"

var sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmexh2 where cod_fecha in ($v_periodo)"

if (escenario == "REAL"){
    sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmexh2 where cod_fecha in ($v_periodo) and id_escenario = 1"
}else if (escenario == "PA-UPA"){
    sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmexh2 where cod_fecha in ($v_periodo) and id_escenario in (2,3)"
}else if (escenario == "EST"){
    sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmexh2 where cod_fecha in ($v_periodo) and id_escenario = 4"
}else if (escenario == "PA"){
    sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmexh2 where cod_fecha in ($v_periodo) and id_escenario = 2"
}else if (escenario == "UPA"){
    sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmexh2 where cod_fecha in ($v_periodo) and id_escenario = 3"
}

print(sqlQuery+"\n")
executeSQLPool (token, url, sqlQuery , schemaName, "sch_anl.cli0010_tb_fac_m_pbi_mmexh2");


# In[98]:


var finaliza = finaliza_pbi.select(
    col("ID_Informe").cast(IntegerType),
    col("ID_concepto").cast(IntegerType),
    col("ID_Negocio").cast(VarcharType(30)),
    lit(1657).as("ID_Sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("ID_Dim1").cast(IntegerType),
    col("ID_Dim2").cast(IntegerType),
    col("ID_Escenario").cast(IntegerType),
    lit("MX").as("val_pais").cast(VarcharType(30)),
    col("ID_canal").cast(IntegerType),
    col("ID_producto").cast(IntegerType),
    col("ID_origen").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,3)),
    col("val_divisa").cast(VarcharType(30)),
    col("id_tipocoste").cast(IntegerType),
    col("cod_fecha").cast(IntegerType)).dropDuplicates.cache


# In[99]:


finaliza.
write.
    format("com.microsoft.sqlserver.jdbc.spark").
    mode("append"). //overwrite
    option("url", url).
    option("dbtable", s"sch_anl.cli0010_tb_fac_m_pbi_mmexh2").
    option("mssqlIsolationLevel", "READ_UNCOMMITTED").
    option("truncate", "false").
    option("tableLock","true").
    option("reliabilityLevel","BEST_EFFORT").
    option("numPartitions","2").
    option("batchsize","1000000").
    option("accessToken", token).
    save()


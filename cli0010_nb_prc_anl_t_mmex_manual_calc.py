#!/usr/bin/env python
# coding: utf-8

# ## cli0010_nb_prc_anl_t_mmex_manual_calc
# 
# 
# 

# In[1]:


val v_year = "2023"
val v_month  = "01"
val escenario = "PA" // {ALL / PA-UPA / EST / REAL / PA / UPA}
val applicationName: String = ""
val parentUid:String = "N/A"
val uuid:String = "N/A"


# In[2]:


get_ipython().run_line_magic('run', 'cli0010/util/cli0010_nb_prc_app_obj_module_properties')


# In[3]:


get_ipython().run_line_magic('run', 'cli0010/util/cli0010_nb_prc_app_obj_module_library')


# In[4]:


//Paths del LakeHouse
val lakehousePath = "CLI0010/trn"
val edwPath = "CLI0010/edw"
val container_output = "lakehouse"

val linked_service_name = "DL_COM"
val my_container = "processed"
val cont_lakehouse = "lakehouse"
val my_account = conexion("Endpoint").toString.substring(8)


# In[5]:


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


# In[6]:


import java.io.IOException
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.regexp_replace


# In[7]:


val business="AM_COM_Vista_Cliente"
val ds_output = s"cli0010_tb_fac_m_mod_mmex/"
val ds_output_temp = ds_output.concat("temp")
val pathWriteTemp = s"$edwPath/$business/$ds_output_temp"
val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

var aperiodo = v_year.concat(v_month)


# In[10]:


// {ALL / PA-UPA / EST / REAL}

var t_Pool_fac_SQLPOOL = spark.emptyDataFrame

if (escenario == "REAL"){
    t_Pool_fac_SQLPOOL = spark.read.parquet(parquet_path_temp).where(col("num_periodo") === aperiodo && col("id_escenario") === 3)
}else if (escenario == "PA-UPA"){
    t_Pool_fac_SQLPOOL = spark.read.parquet(parquet_path_temp).where(col("num_periodo") === aperiodo && col("id_escenario").isin(2,4))
}else if (escenario == "EST"){
    t_Pool_fac_SQLPOOL = spark.read.parquet(parquet_path_temp).where(col("num_periodo") === aperiodo && col("id_escenario") === 1)
}else if (escenario == "PA"){
    t_Pool_fac_SQLPOOL = spark.read.parquet(parquet_path_temp).where(col("num_periodo") === aperiodo && col("id_escenario") === 2)
}else if (escenario == "UPA"){
    t_Pool_fac_SQLPOOL = spark.read.parquet(parquet_path_temp).where(col("num_periodo") === aperiodo && col("id_escenario") === 4)
}else{
    t_Pool_fac_SQLPOOL = spark.read.parquet(parquet_path_temp).where(col("num_periodo") === aperiodo)
}

t_Pool_fac_SQLPOOL.cache


# In[11]:


var t_Pool_kpi = readFromSQLPool("sch_anl","cli0010_tb_dim_m_kpi_mmex", token).cache

// Agregado LUCAS
var t_Pool_fac = t_Pool_fac_SQLPOOL.dropDuplicates("num_periodo", "id_escenario"
    , "val_unidadnegocio", "val_origen", "val_producto", "val_pais", "val_proceso", "val_sociedad", "val_unidadmedida", "val_canal", 
    "id_kpi", "val_kpi", "cod_kpi", "num_periodo_mensacum", "val_flag_calculado").cache
    //.where( ( col("periodo_mensacum") === "mensual" && col("id_escenario") === 3 ) || ( col("id_escenario").notEqual(3) || (col("kpi").equalTo("MMCCRREAL700" )))) // Agregado LUCAS


# In[12]:


def look_column_else_zero0( tabla : DataFrame , columna : String) : DataFrame =
{
    if ( tabla.columns.map(_.toUpperCase).contains(columna.toUpperCase) ) {
     return tabla }
    else  {
        return tabla.withColumn(columna, lit(0).cast(DecimalType(24,10))) }
        
}

def look_column_else_zero( tabla : DataFrame , columna : String) : DataFrame =
{
    if ( tabla.columns.map(_.toUpperCase).contains(columna.toUpperCase) ) {
     return tabla }
    else  {
        return tabla.withColumn(columna, lit(null).cast(DecimalType(24,10))) }
        
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


# In[13]:


var df_resultados_generados2 = t_Pool_fac.join(t_Pool_kpi, t_Pool_fac("id_kpi") === t_Pool_kpi("id_kpi"))
    .select(t_Pool_fac("num_periodo"), 
        t_Pool_fac("id_escenario"), 
        t_Pool_fac("id_kpi"), 
        t_Pool_kpi("des_kpi"), 
        t_Pool_fac("val_kpi")).withColumn("DES2", regexp_replace($"des_kpi", "Mov. Mex.", "")).drop("des_kpi").cache

//display(df_resultados_generados2.where( col("des_kpi").like("%Mon%")))


# In[14]:


var df_resultados_generados = df_resultados_generados2.dropDuplicates


# In[15]:


val pivotea = df_resultados_generados2.groupBy("num_periodo", "id_escenario").pivot("DES2").sum("val_kpi")


# In[16]:


val business="AM_COM_Vista_Cliente"
val ds_output = s"cli0010_tb_aux_m_bfc_dg_mmex/"
val ds_output_temp = ds_output.concat("temp")
val pathWriteTemp = s"$edwPath/$business/$ds_output_temp"
val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

var BfcDG_sql = spark.read.parquet(parquet_path_temp).where(col("id_periodo") === aperiodo).cache


# In[17]:


var pivot = pivotea.join(BfcDG_sql, pivotea("id_escenario") === BfcDG_sql("id_escenario_bfc") && pivotea("num_periodo") === BfcDG_sql("id_periodo"), "left").dropDuplicates.cache


# In[18]:


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


# In[19]:


var df_177 = look_columns_else_null(pivot,columnsToEnsure).cache


# In[20]:


var n = df_177.select(
    concat(col("num_periodo"), lit("01")).cast(IntegerType).as("cod_periodo"),
    col("id_escenario").cast(IntegerType),
    //col(" EES_Cuota en número de EES").cast(DecimalType(24,10)).as("imp_ees_cuota_num_ees"),
    col("Central Movilidad Mexico_Amortizaciones").cast(DecimalType(24,10)).as("imp_ctral_amort"),
    col("imp_ctral_cf_est_2_viaj").cast(DecimalType(24,10)).as("imp_ctral_cf_est_2_viaj"),
    //col(" EES_Índice de eficiencia EES").cast(DecimalType(24,10)).as("imp_ees_ind_efic_ees"),
    col("Central Movilidad Mexico_Costes fijos de estructura##Comunicación y relaciones públicas").cast(DecimalType(24,10)).as("imp_ctral_cf_est_2_com_rrpp"),
    col("Central Movilidad Mexico_Costes fijos de estructura##Otros servicios").cast(DecimalType(24,10)).as("imp_ctral_cf_est_2_otros_serv"),
    col("Central Movilidad Mexico_Costes fijos de estructura##Personal").cast(DecimalType(24,10)).as("imp_ctral_cf_est_2_pers"),
    col("Central Movilidad Mexico_Costes fijos de estructura##Primas de seguros").cast(DecimalType(24,10)).as("imp_ctral_cf_est_2_primas_seg"),
    col("Central Movilidad Mexico_Costes fijos de estructura##Publicidad y relaciones públicas").cast(DecimalType(24,10)).as("imp_ctral_cf_est_2_pub_rrpp"),
    col("Central Movilidad Mexico_Costes fijos de estructura##Servicios bancarios y similares").cast(DecimalType(24,10)).as("imp_ctral_cf_est_2_serv_banc"),
    col("Central Movilidad Mexico_Costes fijos de estructura##Servicios profesionales").cast(DecimalType(24,10)).as("imp_ctral_cf_est_2_serv_prof"),
    col("Central Movilidad Mexico_Costes fijos de estructura##Suministros").cast(DecimalType(24,10)).as("imp_ctral_cf_est_2_sumin"),
    col("Central Movilidad Mexico_Margen de contribución").cast(DecimalType(24,10)).as("imp_ctral_mc"),
    col("Central Movilidad Mexico_Otros gastos").cast(DecimalType(24,10)).as("imp_ctral_otros_gastos"),
    col("Central Movilidad Mexico_Otros resultados").cast(DecimalType(24,10)).as("imp_ctral_otros_result"),
    col("Central Movilidad Mexico_Otros servicios DG").cast(DecimalType(24,10)).as("imp_ctral_otros_serv_DG"),
    col("Central Movilidad Mexico_Personal##Otros costes de personal").cast(DecimalType(24,10)).as("imp_ctral_pers_2_otros_cost"),
    col("Central Movilidad Mexico_Personal##Retribución").cast(DecimalType(24,10)).as("imp_ctral_pers_2_retrib"),
    col("Central Movilidad Mexico_Provisiones recurrentes").cast(DecimalType(24,10)).as("imp_ctral_provis_recur"),
    col("Central Movilidad Mexico_Servicios corporativos").cast(DecimalType(24,10)).as("imp_ctral_serv_corp"),
    col("Central Movilidad Mexico_Servicios externos##Arrendamientos y cánones").cast(DecimalType(24,10)).as("imp_ctral_serv_ext_2_arrycan"),
    col("Central Movilidad Mexico_Servicios externos##Mantenimiento y reparaciones").cast(DecimalType(24,10)).as("imp_ctral_serv_ext_2_mant_rep"),
    col("Central Movilidad Mexico_Servicios externos##Otros servicios externos").cast(DecimalType(24,10)).as("imp_ctral_sv_ext_2_otr_sv_ext"),
    col("Central Movilidad Mexico_Servicios externos##Publicidad y relaciones públicas").cast(DecimalType(24,10)).as("imp_ctral_serv_ext_2_pub_rrpp"),
    col("Central Movilidad Mexico_Servicios externos##Seguros").cast(DecimalType(24,10)).as("imp_ctral_serv_ext_2_seg"),
    col("Central Movilidad Mexico_Servicios externos##Servicios profesionales").cast(DecimalType(24,10)).as("imp_ctral_serv_ext_2_serv_prof"),
    col("Central Movilidad Mexico_Servicios externos##Suministros").cast(DecimalType(24,10)).as("imp_ctral_serv_ext_2_sumin"),
    col("Central Movilidad Mexico_Servicios externos##Tributos").cast(DecimalType(24,10)).as("imp_ctral_serv_ext_2_trib"),
    col("Central Movilidad Mexico_Servicios externos##Viajes").cast(DecimalType(24,10)).as("imp_ctral_serv_ext_2_viajes"),
    col("Central Movilidad Mexico_Servicios transversales DG").cast(DecimalType(24,10)).as("imp_ctral_serv_transv_DG"),
    col("Central Movilidad Mexico_Tipo de Cambio MXN/EUR acumulado").cast(DecimalType(24,10)).as("imp_ctral_Tcambio_EUR_MXN"), // por faltante de kpi
    col("Central Movilidad Mexico_Tipo de Cambio MXN/EUR acumulado").cast(DecimalType(24,10)).as("imp_ctral_Tcambio_MXN_EUR_ac"),
    col("Central Movilidad Mexico_Tipo de Cambio MXN/EUR mensual").cast(DecimalType(24,10)).as("imp_ctral_Tcambio_MXN_EUR_m"),
    col(" EES Mayorista_Amortizaciones").cast(DecimalType(24,10)).as("imp_ees_mayor_amort"),
    col(" EES Mayorista_Corporativos").cast(DecimalType(24,10)).as("imp_ees_mayor_corp"),
    col(" EES Mayorista_Costes fijos EES##Actividades promocionales").cast(DecimalType(24,10)).as("imp_ees_mayor_cf_ees_2_act_prm"),
    col(" EES Mayorista_Costes fijos EES##Mantenimiento y reparaciones").cast(DecimalType(24,10)).as("imp_ees_mayor_cf_ees_2_mante"),
    col(" EES Mayorista_Costes fijos EES##Otros costes fijos EES").cast(DecimalType(24,10)).as("imp_ees_mayor_cf_ees_2_otros"),
    col(" EES Mayorista_Costes fijos EES##Sistemas de controles volumétricos").cast(DecimalType(24,10)).as("imp_ees_mayor_cf_ees_2_sc_volu"),
    col(" EES Mayorista_Costes fijos EES##Técnicos Neotech").cast(DecimalType(24,10)).as("imp_ees_mayor_cf_ees_2_neotech"),
    col(" EES Mayorista_EBIT CCS recurrente Filiales##JV Mar Cortés (50%)").cast(DecimalType(24,10)).as("imp_ees_mayor_EBIT_CCS_JV_cort"),
    col(" EES Mayorista_Margen de Contribución EES##Efecto Reservas Estratégicas").cast(DecimalType(24,10)).as("imp_ees_mayor_mc_ees_2_res_est"),
    col(" EES Mayorista_Margen de Contribución EES##Extramargen Monterra EES").cast(DecimalType(24,10)).as("imp_ees_mayor_mc_ees_2_ext_mon"),
    col(" EES Mayorista_Margen de Contribución EES##Extramargen Olstor EES").cast(DecimalType(24,10)).as("imp_ees_mayor_mc_ees_2_ext_ols"),
    col(" EES Mayorista_Margen de Contribución EES##MC sin Reservas Estratégicas").cast(DecimalType(24,10)).as("imp_ees_mayor_mc_ees_2_sin_res"),
    col(" EES Mayorista_Otros gastos").cast(DecimalType(24,10)).as("imp_ees_mayor_otros_gastos"),
    col(" EES Mayorista_Otros resultados").cast(DecimalType(24,10)).as("imp_ees_mayor_otros_result"),
    col(" EES Mayorista_Otros servicios DG").cast(DecimalType(24,10)).as("imp_ees_mayor_otros_serv_DG"),
    col(" EES Mayorista_Personal##Otros costes de personal").cast(DecimalType(24,10)).as("imp_ees_mayor_pers_2_otro_cost"),
    col(" EES Mayorista_Personal##Retribución").cast(DecimalType(24,10)).as("imp_ees_mayor_pers_2_retrib"),
    col(" EES Mayorista_Provisiones recurrentes").cast(DecimalType(24,10)).as("imp_ees_mayor_provis_recur"),
    col(" EES Mayorista_Servicios corporativos").cast(DecimalType(24,10)).as("imp_ees_mayor_serv_corp"),
    col(" EES Mayorista_Servicios externos##Arrendamientos y cánones").cast(DecimalType(24,10)).as("imp_ees_mayor_sv_ext_2_arrycan"),
    col(" EES Mayorista_Servicios externos##Mantenimiento y reparaciones").cast(DecimalType(24,10)).as("imp_ees_mayor_sv_ext_2_man_rep"),
    col(" EES Mayorista_Servicios externos##Otros servicios externos").cast(DecimalType(24,10)).as("imp_ees_mayor_sv_ext_2_otro_sv"),
    col(" EES Mayorista_Servicios externos##Publicidad y relaciones públicas").cast(DecimalType(24,10)).as("imp_ees_mayor_sv_ext_2_pb_rrpp"),
    col(" EES Mayorista_Servicios externos##Seguros").cast(DecimalType(24,10)).as("imp_ees_mayor_serv_ext_2_seg"),
    col(" EES Mayorista_Servicios externos##Servicios profesionales").cast(DecimalType(24,10)).as("imp_ees_mayor_sv_ext_2_sv_prof"),
    col(" EES Mayorista_Servicios externos##Suministros").cast(DecimalType(24,10)).as("imp_ees_mayor_serv_ext_2_sumin"),
    col(" EES Mayorista_Servicios externos##Tributos").cast(DecimalType(24,10)).as("imp_ees_mayor_serv_ext_2_trib"),
    col(" EES Mayorista_Servicios externos##Viajes").cast(DecimalType(24,10)).as("imp_ees_mayor_sv_ext_2_viajes"),
    col(" EES Mayorista_Servicios transversales DG").cast(DecimalType(24,10)).as("imp_ees_mayor_serv_transv_DG"),
    col(" EES Mayorista_Tipo de Cambio MXN/EUR acumulado").cast(DecimalType(24,10)).as("imp_ees_mayor_Tcamb_MXN_EUR_ac"),
    col(" EES Mayorista_Tipo de Cambio MXN/EUR mensual").cast(DecimalType(24,10)).as("imp_ees_mayor_Tcamb_MXN_EUR_m"),
    col(" EES Mayorista_VentasGasóleo A regular: Diesel").cast(DecimalType(24,10)).as("imp_ees_mayor_vta_go_reg_diesl"),
    col(" EES Mayorista_VentasPremium: 92").cast(DecimalType(24,10)).as("imp_ees_mayor_ventas_prem_92"),
    col(" EES Mayorista_VentasRegular: 87").cast(DecimalType(24,10)).as("imp_ees_mayor_ventas_reg_87"),
    col(" EES Minorista_Amortizaciones").cast(DecimalType(24,10)).as("imp_ees_minor_amort"),
    col(" EES Minorista_Corporativos").cast(DecimalType(24,10)).as("imp_ees_minor_corp"),
    col(" EES Minorista_Costes fijos EES##Actividades promocionales").cast(DecimalType(24,10)).as("imp_ees_minor_cf_ees_2_act_prm"),
    col(" EES Minorista_Costes fijos EES##Mantenimiento y reparaciones").cast(DecimalType(24,10)).as("imp_ees_minor_cf_ees_2_man_rep"),
    col(" EES Minorista_Costes fijos EES##Otros costes fijos EES").cast(DecimalType(24,10)).as("imp_ees_minor_cf_ees_2_otro_cf"),
    col(" EES Minorista_Costes fijos EES##Sistemas de controles volumétricos").cast(DecimalType(24,10)).as("imp_ees_minor_cf_ees_2_sc_volu"),
    col(" EES Minorista_Costes fijos EES##Técnicos Neotech").cast(DecimalType(24,10)).as("imp_ees_minor_cf_ees_2_neotech"),
    col(" EES Minorista_Otros gastos").cast(DecimalType(24,10)).as("imp_ees_minor_otros_gastos"),
    col(" EES Minorista_Otros márgenes de contribución").cast(DecimalType(24,10)).as("imp_ees_minor_otros_mc"),
    col(" EES Minorista_Otros márgenes de contribución##Gestión Directa (Margen de contribución EES Minorista)").cast(DecimalType(24,10)).as("imp_ees_minor_otro_mc_2_gdirec"),
    col(" EES Minorista_Otros resultados").cast(DecimalType(24,10)).as("imp_ees_minor_otros_result"),
    col(" EES Minorista_Otros servicios DG").cast(DecimalType(24,10)).as("imp_ees_minor_otros_serv_DG"),
    col(" EES Minorista_Personal##Otros costes de personal").cast(DecimalType(24,10)).as("imp_ees_minor_pers_2_otro_cost"),
    col(" EES Minorista_Personal##Retribución").cast(DecimalType(24,10)).as("imp_ees_minor_pers_2_retrib"),
    col(" EES Minorista_Provisiones recurrentes").cast(DecimalType(24,10)).as("imp_ees_minor_provis_recur"),
    col(" EES Minorista_Servicios corporativos").cast(DecimalType(24,10)).as("imp_ees_minor_serv_corp"),
    col(" EES Minorista_Servicios externos##Arrendamientos y cánones").cast(DecimalType(24,10)).as("imp_ees_minor_sv_ext_2_arrycan"),
    col(" EES Minorista_Servicios externos##Mantenimiento y reparaciones").cast(DecimalType(24,10)).as("imp_ees_minor_sv_ext_2_man_rep"),
    col(" EES Minorista_Servicios externos##Otros servicios externos").cast(DecimalType(24,10)).as("imp_ees_minor_sv_ext_2_otro_sv"),
    col(" EES Minorista_Servicios externos##Publicidad y relaciones públicas").cast(DecimalType(24,10)).as("imp_ees_minor_sv_ext_2_pb_rrpp"),
    col(" EES Minorista_Servicios externos##Seguros").cast(DecimalType(24,10)).as("imp_ees_minor_serv_ext_2_seg"),
    col(" EES Minorista_Servicios externos##Servicios profesionales").cast(DecimalType(24,10)).as("imp_ees_minor_sv_ext_2_sv_prof"),
    col(" EES Minorista_Servicios externos##Suministros").cast(DecimalType(24,10)).as("imp_ees_minor_serv_ext_2_sumin"),
    col(" EES Minorista_Servicios externos##Tributos").cast(DecimalType(24,10)).as("imp_ees_minor_serv_ext_2_trib"),
    col(" EES Minorista_Servicios externos##Viajes").cast(DecimalType(24,10)).as("imp_ees_minor_sv_ext_2_viajes"),
    col(" EES Minorista_Servicios transversales DG").cast(DecimalType(24,10)).as("imp_ees_minor_serv_transv_DG"),
    col(" EES Minorista_VentasGasóleo A regular: Diesel").cast(DecimalType(24,10)).as("imp_ees_minor_vta_go_reg_diesl"),
    col(" EES Minorista_VentasPremium: 92").cast(DecimalType(24,10)).as("imp_ees_minor_ventas_prem_92"),
    col(" EES Minorista_VentasRegular: 87").cast(DecimalType(24,10)).as("imp_ees_minor_ventas_reg_87"),
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
    //col(" EES_Cuotas").cast(DecimalType(24,10)).as("imp_ees_cu"),
    //col(" EES_Cuotas##Gasolina automociónGasolina automoción").cast(DecimalType(24,10)).as("imp_ees_cu_2_gna_autom"),
    //col(" EES_Cuotas##Gasóleo A regular: DieselGasóleo A regular: Diesel").cast(DecimalType(24,10)).as("imp_ees_cu_2_go_a_reg_diesel"),
    //col(" EES_Cuotas##Gasóleo terrestreGasóleo terrestre").cast(DecimalType(24,10)).as("imp_ees_cu_2_go_ter"),
    //col(" EES_Cuotas##Gna automoción + Go A terrestreGna automoción + Go A terrestre").cast(DecimalType(24,10)).as("imp_ees_cu_2_gna_auto_go_a_ter"),
    //col(" EES_Cuotas##Premium: 92Premium: 92").cast(DecimalType(24,10)).as("imp_ees_cu_2_prem_92"),
    //col(" EES_Cuotas##Regular: 87Regular: 87").cast(DecimalType(24,10)).as("imp_ees_cu_2_reg_87"),
    //col(" EES_Cuotas##Total Movilidad MéxicoTotal Movilidad México").cast(DecimalType(24,10)).as("imp_ees_cu_2_tot_mm"),
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
    //col(" EES_Número de EES Repsol").cast(DecimalType(24,10)).as("imp_ees_num_ees_repsol"),
    //col(" EES_Número de EES mercado").cast(DecimalType(24,10)).as("imp_ees_num_ees_mercado"),
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
    //col(" EES_?Cuota en número de EES").cast(DecimalType(24,10)).as("imp_ees_cu_num_ees"),
    //col(" EES_?Índice de eficiencia EES").cast(DecimalType(24,10)).as("imp_ees_ind_efic_ees"),
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
    //col("2049_0919_imp_impuestos").cast(DecimalType(24,10)).as("2049_0919_imp_impuestos"),
    col("imp_2049_imp_juridico").cast(DecimalType(24,10)).as("imp_2049_imp_juridico"),
    col("imp_2049_imp_compras").cast(DecimalType(24,10)).as("imp_2049_imp_compras"),
    col("imp_2049_imp_econom_admin").cast(DecimalType(24,10)).as("imp_2049_imp_econom_admin"),
    col("imp_2049_imp_financiero").cast(DecimalType(24,10)).as("imp_2049_imp_financiero"),
    col("imp_2049_imp_fiscal").cast(DecimalType(24,10)).as("imp_2049_imp_fiscal"),
    col("imp_2049_imp_impuestos").cast(DecimalType(24,10)).as("imp_2049_imp_impuestos_prev"),
    col("imp_2049_imp_minoritarios").cast(DecimalType(24,10)).as("imp_2049_imp_minoritarios"),
    col("imp_2049_imp_otros").cast(DecimalType(24,10)).as("imp_2049_imp_otros"),
    col("imp_2049_imp_pyo").cast(DecimalType(24,10)).as("imp_2049_imp_pyo"),
    col("imp_2049_imp_resul_espec_adi").cast(DecimalType(24,10)).as("imp_2049_imp_resul_espec_adi"),
    col("imp_2049_imp_serv_globales").cast(DecimalType(24,10)).as("imp_2049_imp_serv_globales"),
    col("imp_2049_imp_ti").cast(DecimalType(24,10)).as("imp_2049_imp_ti"),
    col("imp_2049_imp_ti_as_a_service").cast(DecimalType(24,10)).as("imp_2049_imp_ti_as_a_service"),
    col("imp_2049_imp_efecto_patri_adi").cast(DecimalType(24,10)).as("imp_2049_imp_efecto_patri_adi"),
    col("imp_2049_imp_participadas").cast(DecimalType(24,10)).as("imp_2049_imp_participadas"),
    col("imp_2049_imp_seguros").cast(DecimalType(24,10)).as("imp_2049_imp_seguros"),
    col("imp_2049_imp_digitalizacion").cast(DecimalType(24,10)).as("imp_2049_imp_digitalizacion"),
    col("imp_2049_imp_sostenibilidad").cast(DecimalType(24,10)).as("imp_2049_imp_sostenibilidad"),
    col("imp_2049_imp_auditoria").cast(DecimalType(24,10)).as("imp_2049_imp_auditoria"),
    col("imp_2049_imp_planif_control").cast(DecimalType(24,10)).as("imp_2049_imp_planif_control"),
    col("imp_2049_imp_ser_corporativos").cast(DecimalType(24,10)).as("imp_2049_imp_ser_corporativos"),
    col("imp_2049_imp_patrimonial_seg").cast(DecimalType(24,10)).as("imp_2049_imp_patrimonial_seg"),
    col("imp_2049_imp_comunicacion").cast(DecimalType(24,10)).as("imp_2049_imp_comunicacion"),
    col("imp_2049_imp_techlab").cast(DecimalType(24,10)).as("imp_2049_imp_techlab"),
    col("imp_2049_imp_ingenieria").cast(DecimalType(24,10)).as("imp_2049_imp_ingenieria"),
    col("imp_otros_serv_dg_crc").cast(DecimalType(24,10)).as("imp_otros_serv_dg_crc"),
    col("imp_otros_serv_dg_e_commerce").cast(DecimalType(24,10)).as("imp_otros_serv_dg_e_commerce"),
    col("imp_otros_serv_dg_fidel_global").cast(DecimalType(24,10)).as("imp_otros_serv_dg_fidel_global"),
    col("imp_otros_serv_dg_int_cliente").cast(DecimalType(24,10)).as("imp_otros_serv_dg_int_cliente"),
    col("imp_otros_serv_dg_mkt_cloud").cast(DecimalType(24,10)).as("imp_otros_serv_dg_mkt_cloud"),
    col("imp_otros_serv_dg_mkt_fid_evt").cast(DecimalType(24,10)).as("imp_otros_serv_dg_mkt_fid_evt"),
    col("imp_otros_serv_dg_otros_serv").cast(DecimalType(24,10)).as("imp_otros_serv_dg_otros_serv"),
    col("imp_serv_trans_otros_ser_trans").cast(DecimalType(24,10)).as("imp_serv_trans_otros_ser_trans"),
    col("imp_serv_transv_pyc_cliente").cast(DecimalType(24,10)).as("imp_serv_transv_pyc_cliente"),
    col("imp_serv_transv_pyo_cliente").cast(DecimalType(24,10)).as("imp_serv_transv_pyo_cliente"),
    col("imp_serv_transv_sost_cliente").cast(DecimalType(24,10)).as("imp_serv_transv_sost_cliente"),
    col("imp_2049_imp_ebit_css_recurrente").cast(DecimalType(24,10)).as("imp_ebit_css_recurrente")
)

n.limit(1).count()


# **Mov. Mex. Central**

# In[21]:


// var CentralPersonal = n.withColumn("imp_ctral_pers", when(isnull(col("imp_ctral_pers_2_otros_cost")), 0).otherwise(col("imp_ctral_pers_2_otros_cost"))
//     + when(isnull(col("imp_ctral_pers_2_retrib")), 0).otherwise(col("imp_ctral_pers_2_retrib")))


// var CentralSerExt  = CentralPersonal.withColumn("imp_ctral_ser_ext", when(isnull(col("imp_ctral_serv_ext_2_mant_rep")), 0).otherwise(col("imp_ctral_serv_ext_2_mant_rep"))
//     + when(isnull(col("imp_ctral_sv_ext_2_otr_sv_ext")), 0).otherwise(col("imp_ctral_sv_ext_2_otr_sv_ext"))
//     + when(isnull(col("imp_ctral_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_ctral_serv_ext_2_pub_rrpp"))
//     + when(isnull(col("imp_ctral_serv_ext_2_seg")), 0).otherwise(col("imp_ctral_serv_ext_2_seg"))
//     + when(isnull(col("imp_ctral_serv_ext_2_serv_prof")), 0).otherwise(col("imp_ctral_serv_ext_2_serv_prof"))
//     + when(isnull(col("imp_ctral_serv_ext_2_sumin")), 0).otherwise(col("imp_ctral_serv_ext_2_sumin"))
//     + when(isnull(col("imp_ctral_serv_ext_2_trib")), 0).otherwise(col("imp_ctral_serv_ext_2_trib"))
//     + when(isnull(col("imp_ctral_serv_ext_2_arrycan")), 0).otherwise(col("imp_ctral_serv_ext_2_arrycan"))
//     + when(isnull(col("imp_ctral_serv_ext_2_viajes")), 0).otherwise(col("imp_ctral_serv_ext_2_viajes")))


// var CentralTotalCostesFijosConcepto  = CentralSerExt.withColumn("imp_ctral_total_cf_concept", when(isnull(col("imp_ctral_pers")), 0).otherwise(col("imp_ctral_pers"))
//     + when(isnull(col("imp_ctral_ser_ext")), 0).otherwise(col("imp_ctral_ser_ext"))
//     + when(isnull(col("imp_ctral_serv_corp")), 0).otherwise(col("imp_ctral_serv_corp"))
//     + when(isnull(col("imp_ctral_serv_transv_DG")), 0).otherwise(col("imp_ctral_serv_transv_DG"))
//     + when(isnull(col("imp_ctral_otros_serv_DG")), 0).otherwise(col("imp_ctral_otros_serv_DG")))

// var CentralCostesFijosDeEstructura  = CentralTotalCostesFijosConcepto.withColumn("imp_ctral_cost_fij_estruct", when(isnull(col("imp_ctral_cf_est_2_com_rrpp")), 0).otherwise(col("imp_ctral_cf_est_2_com_rrpp"))
//     + when(isnull(col("imp_ctral_cf_est_2_otros_serv")), 0).otherwise(col("imp_ctral_cf_est_2_otros_serv"))
//     + when(isnull(col("imp_ctral_cf_est_2_pers")), 0).otherwise(col("imp_ctral_cf_est_2_pers"))
//     + when(isnull(col("imp_ctral_cf_est_2_primas_seg")), 0).otherwise(col("imp_ctral_cf_est_2_primas_seg"))
//     + when(isnull(col("imp_ctral_cf_est_2_pub_rrpp")), 0).otherwise(col("imp_ctral_cf_est_2_pub_rrpp"))
//     + when(isnull(col("imp_ctral_cf_est_2_serv_banc")), 0).otherwise(col("imp_ctral_cf_est_2_serv_banc"))
//     + when(isnull(col("imp_ctral_cf_est_2_sumin")), 0).otherwise(col("imp_ctral_cf_est_2_sumin"))
//     + when(isnull(col("imp_ctral_cf_est_2_serv_prof")), 0).otherwise(col("imp_ctral_cf_est_2_serv_prof"))
//     + when(isnull(col("imp_ctral_cf_est_2_viaj")), 0).otherwise(col("imp_ctral_cf_est_2_viaj"))
//     )

// var CentralTotalCostesFijosAnalitica = CentralCostesFijosDeEstructura.withColumn("imp_ctral_total_cf_analitica", when(isnull(col("imp_ctral_cost_fij_estruct")), 0).otherwise(col("imp_ctral_cost_fij_estruct"))
//     + when(isnull(col("imp_ctral_otros_gastos")), 0).otherwise(col("imp_ctral_otros_gastos")))
//     //+ when(isnull(col("imp_ctral_serv_corp")), 0).otherwise(col("imp_ctral_serv_corp")))




// var CentralCostesFijos  = CentralTotalCostesFijosAnalitica.withColumn("imp_ctral_cost_fijos", when(isnull(col("imp_ctral_total_cf_analitica")), 0).otherwise(col("imp_ctral_total_cf_analitica")))


// var CentralResuOperaciones  = CentralCostesFijos.withColumn("imp_ctral_resu_ope", when(isnull(col("imp_ctral_mc")), 0).otherwise(col("imp_ctral_mc"))
//     - when(isnull(col("imp_ctral_cost_fijos")), 0).otherwise(col("imp_ctral_cost_fijos"))
//     - when(isnull(col("imp_ctral_amort")), 0).otherwise(col("imp_ctral_amort"))
//     - when(isnull(col("imp_ctral_provis_recur")), 0).otherwise(col("imp_ctral_provis_recur"))
//     + when(isnull(col("imp_ctral_otros_result")), 0).otherwise(col("imp_ctral_otros_result"))).cache



# **MOV. MEX. EES MEXICO**

# In[22]:


// var imp_ees_result_otras_soc = CentralResuOperaciones.withColumn("imp_ees_result_otras_soc", when(isnull(col("imp_ees_EBIT_CCS_JV_mar_cortes")), 0).otherwise(col("imp_ees_EBIT_CCS_JV_mar_cortes")))

// var imp_ees_ventas_monterra = imp_ees_result_otras_soc.withColumn("imp_ees_ventas_monterra", when(isnull(col("imp_ees_ventas_mont_reg_diesel")), 0).otherwise(col("imp_ees_ventas_mont_reg_diesel"))
//     + when(isnull(col("imp_ees_ventas_mont_premium")), 0).otherwise(col("imp_ees_ventas_mont_premium"))
//     + when(isnull(col("imp_ees_ventas_mont_regular")), 0).otherwise(col("imp_ees_ventas_mont_regular")))

// var imp_ees_ventas_olstor = imp_ees_ventas_monterra.withColumn("imp_ees_ventas_olstor", when(isnull(col("imp_ees_ventas_olstor_rg_diesl")), 0).otherwise(col("imp_ees_ventas_olstor_rg_diesl"))
//     + when(isnull(col("imp_ees_ventas_olstor_premium")), 0).otherwise(col("imp_ees_ventas_olstor_premium"))
//     + when(isnull(col("imp_ees_ventas_olstor_regular")), 0).otherwise(col("imp_ees_ventas_olstor_regular")))

// var EESVentas = imp_ees_ventas_olstor.withColumn("imp_ees_ventas", when(isnull(col("imp_ees_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_ees_ventas_go_a_reg_diesel"))
//     + when(isnull(col("imp_ees_ventas_prem_92")), 0).otherwise(col("imp_ees_ventas_prem_92"))
//     + when(isnull(col("imp_ees_ventas_reg_87")), 0).otherwise(col("imp_ees_ventas_reg_87"))
//     + when(isnull(col("imp_ees_ventas_monterra")), 0).otherwise(col("imp_ees_ventas_monterra"))
//     + when(isnull(col("imp_ees_ventas_olstor")), 0).otherwise(col("imp_ees_ventas_olstor")))

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

// var imp_ees_mc_unit_ees_2_extr_mon2 = imp_ees_mc_unit_ees_2_extr_mon.withColumn("imp_ees_mc_unit_ees_2_extr_mon", when(isnull(col("imp_ees_mc_unit_ees_2_extr_mon")), 0).otherwise(col("imp_ees_mc_unit_ees_2_extr_mon")))

// var EESResPersonal = imp_ees_mc_unit_ees_2_extr_mon2.withColumn("imp_ees_pers", when(isnull(col("imp_ees_pers_2_retrib")), 0).otherwise(col("imp_ees_pers_2_retrib"))
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
//     + when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos")))
//    // + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp")))

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
//     - when(isnull(col("imp_ees_cost_log_dist_2_transp")), 0).otherwise(col("imp_ees_cost_log_dist_2_transp")))

// var EESCostesVariablesTotales = EESMargenDeContribucion.withColumn("imp_ees_cv_tot", when(isnull(col("imp_ees_coste_prod")), 0).otherwise(col("imp_ees_coste_prod"))
//     + when(isnull(col("imp_ees_coste_logist_dist")), 0).otherwise(col("imp_ees_coste_logist_dist"))
//     + when(isnull(col("imp_ees_coste_canal")), 0).otherwise(col("imp_ees_coste_canal")))

// var imp_ees_cv_unit =  EESCostesVariablesTotales.withColumn("imp_ees_cv_unit", when(isnull(col("imp_ees_cv_tot")), 0).otherwise(col("imp_ees_cv_tot"))
//     / when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas"))).cache



# **Mov. Mex. Mayorista**

# In[23]:


// var EESMayoVentas = imp_ees_cv_unit.withColumn("imp_ees_mayor_ventas", when(isnull(col("imp_ees_mayor_vta_go_reg_diesl")), 0).otherwise(col("imp_ees_mayor_vta_go_reg_diesl"))
//     + when(isnull(col("imp_ees_mayor_ventas_prem_92")), 0).otherwise(col("imp_ees_mayor_ventas_prem_92"))
//     + when(isnull(col("imp_ees_mayor_ventas_reg_87")), 0).otherwise(col("imp_ees_mayor_ventas_reg_87")))


// var EESMayorMC = EESMayoVentas.withColumn("imp_ees_mayor_mc_ees", when(isnull(col("imp_ees_mayor_mc_ees_2_sin_res")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_sin_res"))
//     + when(isnull(col("imp_ees_mayor_mc_ees_2_res_est")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_res_est"))
//     + when(isnull(col("imp_ees_mayor_mc_ees_2_ext_ols")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_ext_ols"))
//     + when(isnull(col("imp_ees_mayor_mc_ees_2_ext_mon")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_ext_mon")))

// var ESSMayoMCunitario = EESMayorMC.withColumn("imp_ees_mayor_mc_unit_ees", when(isnull(col("imp_ees_mayor_mc_ees")), 0).otherwise(col("imp_ees_mayor_mc_ees"))
//     / when(isnull(col("imp_ees_mayor_ventas")), 0).otherwise(col("imp_ees_mayor_ventas")))

// var EESMayMCunitSinReservas =  ESSMayoMCunitario.withColumn("imp_mayor_mc_uni_ees_2_sres_et", when(isnull(col("imp_ees_mayor_mc_ees_2_sin_res")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_sin_res"))
//     / when(isnull(col("imp_ees_mayor_ventas")), 0).otherwise(col("imp_ees_mayor_ventas")))

// var EESMayMCunitResEstra =  EESMayMCunitSinReservas.withColumn("imp_mayo_mc_uni_ees_2_efres_et", when(isnull(col("imp_ees_mayor_mc_ees_2_res_est")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_res_est"))
//     / when(isnull(col("imp_ees_mayor_ventas")), 0).otherwise(col("imp_ees_mayor_ventas")))

// var  EESMayMCunitExtraMargOlstor =  EESMayMCunitResEstra.withColumn("imp_mayor_mc_uni_ees_2_ext_ols", when(isnull(col("imp_ees_mayor_mc_ees_2_ext_ols")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_ext_ols"))
//     / when(isnull(col("imp_ees_mayor_ventas")), 0).otherwise(col("imp_ees_mayor_ventas")))

// //EES MAYORISTA OLSTOR

// var EESMayMCunitExtraMargMonterra =  EESMayMCunitExtraMargOlstor.withColumn("imp_mayor_mc_uni_ees_2_ext_mon", when(isnull(col("imp_ees_mayor_mc_ees_2_ext_mon")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_ext_mon"))
//     / when(isnull(col("imp_ees_mayor_ventas")), 0).otherwise(col("imp_ees_mayor_ventas")))
// //EES MAYORISTA OLSTOR VENTAS
// var imp_mayor_mc_uni_ees_2_ext_mon =  EESMayMCunitExtraMargMonterra.withColumn("imp_mayor_mc_uni_ees_2_ext_mon", when(isnull(col("imp_mayor_mc_uni_ees_2_ext_mon")), 0).otherwise(col("imp_mayor_mc_uni_ees_2_ext_mon")))

// var EESMayPersonal =  imp_mayor_mc_uni_ees_2_ext_mon.withColumn("imp_ees_mayor_pers", when(isnull(col("imp_ees_mayor_pers_2_retrib")), 0).otherwise(col("imp_ees_mayor_pers_2_retrib"))
//     + when(isnull(col("imp_ees_mayor_pers_2_otro_cost")), 0).otherwise(col("imp_ees_mayor_pers_2_otro_cost")))

// var EESMaySerExt =  EESMayPersonal.withColumn("imp_ees_mayor_serv_ext", when(isnull(col("imp_ees_mayor_sv_ext_2_arrycan")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_arrycan"))
//     + when(isnull(col("imp_ees_mayor_sv_ext_2_pb_rrpp")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_pb_rrpp"))
//     + when(isnull(col("imp_ees_mayor_serv_ext_2_sumin")), 0).otherwise(col("imp_ees_mayor_serv_ext_2_sumin"))
//     + when(isnull(col("imp_ees_mayor_sv_ext_2_man_rep")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_man_rep"))
//     + when(isnull(col("imp_ees_mayor_sv_ext_2_sv_prof")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_sv_prof"))
//     + when(isnull(col("imp_ees_mayor_sv_ext_2_viajes")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_viajes"))
//     + when(isnull(col("imp_ees_mayor_serv_ext_2_seg")), 0).otherwise(col("imp_ees_mayor_serv_ext_2_seg"))
//     + when(isnull(col("imp_ees_mayor_serv_ext_2_trib")), 0).otherwise(col("imp_ees_mayor_serv_ext_2_trib"))
//     + when(isnull(col("imp_ees_mayor_sv_ext_2_otro_sv")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_otro_sv")))

// var EESMayTotalCFConceptos =  EESMaySerExt.withColumn("imp_ees_mayor_tot_cf_concep", when(isnull(col("imp_ees_mayor_pers")), 0).otherwise(col("imp_ees_mayor_pers"))
//     + when(isnull(col("imp_ees_mayor_serv_ext")), 0).otherwise(col("imp_ees_mayor_serv_ext"))
//     + when(isnull(col("imp_ees_mayor_serv_corp")), 0).otherwise(col("imp_ees_mayor_serv_corp"))
//     + when(isnull(col("imp_ees_mayor_serv_transv_DG")), 0).otherwise(col("imp_ees_mayor_serv_transv_DG"))
//     + when(isnull(col("imp_ees_mayor_otros_serv_DG")), 0).otherwise(col("imp_ees_mayor_otros_serv_DG")))

// var EESMayCFEES =  EESMayTotalCFConceptos.withColumn("imp_ees_mayor_cf_ees", when(isnull(col("imp_ees_mayor_cf_ees_2_mante")), 0).otherwise(col("imp_ees_mayor_cf_ees_2_mante"))
//     + when(isnull(col("imp_ees_mayor_cf_ees_2_neotech")), 0).otherwise(col("imp_ees_mayor_cf_ees_2_neotech"))
//     + when(isnull(col("imp_ees_mayor_cf_ees_2_sc_volu")), 0).otherwise(col("imp_ees_mayor_cf_ees_2_sc_volu"))
//     + when(isnull(col("imp_ees_mayor_cf_ees_2_act_prm")), 0).otherwise(col("imp_ees_mayor_cf_ees_2_act_prm"))
//     + when(isnull(col("imp_ees_mayor_cf_ees_2_otros")), 0).otherwise(col("imp_ees_mayor_cf_ees_2_otros")))

// var EESMayCFAnalitica =  EESMayCFEES.withColumn("imp_ees_mayor_tot_cf_analit", when(isnull(col("imp_ees_mayor_cf_ees")), 0).otherwise(col("imp_ees_mayor_cf_ees"))
//     + when(isnull(col("imp_ees_mayor_otros_gastos")), 0).otherwise(col("imp_ees_mayor_otros_gastos")))
//    // + when(isnull(col("imp_ees_mayor_serv_corp")), 0).otherwise(col("imp_ees_mayor_serv_corp")))

// var EESMayCF =  EESMayCFAnalitica.withColumn("imp_ees_mayor_cf", when(isnull(col("imp_ees_mayor_tot_cf_analit")), 0).otherwise(col("imp_ees_mayor_tot_cf_analit")))

// var EESMayResuOpePrin =  EESMayCF.withColumn("imp_ees_mayor_result_op_ppal", when(isnull(col("imp_ees_mayor_mc_ees")), 0).otherwise(col("imp_ees_mayor_mc_ees"))
//     - when(isnull(col("imp_ees_mayor_cf")), 0).otherwise(col("imp_ees_mayor_cf"))
//     - when(isnull(col("imp_ees_mayor_amort")), 0).otherwise(col("imp_ees_mayor_amort"))
//     - when(isnull(col("imp_ees_mayor_provis_recur")), 0).otherwise(col("imp_ees_mayor_provis_recur"))
//     + when(isnull(col("imp_ees_mayor_otros_result")), 0).otherwise(col("imp_ees_mayor_otros_result")))

// // + Otros margenes de contribucion

// var  EESMayResOtrasSoc =  EESMayResuOpePrin.withColumn("imp_ees_mayor_result_otras_soc", when(isnull(col("imp_ees_mayor_EBIT_CCS_JV_cort")), 0).otherwise(col("imp_ees_mayor_EBIT_CCS_JV_cort")))

// var EESMayResOpe =  EESMayResOtrasSoc.withColumn("imp_ees_mayor_result_op", when(isnull(col("imp_ees_mayor_result_op_ppal")), 0).otherwise(col("imp_ees_mayor_result_op_ppal"))
//     + when(isnull(col("imp_ees_mayor_result_otras_soc")), 0).otherwise(col("imp_ees_mayor_result_otras_soc"))).cache


# **Mov. Mex. Minorista**

# In[24]:


// var EESMinoVentas =  EESMayResOpe.withColumn("imp_ees_minor_ventas", when(isnull(col("imp_ees_minor_vta_go_reg_diesl")), 0).otherwise(col("imp_ees_minor_vta_go_reg_diesl"))
//     + when(isnull(col("imp_ees_minor_ventas_prem_92")), 0).otherwise(col("imp_ees_minor_ventas_prem_92"))
//     + when(isnull(col("imp_ees_minor_ventas_reg_87")), 0).otherwise(col("imp_ees_minor_ventas_reg_87")))

// var EESMinoPersonal = EESMinoVentas.withColumn("imp_ees_minor_pers", when(isnull(col("imp_ees_minor_pers_2_retrib")), 0).otherwise(col("imp_ees_minor_pers_2_retrib"))
//     + when(isnull(col("imp_ees_minor_pers_2_otro_cost")), 0).otherwise(col("imp_ees_minor_pers_2_otro_cost")))

// var EESMinoServExternos =  EESMinoPersonal.withColumn("imp_ees_minor_serv_ext", when(isnull(col("imp_ees_minor_sv_ext_2_arrycan")), 0).otherwise(col("imp_ees_minor_sv_ext_2_arrycan"))
//     + when(isnull(col("imp_ees_minor_sv_ext_2_pb_rrpp")), 0).otherwise(col("imp_ees_minor_sv_ext_2_pb_rrpp"))
//     + when(isnull(col("imp_ees_minor_serv_ext_2_sumin")), 0).otherwise(col("imp_ees_minor_serv_ext_2_sumin"))
//     + when(isnull(col("imp_ees_minor_sv_ext_2_man_rep")), 0).otherwise(col("imp_ees_minor_sv_ext_2_man_rep"))
//     + when(isnull(col("imp_ees_minor_sv_ext_2_sv_prof")), 0).otherwise(col("imp_ees_minor_sv_ext_2_sv_prof"))
//     + when(isnull(col("imp_ees_minor_sv_ext_2_viajes")), 0).otherwise(col("imp_ees_minor_sv_ext_2_viajes"))
//     + when(isnull(col("imp_ees_minor_serv_ext_2_seg")), 0).otherwise(col("imp_ees_minor_serv_ext_2_seg"))
//     + when(isnull(col("imp_ees_minor_serv_ext_2_trib")), 0).otherwise(col("imp_ees_minor_serv_ext_2_trib"))
//     + when(isnull(col("imp_ees_minor_sv_ext_2_otro_sv")), 0).otherwise(col("imp_ees_minor_sv_ext_2_otro_sv")))

// var EESMinoTotCFConcepto = EESMinoServExternos.withColumn("imp_ees_minor_tot_cf_concep", when(isnull(col("imp_ees_minor_pers")), 0).otherwise(col("imp_ees_minor_pers"))
//     + when(isnull(col("imp_ees_minor_serv_ext")), 0).otherwise(col("imp_ees_minor_serv_ext"))
//     + when(isnull(col("imp_ees_minor_serv_corp")), 0).otherwise(col("imp_ees_minor_serv_corp"))
//     + when(isnull(col("imp_ees_minor_serv_transv_DG")), 0).otherwise(col("imp_ees_minor_serv_transv_DG"))
//     + when(isnull(col("imp_ees_minor_otros_serv_DG")), 0).otherwise(col("imp_ees_minor_otros_serv_DG")))


// var EESMinoCFEES =  EESMinoTotCFConcepto.withColumn("imp_ees_minor_cf_ees", when(isnull(col("imp_ees_minor_cf_ees_2_man_rep")), 0).otherwise(col("imp_ees_minor_cf_ees_2_man_rep"))
//     + when(isnull(col("imp_ees_minor_cf_ees_2_neotech")), 0).otherwise(col("imp_ees_minor_cf_ees_2_neotech"))
//     + when(isnull(col("imp_ees_minor_cf_ees_2_sc_volu")), 0).otherwise(col("imp_ees_minor_cf_ees_2_sc_volu"))
//     + when(isnull(col("imp_ees_minor_cf_ees_2_act_prm")), 0).otherwise(col("imp_ees_minor_cf_ees_2_act_prm"))
//     + when(isnull(col("imp_ees_minor_cf_ees_2_otro_cf")), 0).otherwise(col("imp_ees_minor_cf_ees_2_otro_cf")))

// var EESMinoTotCFAnalitica = EESMinoCFEES.withColumn("imp_ees_minor_tot_cf_analit", when(isnull(col("imp_ees_minor_cf_ees")), 0).otherwise(col("imp_ees_minor_cf_ees"))
//     + when(isnull(col("imp_ees_minor_otros_gastos")), 0).otherwise(col("imp_ees_minor_otros_gastos")))
//   //  + when(isnull(col("imp_ees_minor_corp")), 0).otherwise(col("imp_ees_minor_corp")))

// var EESMinoCF =  EESMinoTotCFAnalitica.withColumn("imp_ees_minor_cf", when(isnull(col("imp_ees_minor_tot_cf_analit")), 0).otherwise(col("imp_ees_minor_tot_cf_analit")))

// var EESMinoResuOpe =  EESMinoCF.withColumn("imp_ees_minor_result_op", when(isnull(col("imp_ees_minor_otros_mc")), 0).otherwise(col("imp_ees_minor_otros_mc"))
//     - when(isnull(col("imp_ees_minor_cf")), 0).otherwise(col("imp_ees_minor_cf"))
//     - when(isnull(col("imp_ees_minor_amort")), 0).otherwise(col("imp_ees_minor_amort"))
//     - when(isnull(col("imp_ees_minor_provis_recur")), 0).otherwise(col("imp_ees_minor_provis_recur"))
//     + when(isnull(col("imp_ees_minor_otros_result")), 0).otherwise(col("imp_ees_minor_otros_result"))).cache


# **Mov. Mex. VVDD**

# In[25]:


// var VVDDVentas = EESMinoResuOpe.withColumn("imp_vvdd_ventas", when(isnull(col("imp_vvdd_ventas_go_a_reg_diesl")), 0).otherwise(col("imp_vvdd_ventas_go_a_reg_diesl"))
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

// var VVDDCVTotal = VVDDMargenContri.withColumn("imp_vvdd_cv_tot", when(isnull(col("imp_vvdd_coste_prod")), 0).otherwise(col("imp_vvdd_coste_prod")))
// //+10+13

// var VVDDCVUnit = VVDDCVTotal.withColumn("imp_vvdd_cv_unit", when(isnull(col("imp_vvdd_cv_tot")), 0).otherwise(col("imp_vvdd_cv_tot"))
//     / when(isnull(col("imp_vvdd_ventas")), 0).otherwise(col("imp_vvdd_ventas")))

// var imp_vvdd_tot_cf_analit =  VVDDCVUnit.withColumn("imp_vvdd_tot_cf_analit", when(isnull(col("imp_vvdd_otros_gastos")), 0).otherwise(col("imp_vvdd_otros_gastos")))

// var imp_vvdd_cv_tot =  imp_vvdd_tot_cf_analit.withColumn("imp_vvdd_cv_tot", when(isnull(col("imp_vvdd_coste_prod")), 0).otherwise(col("imp_vvdd_coste_prod"))).cache


# **Mov. Mex. Total**

# In[26]:


// var TotMMVentasDiesel =  imp_vvdd_cv_tot.withColumn("imp_tot_ventas_go_a_reg_diesel", when(isnull(col("imp_vvdd_ventas_go_a_reg_diesl")), 0).otherwise(col("imp_vvdd_ventas_go_a_reg_diesl"))
//     + when(isnull(col("imp_ees_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_ees_ventas_go_a_reg_diesel")))
//     //+ when(isnull(col("imp_ees_minor_vta_go_reg_diesl")), 0).otherwise(col("imp_ees_minor_vta_go_reg_diesl"))
//     //+ when(isnull(col("imp_ees_ventas_mont_reg_diesel")), 0).otherwise(col("imp_ees_ventas_mont_reg_diesel"))
//     //+ when(isnull(col("imp_ees_ventas_olstor_rg_diesl")), 0).otherwise(col("imp_ees_ventas_olstor_rg_diesl")))
//     //+ when(isnull(col("imp_ees_mayor_vta_go_reg_diesl")), 0).otherwise(col("imp_ees_mayor_vta_go_reg_diesl")))

// var TotMMVentasPremium =  TotMMVentasDiesel.withColumn("imp_tot_ventas_prem_92", when(isnull(col("imp_vvdd_ventas_prem_92")), 0).otherwise(col("imp_vvdd_ventas_prem_92"))
//     + when(isnull(col("imp_ees_ventas_prem_92")), 0).otherwise(col("imp_ees_ventas_prem_92")))
//     //+ when(isnull(col("imp_ees_minor_ventas_prem_92")), 0).otherwise(col("imp_ees_minor_ventas_prem_92"))
//     //+ when(isnull(col("imp_ees_ventas_mont_premium")), 0).otherwise(col("imp_ees_ventas_mont_premium"))
//     //+ when(isnull(col("imp_ees_ventas_olstor_premium")), 0).otherwise(col("imp_ees_ventas_olstor_premium")))
//     //+ when(isnull(col("imp_ees_mayor_ventas_prem_92")), 0).otherwise(col("imp_ees_mayor_ventas_prem_92")))

// var TotMMVentasReg =  TotMMVentasPremium.withColumn("imp_tot_ventas_reg_87", when(isnull(col("imp_vvdd_ventas_reg_87")), 0).otherwise(col("imp_vvdd_ventas_reg_87"))
//     + when(isnull(col("imp_ees_ventas_reg_87")), 0).otherwise(col("imp_ees_ventas_reg_87")))
//     //+ when(isnull(col("imp_ees_minor_ventas_reg_87")), 0).otherwise(col("imp_ees_minor_ventas_reg_87"))
//     //+ when(isnull(col("imp_ees_ventas_olstor_regular")), 0).otherwise(col("imp_ees_ventas_olstor_regular"))
//     //+ when(isnull(col("imp_ees_ventas_mont_regular")), 0).otherwise(col("imp_ees_ventas_mont_regular")))
//     //+ when(isnull(col("imp_ees_mayor_ventas_reg_87")), 0).otherwise(col("imp_ees_mayor_ventas_reg_87")))

// var imp_total_ventas = TotMMVentasReg.withColumn("imp_total_ventas", when(isnull(col("imp_tot_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_tot_ventas_go_a_reg_diesel"))
//     + when(isnull(col("imp_tot_ventas_prem_92")), 0).otherwise(col("imp_tot_ventas_prem_92"))
//     + when(isnull(col("imp_tot_ventas_reg_87")), 0).otherwise(col("imp_tot_ventas_reg_87")))

// var TotMMIngNeto =  imp_total_ventas.withColumn("imp_tot_mm_ing_net", when(isnull(col("imp_ees_ing_net")), 0).otherwise(col("imp_ees_ing_net")))

// var TotMMCosteProdMP =  TotMMIngNeto.withColumn("imp_tot_mm_coste_prod_2_m_prim", when(isnull(col("imp_ees_coste_prod_2_mat_prima")), 0).otherwise(col("imp_ees_coste_prod_2_mat_prima")))

// var TotMMCosteProdAdit =  TotMMCosteProdMP.withColumn("imp_tot_mm_coste_prod_2_adt", when(isnull(col("imp_ees_coste_prod_2_aditivos")), 0).otherwise(col("imp_ees_coste_prod_2_aditivos")))

// var TotMMCosteProdMermas =  TotMMCosteProdAdit.withColumn("imp_tot_mm_coste_prod_2_mermas", when(isnull(col("imp_ees_coste_prod_2_mermas")), 0).otherwise(col("imp_ees_coste_prod_2_mermas")))

// var TotMMCosteProdDesc =  TotMMCosteProdMermas.withColumn("imp_tot_mm_coste_prod_2_desc", when(isnull(col("imp_ees_coste_prod_2_desc_comp")), 0).otherwise(col("imp_ees_coste_prod_2_desc_comp")))

// var TotMMCosteProd =  TotMMCosteProdDesc.withColumn("imp_tot_mm_coste_prod", when(isnull(col("imp_tot_mm_coste_prod_2_m_prim")), 0).otherwise(col("imp_tot_mm_coste_prod_2_m_prim"))
//     + when(isnull(col("imp_tot_mm_coste_prod_2_adt")), 0).otherwise(col("imp_tot_mm_coste_prod_2_adt"))
//     + when(isnull(col("imp_tot_mm_coste_prod_2_mermas")), 0).otherwise(col("imp_tot_mm_coste_prod_2_mermas"))
//     + when(isnull(col("imp_tot_mm_coste_prod_2_desc")), 0).otherwise(col("imp_tot_mm_coste_prod_2_desc")))

// var TotMMMB =  TotMMCosteProd.withColumn("imp_tot_mm_mb", when(isnull(col("imp_tot_mm_ing_net")), 0).otherwise(col("imp_tot_mm_ing_net"))
//     - when(isnull(col("imp_tot_mm_coste_prod")), 0).otherwise(col("imp_tot_mm_coste_prod")))

// var TotMMCLTransp =  TotMMMB.withColumn("imp_tot_mm_cost_log_dist_2_tsp", when(isnull(col("imp_ees_coste_prod_2_desc_comp")), 0).otherwise(col("imp_ees_coste_prod_2_desc_comp")))

// var TotMMCosteLogistDist =  TotMMCLTransp.withColumn("imp_tot_mm_coste_logist_dist", when(isnull(col("imp_tot_mm_cost_log_dist_2_tsp")), 0).otherwise(col("imp_tot_mm_cost_log_dist_2_tsp")))

// var TotMMExtramOlstor =  TotMMCosteLogistDist.withColumn("imp_tot_mm_extram_olstor", when(isnull(col("imp_ees_extram_olstor")), 0).otherwise(col("imp_ees_extram_olstor")))

// var TotMMExtramMonterra =  TotMMExtramOlstor.withColumn("imp_tot_mm_extram_monterra", when(isnull(col("imp_ees_extram_monterra")), 0).otherwise(col("imp_ees_extram_monterra")))

// var TotMMCC =  TotMMExtramMonterra.withColumn("imp_tot_mm_coste_canal", when(isnull(col("imp_tot_mm_extram_olstor")), 0).otherwise(col("imp_tot_mm_extram_olstor"))
//     + when(isnull(col("imp_tot_mm_extram_monterra")), 0).otherwise(col("imp_tot_mm_extram_monterra")))

// var TotMMMC =  TotMMCC.withColumn("imp_tot_mm_mc", when(isnull(col("imp_tot_mm_mb")), 0).otherwise(col("imp_tot_mm_mb"))
//     - when(isnull(col("imp_tot_mm_coste_logist_dist")), 0).otherwise(col("imp_tot_mm_coste_logist_dist"))
//     - when(isnull(col("imp_tot_mm_coste_canal")), 0).otherwise(col("imp_tot_mm_coste_canal")))

// var TotMMMBSinDesc_cv =  TotMMMC.withColumn("imp_tot_mm_mb_sin_desc_cv", when(isnull(col("imp_ees_mb_sin_des_ing_c_prima")), 0).otherwise(col("imp_ees_mb_sin_des_ing_c_prima")))

// var TotMMCVTot =  TotMMMBSinDesc_cv.withColumn("imp_tot_mm_cv_tot", when(isnull(col("imp_tot_mm_coste_prod")), 0).otherwise(col("imp_tot_mm_coste_prod"))
//     + when(isnull(col("imp_tot_mm_coste_logist_dist")), 0).otherwise(col("imp_tot_mm_coste_logist_dist"))
//     + when(isnull(col("imp_tot_mm_coste_canal")), 0).otherwise(col("imp_tot_mm_coste_canal")))

// var TotMMCVunit =  TotMMCVTot.withColumn("imp_tot_mm_cv_unit", when(isnull(col("imp_tot_mm_cv_tot")), 0).otherwise(col("imp_tot_mm_cv_tot"))
//     / when(isnull(col("imp_vvdd_ventas")), 0).otherwise(col("imp_vvdd_ventas")))

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


// var imp_tot_mm_cf =  imp_tot_mm_otros_mc.withColumn("imp_tot_mm_cf", when(isnull(col("imp_ees_cf")), 0).otherwise(col("imp_ees_cf"))
//     + when(isnull(col("imp_ctral_cost_fijos")), 0).otherwise(col("imp_ctral_cost_fijos")))

// var imp_tot_mm_amort =  imp_tot_mm_cf.withColumn("imp_tot_mm_amort", when(isnull(col("imp_ees_amort")), 0).otherwise(col("imp_ees_amort"))
//     + when(isnull(col("imp_vvdd_amort")), 0).otherwise(col("imp_vvdd_amort"))
//     + when(isnull(col("imp_ctral_amort")), 0).otherwise(col("imp_ctral_amort")))

// var imp_tot_mm_provis_recur =  imp_tot_mm_amort.withColumn("imp_tot_mm_provis_recur", when(isnull(col("imp_ees_provis_recur")), 0).otherwise(col("imp_ees_provis_recur"))
//     + when(isnull(col("imp_ctral_provis_recur")), 0).otherwise(col("imp_ctral_provis_recur"))
//     + when(isnull(col("imp_vvdd_provis_recur")), 0).otherwise(col("imp_vvdd_provis_recur"))
// )

// var imp_tot_mm_otros_result =  imp_tot_mm_provis_recur.withColumn("imp_tot_mm_otros_result", when(isnull(col("imp_ees_otros_result")), 0).otherwise(col("imp_ees_otros_result"))
//     + when(isnull(col("imp_vvdd_otros_result")), 0).otherwise(col("imp_vvdd_otros_result"))
//     + when(isnull(col("imp_ctral_otros_result")), 0).otherwise(col("imp_ctral_otros_result"))
// )

// var imp_tot_mm_result_op_ppal =  imp_tot_mm_otros_result.withColumn("imp_tot_mm_result_op_ppal", when(isnull(col("imp_tot_mm_mc_ees")), 0).otherwise(col("imp_tot_mm_mc_ees"))
//     + when(isnull(col("imp_tot_mm_otros_mc")), 0).otherwise(col("imp_tot_mm_otros_mc"))
//     - when(isnull(col("imp_tot_mm_cf")), 0).otherwise(col("imp_tot_mm_cf"))
//     - when(isnull(col("imp_tot_mm_amort")), 0).otherwise(col("imp_tot_mm_amort"))
//     - when(isnull(col("imp_tot_mm_provis_recur")), 0).otherwise(col("imp_tot_mm_provis_recur"))
//     + when(isnull(col("imp_tot_mm_otros_result")), 0).otherwise(col("imp_tot_mm_otros_result")))

// var imp_tot_mm_result_otras_soc = imp_tot_mm_result_op_ppal.withColumn("imp_tot_mm_result_otras_soc", when(isnull(col("imp_ees_EBIT_CCS_JV_mar_cortes")),0).otherwise(col("imp_ees_EBIT_CCS_JV_mar_cortes")))

// var imp_tot_mm_result_op =  imp_tot_mm_result_otras_soc.withColumn("imp_tot_mm_result_op", when(isnull(col("imp_tot_mm_result_op_ppal")), 0).otherwise(col("imp_tot_mm_result_op_ppal"))
//      + when(isnull(col("imp_tot_mm_result_otras_soc")), 0).otherwise(col("imp_tot_mm_result_otras_soc")))
      

// var imp_tot_mm_pers_2_retrib =  imp_tot_mm_result_op.withColumn("imp_tot_mm_pers_2_retrib", when(isnull(col("imp_ctral_pers_2_retrib")), 0).otherwise(col("imp_ctral_pers_2_retrib"))
//      + when(isnull(col("imp_vvdd_pers_2_retrib")), 0).otherwise(col("imp_vvdd_pers_2_retrib"))
//      + when(isnull(col("imp_ees_pers_2_retrib")), 0).otherwise(col("imp_ees_pers_2_retrib")))//añadido

// var imp_tot_mm_pers_2_otro_cost =  imp_tot_mm_pers_2_retrib.withColumn("imp_tot_mm_pers_2_otro_cost", when(isnull(col("imp_ctral_pers_2_otros_cost")), 0).otherwise(col("imp_ctral_pers_2_otros_cost"))
//      + when(isnull(col("imp_ees_pers_2_otros_cost_pers")), 0).otherwise(col("imp_ees_pers_2_otros_cost_pers"))
//      + when(isnull(col("imp_vvdd_pers_2_otro_cost_pers")), 0).otherwise(col("imp_vvdd_pers_2_otro_cost_pers")))//añadido

// var imp_tot_mm_pers =  imp_tot_mm_pers_2_otro_cost.withColumn("imp_tot_mm_pers", when(isnull(col("imp_tot_mm_pers_2_retrib")), 0).otherwise(col("imp_tot_mm_pers_2_retrib"))
//     + when(isnull(col("imp_tot_mm_pers_2_otro_cost")), 0).otherwise(col("imp_tot_mm_pers_2_otro_cost")))

// var imp_tot_mm_serv_ext_2_arrycan =  imp_tot_mm_pers.withColumn("imp_tot_mm_serv_ext_2_arrycan", when(isnull(col("imp_ctral_serv_ext_2_arrycan")), 0).otherwise(col("imp_ctral_serv_ext_2_arrycan"))
//      + when(isnull(col("imp_vvdd_serv_ext_2_arrend_can")), 0).otherwise(col("imp_vvdd_serv_ext_2_arrend_can"))
//      + when(isnull(col("imp_ees_serv_ext_2_arrend_can")), 0).otherwise(col("imp_ees_serv_ext_2_arrend_can"))//añadido
// )

// var imp_tot_mm_serv_ext_2_pub_rrpp =  imp_tot_mm_serv_ext_2_arrycan.withColumn("imp_tot_mm_serv_ext_2_pub_rrpp", when(isnull(col("imp_ctral_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_ctral_serv_ext_2_pub_rrpp"))
//      + when(isnull(col("imp_vvdd_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_vvdd_serv_ext_2_pub_rrpp"))
//      + when(isnull(col("imp_ees_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_ees_serv_ext_2_pub_rrpp"))//añadido
// )

// var imp_tot_mm_serv_ext_2_sumin =  imp_tot_mm_serv_ext_2_pub_rrpp.withColumn("imp_tot_mm_serv_ext_2_sumin", when(isnull(col("imp_ctral_serv_ext_2_sumin")), 0).otherwise(col("imp_ctral_serv_ext_2_sumin"))
//      + when(isnull(col("imp_ees_serv_ext_2_sumin")), 0).otherwise(col("imp_ees_serv_ext_2_sumin"))
//      + when(isnull(col("imp_vvdd_serv_ext_2_sumin")), 0).otherwise(col("imp_vvdd_serv_ext_2_sumin"))//añadido
// )

// var imp_tot_mm_serv_ext_2_mant_rep =  imp_tot_mm_serv_ext_2_sumin.withColumn("imp_tot_mm_serv_ext_2_mant_rep", when(isnull(col("imp_ctral_serv_ext_2_mant_rep")), 0).otherwise(col("imp_ctral_serv_ext_2_mant_rep"))
//      + when(isnull(col("imp_vvdd_serv_ext_2_mant_rep")), 0).otherwise(col("imp_vvdd_serv_ext_2_mant_rep"))
//      + when(isnull(col("imp_ees_serv_ext_2_mant_rep")), 0).otherwise(col("imp_ees_serv_ext_2_mant_rep"))//añadido
// )

// var imp_tot_mm_serv_ext_2_sv_prof =  imp_tot_mm_serv_ext_2_mant_rep.withColumn("imp_tot_mm_serv_ext_2_sv_prof", when(isnull(col("imp_ctral_serv_ext_2_serv_prof")), 0).otherwise(col("imp_ctral_serv_ext_2_serv_prof"))
//      + when(isnull(col("imp_ees_serv_ext_2_serv_prof")), 0).otherwise(col("imp_ees_serv_ext_2_serv_prof"))
//      + when(isnull(col("imp_vvdd_serv_ext_2_serv_prof")), 0).otherwise(col("imp_vvdd_serv_ext_2_serv_prof"))//añadido
// )

// var imp_tot_mm_serv_ext_2_viajes =  imp_tot_mm_serv_ext_2_sv_prof.withColumn("imp_tot_mm_serv_ext_2_viajes", when(isnull(col("imp_ctral_serv_ext_2_viajes")), 0).otherwise(col("imp_ctral_serv_ext_2_viajes"))
//      + when(isnull(col("imp_ees_serv_ext_2_viajes")), 0).otherwise(col("imp_ees_serv_ext_2_viajes"))
//      + when(isnull(col("imp_vvdd_serv_ext_2_viajes")), 0).otherwise(col("imp_vvdd_serv_ext_2_viajes"))//añadido
// )

// var imp_tot_mm_serv_ext_2_seg =  imp_tot_mm_serv_ext_2_viajes.withColumn("imp_tot_mm_serv_ext_2_seg", when(isnull(col("imp_ctral_serv_ext_2_seg")), 0).otherwise(col("imp_ctral_serv_ext_2_seg"))
//      + when(isnull(col("imp_vvdd_serv_ext_2_seg")), 0).otherwise(col("imp_vvdd_serv_ext_2_seg"))
//      + when(isnull(col("imp_ees_serv_ext_2_seg")), 0).otherwise(col("imp_ees_serv_ext_2_seg"))//añadido
// )

// var imp_tot_mm_serv_ext_2_trib =  imp_tot_mm_serv_ext_2_seg.withColumn("imp_tot_mm_serv_ext_2_trib", when(isnull(col("imp_ctral_serv_ext_2_trib")), 0).otherwise(col("imp_ctral_serv_ext_2_trib"))
//      + when(isnull(col("imp_ees_serv_ext_2_trib")), 0).otherwise(col("imp_ees_serv_ext_2_trib"))
//      + when(isnull(col("imp_vvdd_serv_ext_2_trib")), 0).otherwise(col("imp_vvdd_serv_ext_2_trib"))//añadido
// )

// var imp_tot_mm_serv_ext_2_otro_sv =  imp_tot_mm_serv_ext_2_trib.withColumn("imp_tot_mm_serv_ext_2_otro_sv", when(isnull(col("imp_ctral_sv_ext_2_otr_sv_ext")), 0).otherwise(col("imp_ctral_sv_ext_2_otr_sv_ext"))
//      + when(isnull(col("imp_vvdd_sv_ext_2_otros_sv_ext")), 0).otherwise(col("imp_vvdd_sv_ext_2_otros_sv_ext"))
//      + when(isnull(col("imp_ees_sv_ext_2_otros_sv_ext")), 0).otherwise(col("imp_ees_sv_ext_2_otros_sv_ext"))//añadido
// )

// var imp_tot_mm_serv_ext =  imp_tot_mm_serv_ext_2_otro_sv.withColumn("imp_tot_mm_serv_ext", when(isnull(col("imp_tot_mm_serv_ext_2_arrycan")), 0).otherwise(col("imp_tot_mm_serv_ext_2_arrycan"))
//     + when(isnull(col("imp_tot_mm_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_tot_mm_serv_ext_2_pub_rrpp"))
//     + when(isnull(col("imp_tot_mm_serv_ext_2_sumin")), 0).otherwise(col("imp_tot_mm_serv_ext_2_sumin"))
//     + when(isnull(col("imp_tot_mm_serv_ext_2_mant_rep")), 0).otherwise(col("imp_tot_mm_serv_ext_2_mant_rep"))
//     + when(isnull(col("imp_tot_mm_serv_ext_2_sv_prof")), 0).otherwise(col("imp_tot_mm_serv_ext_2_sv_prof"))
//     + when(isnull(col("imp_tot_mm_serv_ext_2_viajes")), 0).otherwise(col("imp_tot_mm_serv_ext_2_viajes"))
//     + when(isnull(col("imp_tot_mm_serv_ext_2_seg")), 0).otherwise(col("imp_tot_mm_serv_ext_2_seg"))
//     + when(isnull(col("imp_tot_mm_serv_ext_2_trib")), 0).otherwise(col("imp_tot_mm_serv_ext_2_trib"))
//     + when(isnull(col("imp_tot_mm_serv_ext_2_otro_sv")), 0).otherwise(col("imp_tot_mm_serv_ext_2_otro_sv")))

// var imp_tot_mm_serv_corp =  imp_tot_mm_serv_ext.withColumn("imp_tot_mm_serv_corp", when(isnull(col("imp_ctral_serv_corp")), 0).otherwise(col("imp_ctral_serv_corp"))
//      + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp"))
//      + when(isnull(col("imp_vvdd_serv_corp")), 0).otherwise(col("imp_vvdd_serv_corp"))//añadido
// )

// var imp_tot_mm_serv_transv_DG =  imp_tot_mm_serv_corp.withColumn("imp_tot_mm_serv_transv_DG", when(isnull(col("imp_ctral_serv_transv_DG")), 0).otherwise(col("imp_ctral_serv_transv_DG"))
//      + when(isnull(col("imp_ees_serv_transv_DG")), 0).otherwise(col("imp_ees_serv_transv_DG"))
//      + when(isnull(col("imp_vvdd_serv_transv_DG")), 0).otherwise(col("imp_vvdd_serv_transv_DG"))//añadido
// )

// var imp_tot_mm_otros_serv_DG =  imp_tot_mm_serv_transv_DG.withColumn("imp_tot_mm_otros_serv_DG", when(isnull(col("imp_ctral_otros_serv_DG")), 0).otherwise(col("imp_ctral_otros_serv_DG"))
//      + when(isnull(col("imp_vvdd_otros_serv_DG")), 0).otherwise(col("imp_vvdd_otros_serv_DG"))
//      + when(isnull(col("imp_ees_otros_serv_DG")), 0).otherwise(col("imp_ees_otros_serv_DG"))//añadido
// )

// var imp_tot_mm_tot_cf_concep =  imp_tot_mm_otros_serv_DG.withColumn("imp_tot_mm_tot_cf_concep", when(isnull(col("imp_tot_mm_pers")), 0).otherwise(col("imp_tot_mm_pers"))
//     + when(isnull(col("imp_tot_mm_serv_ext")), 0).otherwise(col("imp_tot_mm_serv_ext"))
//     + when(isnull(col("imp_tot_mm_serv_corp")), 0).otherwise(col("imp_tot_mm_serv_corp"))
//     + when(isnull(col("imp_tot_mm_serv_transv_DG")), 0).otherwise(col("imp_tot_mm_serv_transv_DG"))
//     + when(isnull(col("imp_tot_mm_otros_serv_DG")), 0).otherwise(col("imp_tot_mm_otros_serv_DG")))

// var imp_tot_mm_cf_ees_2_mant_rep =  imp_tot_mm_tot_cf_concep.withColumn("imp_tot_mm_cf_ees_2_mant_rep", when(isnull(col("imp_ees_cf_ees_2_mant_rep")), 0).otherwise(col("imp_ees_cf_ees_2_mant_rep")))

// var imp_tot_mm_cf_ees_2_neotech =  imp_tot_mm_cf_ees_2_mant_rep.withColumn("imp_tot_mm_cf_ees_2_neotech", when(isnull(col("imp_ees_cf_ees_2_tec_neotech")), 0).otherwise(col("imp_ees_cf_ees_2_tec_neotech")))

// var imp_tot_mm_cf_ees_2_ctrl_volu =  imp_tot_mm_cf_ees_2_neotech.withColumn("imp_tot_mm_cf_ees_2_ctrl_volu", when(isnull(col("imp_ees_cf_ees_2_sis_ctrl_volu")), 0).otherwise(col("imp_ees_cf_ees_2_sis_ctrl_volu")))

// var imp_tot_mm_cf_ees_2_act_promo =  imp_tot_mm_cf_ees_2_ctrl_volu.withColumn("imp_tot_mm_cf_ees_2_act_promo", when(isnull(col("imp_ees_cf_ees_2_act_promo")), 0).otherwise(col("imp_ees_cf_ees_2_act_promo")))

// var imp_tot_mm_cf_ees_2_otro_cf =  imp_tot_mm_cf_ees_2_act_promo.withColumn("imp_tot_mm_cf_ees_2_otro_cf", when(isnull(col("imp_ees_cf_ees_2_otros_cf_ees")), 0).otherwise(col("imp_ees_cf_ees_2_otros_cf_ees")))

// var imp_tot_mm_cf_ees =  imp_tot_mm_cf_ees_2_otro_cf.withColumn("imp_tot_mm_cf_ees", when(isnull(col("imp_tot_mm_cf_ees_2_mant_rep")), 0).otherwise(col("imp_tot_mm_cf_ees_2_mant_rep"))
//     + when(isnull(col("imp_tot_mm_cf_ees_2_neotech")), 0).otherwise(col("imp_tot_mm_cf_ees_2_neotech"))
//     + when(isnull(col("imp_tot_mm_cf_ees_2_ctrl_volu")), 0).otherwise(col("imp_tot_mm_cf_ees_2_ctrl_volu"))
//     + when(isnull(col("imp_tot_mm_cf_ees_2_act_promo")), 0).otherwise(col("imp_tot_mm_cf_ees_2_act_promo"))
//     + when(isnull(col("imp_tot_mm_cf_ees_2_otro_cf")), 0).otherwise(col("imp_tot_mm_cf_ees_2_otro_cf")))

// var imp_tot_mm_cf_estr_2_pers =  imp_tot_mm_cf_ees.withColumn("imp_tot_mm_cf_estr_2_pers", when(isnull(col("imp_ctral_cf_est_2_pers")), 0).otherwise(col("imp_ctral_cf_est_2_pers")))

// var imp_tot_mm_cf_estr_2_viajes =  imp_tot_mm_cf_estr_2_pers.withColumn("imp_tot_mm_cf_estr_2_viajes", when(isnull(col("imp_ctral_serv_ext_2_viajes")), 0).otherwise(col("imp_ctral_serv_ext_2_viajes")))

// var imp_tot_mm_cf_estr_2_com_rrpp =  imp_tot_mm_cf_estr_2_viajes.withColumn("imp_tot_mm_cf_estr_2_com_rrpp", when(isnull(col("imp_ctral_cf_est_2_com_rrpp")), 0).otherwise(col("imp_ctral_cf_est_2_com_rrpp")))

// var imp_tot_mm_cf_estr_2_serv_prof =  imp_tot_mm_cf_estr_2_com_rrpp.withColumn("imp_tot_mm_cf_estr_2_serv_prof", when(isnull(col("imp_ctral_cf_est_2_serv_prof")), 0).otherwise(col("imp_ctral_cf_est_2_serv_prof")))

// var imp_tot_mm_cf_estr_2_prima_seg =  imp_tot_mm_cf_estr_2_serv_prof.withColumn("imp_tot_mm_cf_estr_2_prima_seg", when(isnull(col("imp_ctral_cf_est_2_primas_seg")), 0).otherwise(col("imp_ctral_cf_est_2_primas_seg")))

// var imp_tot_mm_cf_estr_2_sv_banc_s =  imp_tot_mm_cf_estr_2_prima_seg.withColumn("imp_tot_mm_cf_estr_2_sv_banc_s", when(isnull(col("imp_ctral_cf_est_2_serv_banc")), 0).otherwise(col("imp_ctral_cf_est_2_serv_banc")))

// var imp_tot_mm_cf_estr_2_pub_rrpp =  imp_tot_mm_cf_estr_2_sv_banc_s.withColumn("imp_tot_mm_cf_estr_2_pub_rrpp", when(isnull(col("imp_ctral_cf_est_2_pub_rrpp")), 0).otherwise(col("imp_ctral_cf_est_2_pub_rrpp")))

// var imp_tot_mm_cf_estr_2_sumin =  imp_tot_mm_cf_estr_2_pub_rrpp.withColumn("imp_tot_mm_cf_estr_2_sumin", when(isnull(col("imp_ctral_cf_est_2_sumin")), 0).otherwise(col("imp_ctral_cf_est_2_sumin")))

// var imp_tot_mm_cf_estr_2_otros_sv =  imp_tot_mm_cf_estr_2_sumin.withColumn("imp_tot_mm_cf_estr_2_otros_sv", when(isnull(col("imp_ctral_cf_est_2_otros_serv")), 0).otherwise(col("imp_ctral_cf_est_2_otros_serv")))

// var imp_tot_mm_cf_estr =  imp_tot_mm_cf_estr_2_otros_sv.withColumn("imp_tot_mm_cf_estr", when(isnull(col("imp_tot_mm_cf_estr_2_pers")), 0).otherwise(col("imp_tot_mm_cf_estr_2_pers"))
//     + when(isnull(col("imp_tot_mm_cf_estr_2_viajes")), 0).otherwise(col("imp_tot_mm_cf_estr_2_viajes"))
//     + when(isnull(col("imp_tot_mm_cf_estr_2_com_rrpp")), 0).otherwise(col("imp_tot_mm_cf_estr_2_com_rrpp"))
//     + when(isnull(col("imp_tot_mm_cf_estr_2_serv_prof")), 0).otherwise(col("imp_tot_mm_cf_estr_2_serv_prof"))
//     + when(isnull(col("imp_tot_mm_cf_estr_2_prima_seg")), 0).otherwise(col("imp_tot_mm_cf_estr_2_prima_seg"))
//     + when(isnull(col("imp_tot_mm_cf_estr_2_sv_banc_s")), 0).otherwise(col("imp_tot_mm_cf_estr_2_sv_banc_s"))
//     + when(isnull(col("imp_tot_mm_cf_estr_2_pub_rrpp")), 0).otherwise(col("imp_tot_mm_cf_estr_2_pub_rrpp"))
//     + when(isnull(col("imp_tot_mm_cf_estr_2_sumin")), 0).otherwise(col("imp_tot_mm_cf_estr_2_sumin"))
//     + when(isnull(col("imp_tot_mm_cf_estr_2_otros_sv")), 0).otherwise(col("imp_tot_mm_cf_estr_2_otros_sv")))

// var imp_tot_mm_otros_gastos =  imp_tot_mm_cf_estr.withColumn("imp_tot_mm_otros_gastos", when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos"))
//     + when(isnull(col("imp_vvdd_otros_gastos")), 0).otherwise(col("imp_vvdd_otros_gastos"))
//     + when(isnull(col("imp_ctral_otros_gastos")), 0).otherwise(col("imp_ctral_otros_gastos")))

// // var imp_tot_mm_corp =  imp_tot_mm_otros_gastos.withColumn("imp_tot_mm_corp", when(isnull(col("imp_ees_corp")), 0).otherwise(col("imp_ees_corp"))
// //     + when(isnull(col("imp_ees_minor_corp")), 0).otherwise(col("imp_ees_minor_corp"))
// //     + when(isnull(col("imp_ees_mayor_corp")), 0).otherwise(col("imp_ees_mayor_corp")))

// // imp_tot_mm_result_net_ajust = imp_tot_mm_result_op + imp_tot_mm_result_op_2_particip_minor - imp_tot_mm_result_op_2_imp

// // imp_tot_mm_result_net = imp_tot_mm_result_net_ajust + imp_tot_mm_result_net_ajust_2_result_especif_ddi + imp_tot_mm_result_net_ajust_2_ef_patrim_ddi

// //Real y PA
// var imp_tot_mm_tot_cf_analit =  imp_tot_mm_otros_gastos.withColumn("imp_tot_mm_tot_cf_analit", when(isnull(col("imp_tot_mm_cf_ees")), 0).otherwise(col("imp_tot_mm_cf_ees"))
//     + when(isnull(col("imp_tot_mm_cf_estr")), 0).otherwise(col("imp_tot_mm_cf_estr"))
//     + when(isnull(col("imp_tot_mm_otros_gastos")), 0).otherwise(col("imp_tot_mm_otros_gastos"))
//     + when(isnull(col("imp_tot_mm_serv_corp")), 0).otherwise(col("imp_tot_mm_serv_corp")))

// //UPA y Estimado
// var imp_tot_mm_tot_cf_an_upa_est = imp_tot_mm_tot_cf_analit.withColumn("imp_tot_mm_tot_cf_an_upa_est", when(isnull(col("imp_ctral_cost_fij_estruct")), 0).otherwise(col("imp_ctral_cost_fij_estruct"))
//     + when(isnull(col("imp_tot_mm_otros_gastos")), 0).otherwise(col("imp_tot_mm_otros_gastos"))
//     + when(isnull(col("imp_tot_mm_cf_ees")), 0).otherwise(col("imp_tot_mm_cf_ees"))).cache


# **Incorporacion BFC/BPC/SERVICIOS DG**

# In[27]:


// //Para Mexico EES
// //REAL
// var imp_ees_bfc_real_serv_corp = imp_tot_mm_tot_cf_an_upa_est.withColumn("imp_ees_bfc_real_serv_corp",when(isnull(col("imp_2049_imp_pyo")), 0).otherwise(col("imp_2049_imp_pyo"))
//     + when(isnull(col("imp_2049_imp_patrimonial_seg")), 0).otherwise(col("imp_2049_imp_patrimonial_seg"))
//     + when(isnull(col("imp_2049_imp_comunicacion")), 0).otherwise(col("imp_2049_imp_comunicacion"))
//     + when(isnull(col("imp_2049_imp_techlab")), 0).otherwise(col("imp_2049_imp_techlab"))
//     + when(isnull(col("imp_2049_imp_ti")), 0).otherwise(col("imp_2049_imp_ti"))
//     //+ when(isnull(col("imp_2049_imp_ti_as_a_service")), 0).otherwise(col("imp_2049_imp_ti_as_a_service"))
//     + when(isnull(col("imp_2049_imp_juridico")), 0).otherwise(col("imp_2049_imp_juridico"))
//     + when(isnull(col("imp_2049_imp_ingenieria")), 0).otherwise(col("imp_2049_imp_ingenieria"))
//     + when(isnull(col("imp_2049_imp_ser_corporativos")), 0).otherwise(col("imp_2049_imp_ser_corporativos"))
//     + when(isnull(col("imp_2049_imp_digitalizacion")), 0).otherwise(col("imp_2049_imp_digitalizacion"))
//     + when(isnull(col("imp_2049_imp_sostenibilidad")), 0).otherwise(col("imp_2049_imp_sostenibilidad"))
//     + when(isnull(col("imp_2049_imp_seguros")), 0).otherwise(col("imp_2049_imp_seguros"))
//     + when(isnull(col("imp_2049_imp_auditoria")), 0).otherwise(col("imp_2049_imp_auditoria"))
//     + when(isnull(col("imp_2049_imp_planif_control")), 0).otherwise(col("imp_2049_imp_planif_control"))
//     + when(isnull(col("imp_2049_imp_compras")), 0).otherwise(col("imp_2049_imp_compras"))
//     + when(isnull(col("imp_2049_imp_financiero")), 0).otherwise(col("imp_2049_imp_financiero"))
//     + when(isnull(col("imp_2049_imp_otros")), 0).otherwise(col("imp_2049_imp_otros"))
//     + when(isnull(col("imp_2049_imp_fiscal")), 0).otherwise(col("imp_2049_imp_fiscal"))
//     + when(isnull(col("imp_2049_imp_econom_admin")), 0).otherwise(col("imp_2049_imp_econom_admin"))
//     + when(isnull(col("imp_2049_imp_serv_globales")), 0).otherwise(col("imp_2049_imp_serv_globales")).cast(DecimalType(30,10)))

// var imp_ees_bfc_real_tot_cf_concep = imp_ees_bfc_real_serv_corp.withColumn("imp_ees_bfc_real_tot_cf_concep", when(isnull(col("imp_ees_pers")), 0).otherwise(col("imp_ees_pers"))
//     + when(isnull(col("imp_ees_serv_ext")), 0).otherwise(col("imp_ees_serv_ext"))
//     + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp")) // usar ser crp del fichero
//     + when(isnull(col("imp_ees_serv_transv_DG")), 0).otherwise(col("imp_ees_serv_transv_DG"))
//     + when(isnull(col("imp_ees_otros_serv_DG")), 0).otherwise(col("imp_ees_otros_serv_DG")))

// var imp_ees_bfc_real_tot_cf_analit = imp_ees_bfc_real_tot_cf_concep.withColumn("imp_ees_bfc_real_tot_cf_analit", when(isnull(col("imp_ees_cf_ees")), 0).otherwise(col("imp_ees_cf_ees"))
//     + when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos")))
//   //  + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp")))

// var imp_ees_bfc_real_result_op_ppl = imp_ees_bfc_real_tot_cf_analit.withColumn("imp_ees_bfc_real_result_op_ppl", when(isnull(col("imp_ees_mc_ees")), 0).otherwise(col("imp_ees_mc_ees"))
//     + when(isnull(col("imp_ees_otros_mc")), 0).otherwise(col("imp_ees_otros_mc"))
//     - when(isnull(col("imp_ees_bfc_real_tot_cf_analit")), 0).otherwise(col("imp_ees_bfc_real_tot_cf_analit"))
//     - when(isnull(col("imp_ees_amort")), 0).otherwise(col("imp_ees_amort"))
//     - when(isnull(col("imp_ees_provis_recur")), 0).otherwise(col("imp_ees_provis_recur"))
//     + when(isnull(col("imp_ees_otros_result")), 0).otherwise(col("imp_ees_otros_result")))

// var imp_ees_bfc_real_result_op = imp_ees_bfc_real_result_op_ppl.withColumn("imp_ees_bfc_real_result_op", when(isnull(col("imp_ees_bfc_real_result_op_ppl")), 0).otherwise(col("imp_ees_bfc_real_result_op_ppl"))
//     + when(isnull(col("imp_ees_result_otras_soc")), 0).otherwise(col("imp_ees_result_otras_soc")))


// //PA
// var imp_ees_bfc_pa_serv_corp = imp_ees_bfc_real_result_op.withColumn("imp_ees_bfc_pa_serv_corp",when(isnull(col("imp_2049_imp_pyo")), 0).otherwise(col("imp_2049_imp_pyo"))
//     + when(isnull(col("imp_2049_imp_patrimonial_seg")), 0).otherwise(col("imp_2049_imp_patrimonial_seg"))
//     + when(isnull(col("imp_2049_imp_comunicacion")), 0).otherwise(col("imp_2049_imp_comunicacion"))
//     + when(isnull(col("imp_2049_imp_techlab")), 0).otherwise(col("imp_2049_imp_techlab"))
//     + when(isnull(col("imp_2049_imp_ti")), 0).otherwise(col("imp_2049_imp_ti"))
//     //+ when(isnull(col("imp_2049_imp_ti_as_a_service")), 0).otherwise(col("imp_2049_imp_ti_as_a_service"))
//     + when(isnull(col("imp_2049_imp_juridico")), 0).otherwise(col("imp_2049_imp_juridico"))
//     + when(isnull(col("imp_2049_imp_ingenieria")), 0).otherwise(col("imp_2049_imp_ingenieria"))
//     + when(isnull(col("imp_2049_imp_ser_corporativos")), 0).otherwise(col("imp_2049_imp_ser_corporativos"))
//     + when(isnull(col("imp_2049_imp_digitalizacion")), 0).otherwise(col("imp_2049_imp_digitalizacion"))
//     + when(isnull(col("imp_2049_imp_sostenibilidad")), 0).otherwise(col("imp_2049_imp_sostenibilidad"))
//     + when(isnull(col("imp_2049_imp_seguros")), 0).otherwise(col("imp_2049_imp_seguros"))
//     + when(isnull(col("imp_2049_imp_auditoria")), 0).otherwise(col("imp_2049_imp_auditoria"))
//     + when(isnull(col("imp_2049_imp_planif_control")), 0).otherwise(col("imp_2049_imp_planif_control"))
//     + when(isnull(col("imp_2049_imp_compras")), 0).otherwise(col("imp_2049_imp_compras"))
//     + when(isnull(col("imp_2049_imp_financiero")), 0).otherwise(col("imp_2049_imp_financiero"))
//     + when(isnull(col("imp_2049_imp_otros")), 0).otherwise(col("imp_2049_imp_otros"))
//     + when(isnull(col("imp_2049_imp_fiscal")), 0).otherwise(col("imp_2049_imp_fiscal"))
//     + when(isnull(col("imp_2049_imp_econom_admin")), 0).otherwise(col("imp_2049_imp_econom_admin"))
//     + when(isnull(col("imp_2049_imp_serv_globales")), 0).otherwise(col("imp_2049_imp_serv_globales")))

// var imp_ees_bfc_pa_tot_cf_concep = imp_ees_bfc_pa_serv_corp.withColumn("imp_ees_bfc_pa_tot_cf_concep", when(isnull(col("imp_ees_pers")), 0).otherwise(col("imp_ees_pers"))
//     + when(isnull(col("imp_ees_serv_ext")), 0).otherwise(col("imp_ees_serv_ext"))
//     + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp"))
//     + when(isnull(col("imp_ees_serv_transv_DG")), 0).otherwise(col("imp_ees_serv_transv_DG"))
//     + when(isnull(col("imp_ees_otros_serv_DG")), 0).otherwise(col("imp_ees_otros_serv_DG")))

// var imp_ees_bfc_pa_tot_cf_analit = imp_ees_bfc_pa_tot_cf_concep.withColumn("imp_ees_bfc_pa_tot_cf_analit", when(isnull(col("imp_ees_cf_ees")), 0).otherwise(col("imp_ees_cf_ees"))
//     + when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos"))
//     + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp")))

// var imp_ees_bfc_pa_result_op_ppal = imp_ees_bfc_pa_tot_cf_analit.withColumn("imp_ees_bfc_pa_result_op_ppal", when(isnull(col("imp_ees_mc_ees")), 0).otherwise(col("imp_ees_mc_ees"))
//     + when(isnull(col("imp_ees_otros_mc")), 0).otherwise(col("imp_ees_otros_mc"))
//     - when(isnull(col("imp_ees_bfc_pa_tot_cf_analit")), 0).otherwise(col("imp_ees_bfc_pa_tot_cf_analit"))
//     - when(isnull(col("imp_ees_amort")), 0).otherwise(col("imp_ees_amort"))
//     - when(isnull(col("imp_ees_provis_recur")), 0).otherwise(col("imp_ees_provis_recur"))
//     + when(isnull(col("imp_ees_otros_result")), 0).otherwise(col("imp_ees_otros_result")))

// var imp_ees_bfc_pa_result_op = imp_ees_bfc_pa_result_op_ppal.withColumn("imp_ees_bfc_pa_result_op", when(isnull(col("imp_ees_bfc_pa_result_op_ppal")), 0).otherwise(col("imp_ees_bfc_pa_result_op_ppal"))
//     + when(isnull(col("imp_ees_result_otras_soc")), 0).otherwise(col("imp_ees_result_otras_soc")))


// //Mexico Minorista ----------------------------------------------------------------------------------------------------------------------------------------------
// //Real
// var imp_ees_mino_bfc_real_tot_cf_c = imp_ees_bfc_pa_result_op.withColumn("imp_ees_mino_bfc_real_tot_cf_c", when(isnull(col("imp_ees_minor_pers")), 0).otherwise(col("imp_ees_minor_pers"))
//     + when(isnull(col("imp_ees_minor_serv_ext")), 0).otherwise(col("imp_ees_minor_serv_ext"))
//     + when(isnull(col("imp_ees_minor_serv_corp")), 0).otherwise(col("imp_ees_minor_serv_corp"))
//     + when(isnull(col("imp_ees_minor_serv_transv_DG")), 0).otherwise(col("imp_ees_minor_serv_transv_DG"))
//     + when(isnull(col("imp_ees_minor_otros_serv_DG")), 0).otherwise(col("imp_ees_minor_otros_serv_DG")))

// var imp_ees_minor_bfc_real_resu_op =  imp_ees_mino_bfc_real_tot_cf_c.withColumn("imp_ees_minor_bfc_real_resu_op", when(isnull(col("imp_ees_minor_otros_mc")), 0).otherwise(col("imp_ees_minor_otros_mc"))
//     - when(isnull(col("imp_ees_minor_tot_cf_analit")), 0).otherwise(col("imp_ees_minor_tot_cf_analit"))
//     - when(isnull(col("imp_ees_minor_amort")), 0).otherwise(col("imp_ees_minor_amort"))
//     - when(isnull(col("imp_ees_minor_provis_recur")), 0).otherwise(col("imp_ees_minor_provis_recur"))
//     + when(isnull(col("imp_ees_minor_otros_result")), 0).otherwise(col("imp_ees_minor_otros_result")))

// //PA
// var imp_ees_minor_bfc_pa_tot_cf_c = imp_ees_minor_bfc_real_resu_op.withColumn("imp_ees_minor_bfc_pa_tot_cf_c", when(isnull(col("imp_ees_minor_pers")), 0).otherwise(col("imp_ees_minor_pers"))
//     + when(isnull(col("imp_ees_minor_serv_ext")), 0).otherwise(col("imp_ees_minor_serv_ext"))
//     + when(isnull(col("imp_ees_minor_serv_corp")), 0).otherwise(col("imp_ees_minor_serv_corp"))
//     + when(isnull(col("imp_ees_minor_serv_transv_DG")), 0).otherwise(col("imp_ees_minor_serv_transv_DG"))
//     + when(isnull(col("imp_ees_minor_otros_serv_DG")), 0).otherwise(col("imp_ees_minor_otros_serv_DG")))

// var imp_ees_minor_bfc_pa_result_op =  imp_ees_minor_bfc_pa_tot_cf_c.withColumn("imp_ees_minor_bfc_pa_result_op", when(isnull(col("imp_ees_minor_otros_mc")), 0).otherwise(col("imp_ees_minor_otros_mc"))
//     - when(isnull(col("imp_ees_minor_tot_cf_analit")), 0).otherwise(col("imp_ees_minor_tot_cf_analit"))
//     - when(isnull(col("imp_ees_minor_amort")), 0).otherwise(col("imp_ees_minor_amort"))
//     - when(isnull(col("imp_ees_minor_provis_recur")), 0).otherwise(col("imp_ees_minor_provis_recur"))
//     + when(isnull(col("imp_ees_minor_otros_result")), 0).otherwise(col("imp_ees_minor_otros_result")))



// //Mexico Central-----------------------------------------------------------------------------------------------------
// var imp_ctral_real_otros_serv_DG =  imp_ees_minor_bfc_pa_result_op.withColumn("imp_ctral_real_otros_serv_DG", when(isnull(col("imp_otros_serv_dg_crc")), 0).otherwise(col("imp_otros_serv_dg_crc"))
//     + when(isnull(col("imp_otros_serv_dg_e_commerce")), 0).otherwise(col("imp_otros_serv_dg_e_commerce"))
//     + when(isnull(col("imp_otros_serv_dg_fidel_global")), 0).otherwise(col("imp_otros_serv_dg_fidel_global"))
//     + when(isnull(col("imp_otros_serv_dg_int_cliente")), 0).otherwise(col("imp_otros_serv_dg_int_cliente"))
//     + when(isnull(col("imp_otros_serv_dg_mkt_cloud")), 0).otherwise(col("imp_otros_serv_dg_mkt_cloud")))

// var imp_ctral_real_serv_transv_DG =  imp_ctral_real_otros_serv_DG.withColumn("imp_ctral_real_serv_transv_DG", when(isnull(col("imp_serv_trans_otros_ser_trans")), 0).otherwise(col("imp_serv_trans_otros_ser_trans"))
//     + when(isnull(col("imp_serv_transv_pyc_cliente")), 0).otherwise(col("imp_serv_transv_pyc_cliente")) 
//     + when(isnull(col("imp_serv_transv_pyo_cliente")), 0).otherwise(col("imp_serv_transv_pyo_cliente"))
//     + when(isnull(col("imp_serv_transv_sost_cliente")), 0).otherwise(col("imp_serv_transv_sost_cliente")))

// var imp_bfc_impuestos  =  imp_ctral_real_serv_transv_DG.withColumn("imp_2049_imp_impuestos", when(isnull(col("imp_2049_imp_impuestos_prev")), 0).otherwise(col("imp_2049_imp_impuestos_prev") * -1))
//     .drop("imp_2049_imp_impuestos_prev")
//     .withColumn("imp_bfc_impuestos", when(isnull(col("imp_2049_imp_impuestos")), 0).otherwise(col("imp_2049_imp_impuestos")))
//    // + when(isnull(col("imp_2049_imp_impuestos")), 0).otherwise(col("imp_2049_imp_impuestos")))

// var imp_bfc_resultado_esp_adi =  imp_bfc_impuestos.withColumn("imp_bfc_resultado_esp_adi", when(isnull(col("imp_2049_imp_resul_espec_adi")), 0).otherwise(col("imp_2049_imp_resul_espec_adi")))
//     //+ when(isnull(col("2049_1657_imp_resultado_especifico_adi")), 0).otherwise(col("2049_1657_imp_resultado_especifico_adi")))

// var imp_bfc_minoritarios =  imp_bfc_resultado_esp_adi.withColumn("imp_bfc_minoritarios", when(isnull(col("imp_2049_imp_minoritarios")), 0).otherwise(col("imp_2049_imp_minoritarios")))
// //    + when(isnull(col("2049_1657_imp_minoritarios")), 0).otherwise(col("2049_1657_imp_minoritarios")))

// var imp_bfc_participadas =  imp_bfc_minoritarios.withColumn("imp_bfc_participadas", when(isnull(col("imp_2049_imp_participadas")), 0).otherwise(col("imp_2049_imp_participadas")))

// var imp_bfc_otros =  imp_bfc_participadas.withColumn("imp_bfc_otros", when(isnull(col("imp_2049_imp_otros")), 0).otherwise(col("imp_2049_imp_otros")))
//    // + when(isnull(col("2049_0919_imp_otros")), 0).otherwise(col("2049_0919_imp_otros")))

// var imp_bfc_pyo =  imp_bfc_otros.withColumn("imp_bfc_pyo", when(isnull(col("imp_2049_imp_pyo")), 0).otherwise(col("imp_2049_imp_pyo")))
//     //+ when(isnull(col("imp_2049_imp_pyo")), 0).otherwise(col("imp_2049_imp_pyo")))

// var imp_bfc_ti =  imp_bfc_pyo.withColumn("imp_bfc_ti", when(isnull(col("imp_2049_imp_ti")), 0).otherwise(col("imp_2049_imp_ti")))
//   //  + when(isnull(col("imp_2049_imp_ti")), 0).otherwise(col("imp_2049_imp_ti")))

// //var imp_bfc_ti_as_a_service =  imp_bfc_ti.withColumn("imp_bfc_ti_as_a_service", when(isnull(col("imp_2049_imp_ti_as_a_service")), 0).otherwise(col("imp_2049_imp_ti_as_a_service")))
//  //   + when(isnull(col("2049_0919_imp_ti_as_a_service")), 0).otherwise(col("2049_0919_imp_ti_as_a_service")))


// //  |-- 2049_0919_imp_juridico: double (nullable = true)
// //  |-- 2049_1657_imp_compras: double (nullable = true)
// //  |-- 2049_1657_imp_econom_admin: double (nullable = true)
// //  |-- 2049_1657_imp_financiero: double (nullable = true)
// //  |-- 2049_1657_imp_fiscal: double (nullable = true)
// //  |-- 2049_1657_imp_servicios_globales: double (nullable = true)
 


// //Real
// var imp_bfc_real_serv_corp = imp_bfc_ti.withColumn("imp_bfc_real_serv_corp", when(isnull(col("imp_2049_imp_pyo")), 0).otherwise(col("imp_2049_imp_pyo"))
//     + when(isnull(col("imp_2049_imp_patrimonial_seg")), 0).otherwise(col("imp_2049_imp_patrimonial_seg"))
//     + when(isnull(col("imp_2049_imp_comunicacion")), 0).otherwise(col("imp_2049_imp_comunicacion"))
//     + when(isnull(col("imp_2049_imp_techlab")), 0).otherwise(col("imp_2049_imp_techlab"))
//     + when(isnull(col("imp_2049_imp_ti")), 0).otherwise(col("imp_2049_imp_ti"))
//     //+ when(isnull(col("imp_2049_imp_ti_as_a_service")), 0).otherwise(col("imp_2049_imp_ti_as_a_service"))
//     + when(isnull(col("imp_2049_imp_juridico")), 0).otherwise(col("imp_2049_imp_juridico"))
//     + when(isnull(col("imp_2049_imp_ingenieria")), 0).otherwise(col("imp_2049_imp_ingenieria"))
//     + when(isnull(col("imp_2049_imp_ser_corporativos")), 0).otherwise(col("imp_2049_imp_ser_corporativos"))
//     + when(isnull(col("imp_2049_imp_digitalizacion")), 0).otherwise(col("imp_2049_imp_digitalizacion"))
//     + when(isnull(col("imp_2049_imp_sostenibilidad")), 0).otherwise(col("imp_2049_imp_sostenibilidad"))
//     + when(isnull(col("imp_2049_imp_seguros")), 0).otherwise(col("imp_2049_imp_seguros"))
//     + when(isnull(col("imp_2049_imp_auditoria")), 0).otherwise(col("imp_2049_imp_auditoria"))
//     + when(isnull(col("imp_2049_imp_planif_control")), 0).otherwise(col("imp_2049_imp_planif_control"))
//     + when(isnull(col("imp_2049_imp_compras")), 0).otherwise(col("imp_2049_imp_compras"))
//     + when(isnull(col("imp_2049_imp_financiero")), 0).otherwise(col("imp_2049_imp_financiero"))
//     + when(isnull(col("imp_2049_imp_otros")), 0).otherwise(col("imp_2049_imp_otros"))
//     + when(isnull(col("imp_2049_imp_fiscal")), 0).otherwise(col("imp_2049_imp_fiscal"))
//     + when(isnull(col("imp_2049_imp_econom_admin")), 0).otherwise(col("imp_2049_imp_econom_admin"))
//     + when(isnull(col("imp_2049_imp_serv_globales")), 0).otherwise(col("imp_2049_imp_serv_globales")))

// var imp_ctral_total_bfc_real_cf_c  = imp_bfc_real_serv_corp.withColumn("imp_ctral_total_bfc_real_cf_c", when(isnull(col("imp_ctral_pers")), 0).otherwise(col("imp_ctral_pers"))
//     + when(isnull(col("imp_ctral_ser_ext")), 0).otherwise(col("imp_ctral_ser_ext"))
//     + when(isnull(col("imp_bfc_real_serv_corp")), 0).otherwise(col("imp_bfc_real_serv_corp"))
//     + when(isnull(col("imp_ctral_real_serv_transv_DG")), 0).otherwise(col("imp_ctral_real_serv_transv_DG"))
//     + when(isnull(col("imp_ctral_real_otros_serv_DG")), 0).otherwise(col("imp_ctral_real_otros_serv_DG")))

// var imp_c_real_total_cf_analitica = imp_ctral_total_bfc_real_cf_c.withColumn("imp_c_real_total_cf_analitica", when(isnull(col("imp_ctral_cost_fij_estruct")), 0).otherwise(col("imp_ctral_cost_fij_estruct"))
//     //+ when(isnull(col("imp_ctral_otros_gastos")), 0).otherwise(col("imp_ctral_otros_gastos")))
//     + when(isnull(col("imp_bfc_real_serv_corp")), 0).otherwise(col("imp_bfc_real_serv_corp"))) 

// var imp_real_ctral_resu_ope  = imp_c_real_total_cf_analitica.withColumn("imp_real_ctral_resu_ope", when(isnull(col("imp_ctral_mc")), 0).otherwise(col("imp_ctral_mc"))
//     - when(isnull(col("imp_c_real_total_cf_analitica")), 0).otherwise(col("imp_c_real_total_cf_analitica"))
//     - when(isnull(col("imp_ctral_amort")), 0).otherwise(col("imp_ctral_amort"))
//     - when(isnull(col("imp_ctral_provis_recur")), 0).otherwise(col("imp_ctral_provis_recur"))
//     + when(isnull(col("imp_ctral_otros_result")), 0).otherwise(col("imp_ctral_otros_result")))

// //ser corp de bfc para analitica real
 
// //PA
// var imp_ctral_total_bfc_pa_cf_c  = imp_real_ctral_resu_ope.withColumn("imp_ctral_total_bfc_pa_cf_c", when(isnull(col("imp_ctral_pers")), 0).otherwise(col("imp_ctral_pers"))
//     + when(isnull(col("imp_ctral_ser_ext")), 0).otherwise(col("imp_ctral_ser_ext"))
//     + when(isnull(col("imp_ees_bfc_pa_serv_corp")), 0).otherwise(col("imp_ees_bfc_pa_serv_corp"))
//     + when(isnull(col("imp_ctral_serv_transv_DG")), 0).otherwise(col("imp_ctral_serv_transv_DG"))
//     + when(isnull(col("imp_ctral_otros_serv_DG")), 0).otherwise(col("imp_ctral_otros_serv_DG")))

// var imp_c_pa_total_cf_analitica = imp_ctral_total_bfc_pa_cf_c.withColumn("imp_c_pa_total_cf_analitica", when(isnull(col("imp_ctral_cost_fij_estruct")), 0).otherwise(col("imp_ctral_cost_fij_estruct"))
//     + when(isnull(col("imp_ees_bfc_pa_serv_corp")), 0).otherwise(col("imp_ees_bfc_pa_serv_corp")))
//     //+ when(isnull(col("imp_bfc_real_serv_corp")), 0).otherwise(col("imp_bfc_real_serv_corp")))

// var imp_pa_ctral_resu_ope  = imp_c_pa_total_cf_analitica.withColumn("imp_pa_ctral_resu_ope", when(isnull(col("imp_ctral_mc")), 0).otherwise(col("imp_ctral_mc"))
//     - when(isnull(col("imp_c_pa_total_cf_analitica")), 0).otherwise(col("imp_c_pa_total_cf_analitica"))
//     - when(isnull(col("imp_ctral_amort")), 0).otherwise(col("imp_ctral_amort"))
//     - when(isnull(col("imp_ctral_provis_recur")), 0).otherwise(col("imp_ctral_provis_recur"))
//     + when(isnull(col("imp_ctral_otros_result")), 0).otherwise(col("imp_ctral_otros_result")))


// //Mexico Ventas
// //Real
// var imp_real_vvdd_tot_cf_concep = imp_pa_ctral_resu_ope.withColumn("imp_real_vvdd_tot_cf_concep", when(isnull(col("imp_vvdd_pers")), 0).otherwise(col("imp_vvdd_pers"))
//     + when(isnull(col("imp_vvdd_serv_ext")), 0).otherwise(col("imp_vvdd_serv_ext"))
//     + when(isnull(col("imp_vvdd_serv_corp")), 0).otherwise(col("imp_vvdd_serv_corp"))
//     + when(isnull(col("imp_vvdd_serv_transv_DG")), 0).otherwise(col("imp_vvdd_serv_transv_DG"))
//     + when(isnull(col("imp_vvdd_otros_serv_DG")), 0).otherwise(col("imp_vvdd_otros_serv_DG")))

// var imp_real_vvdd_result_op = imp_real_vvdd_tot_cf_concep.withColumn("imp_real_vvdd_result_op", when(isnull(col("imp_vvdd_otros_mc")), 0).otherwise(col("imp_vvdd_otros_mc"))
//     - when(isnull(col("imp_vvdd_tot_cf_analit")), 0).otherwise(col("imp_vvdd_tot_cf_analit"))
//     - when(isnull(col("imp_vvdd_amort")), 0).otherwise(col("imp_vvdd_amort"))
//     - when(isnull(col("imp_vvdd_provis_recur")), 0).otherwise(col("imp_vvdd_provis_recur"))
//     + when(isnull(col("imp_vvdd_otros_result")), 0).otherwise(col("imp_vvdd_otros_result")))

// //PA
// var imp_pa_vvdd_tot_cf_concep = imp_real_vvdd_result_op.withColumn("imp_pa_vvdd_tot_cf_concep", when(isnull(col("imp_vvdd_pers")), 0).otherwise(col("imp_vvdd_pers"))
//     + when(isnull(col("imp_vvdd_serv_ext")), 0).otherwise(col("imp_vvdd_serv_ext"))
//     + when(isnull(col("imp_vvdd_serv_corp")), 0).otherwise(col("imp_vvdd_serv_corp"))
//     + when(isnull(col("imp_vvdd_serv_transv_DG")), 0).otherwise(col("imp_vvdd_serv_transv_DG"))
//     + when(isnull(col("imp_vvdd_otros_serv_DG")), 0).otherwise(col("imp_vvdd_otros_serv_DG")))

// var imp_pa_vvdd_result_op = imp_pa_vvdd_tot_cf_concep.withColumn("imp_pa_vvdd_result_op", when(isnull(col("imp_vvdd_otros_mc")), 0).otherwise(col("imp_vvdd_otros_mc"))
//     - when(isnull(col("imp_vvdd_tot_cf_analit")), 0).otherwise(col("imp_vvdd_tot_cf_analit"))
//     - when(isnull(col("imp_vvdd_amort")), 0).otherwise(col("imp_vvdd_amort"))
//     - when(isnull(col("imp_vvdd_provis_recur")), 0).otherwise(col("imp_vvdd_provis_recur"))
//     + when(isnull(col("imp_vvdd_otros_result")), 0).otherwise(col("imp_vvdd_otros_result")))


// //Mexico Total


// var imp_tot_bfc_real_serv_corp =  imp_pa_vvdd_result_op.withColumn("imp_tot_bfc_real_serv_corp", when(isnull(col("imp_bfc_real_serv_corp")), 0).otherwise(col("imp_bfc_real_serv_corp")))
    
// var imp_tot_real_tot_cf_concep =  imp_tot_bfc_real_serv_corp.withColumn("imp_tot_real_tot_cf_concep", when(isnull(col("imp_tot_mm_pers")), 0).otherwise(col("imp_tot_mm_pers"))
//     + when(isnull(col("imp_tot_mm_serv_ext")), 0).otherwise(col("imp_tot_mm_serv_ext"))
//     + when(isnull(col("imp_tot_bfc_real_serv_corp")), 0).otherwise(col("imp_tot_bfc_real_serv_corp"))
//     + when(isnull(col("imp_ctral_real_serv_transv_DG")), 0).otherwise(col("imp_ctral_real_serv_transv_DG"))
//     + when(isnull(col("imp_ctral_real_otros_serv_DG")), 0).otherwise(col("imp_ctral_real_otros_serv_DG")))
//     // + when(isnull(col("imp_tot_mm_serv_transv_DG")), 0).otherwise(col("imp_tot_mm_serv_transv_DG"))
//     // + when(isnull(col("imp_tot_mm_otros_serv_DG")), 0).otherwise(col("imp_tot_mm_otros_serv_DG")))

// var imp_tot_real_tot_cf_analit =  imp_tot_real_tot_cf_concep.withColumn("imp_real_mm_tot_cf_analit", when(isnull(col("imp_tot_mm_cf_ees")), 0).otherwise(col("imp_tot_mm_cf_ees"))
//     + when(isnull(col("imp_tot_mm_cf_estr")), 0).otherwise(col("imp_tot_mm_cf_estr"))
//     + when(isnull(col("imp_ees_bfc_real_serv_corp")), 0).otherwise(col("imp_ees_bfc_real_serv_corp")))
//    // + when(isnull(col("imp_tot_bfc_real_serv_corp")), 0).otherwise(col("imp_tot_bfc_real_serv_corp")))

// var imp_tot_real_serv_transv_DG =  imp_tot_real_tot_cf_analit.withColumn("imp_tot_real_serv_transv_DG", when(isnull(col("imp_ctral_real_serv_transv_DG")), 0).otherwise(col("imp_ctral_real_serv_transv_DG")))

// var imp_tot_real_otros_transv_DG =  imp_tot_real_serv_transv_DG.withColumn("imp_tot_real_otros_transv_DG", when(isnull(col("imp_ctral_real_otros_serv_DG")), 0).otherwise(col("imp_ctral_real_otros_serv_DG")))

// var imp_tot_real_result_op_ppal =  imp_tot_real_otros_transv_DG.withColumn("imp_tot_real_result_op_ppal", when(isnull(col("imp_tot_mm_mc_ees")), 0).otherwise(col("imp_tot_mm_mc_ees"))
//     + when(isnull(col("imp_tot_mm_otros_mc")), 0).otherwise(col("imp_tot_mm_otros_mc"))
//     - when(isnull(col("imp_real_mm_tot_cf_analit")), 0).otherwise(col("imp_real_mm_tot_cf_analit"))
//     - when(isnull(col("imp_tot_mm_amort")), 0).otherwise(col("imp_tot_mm_amort"))
//     - when(isnull(col("imp_tot_mm_provis_recur")), 0).otherwise(col("imp_tot_mm_provis_recur"))
//     + when(isnull(col("imp_tot_mm_otros_result")), 0).otherwise(col("imp_tot_mm_otros_result")))

// var imp_tot_real_result_op =  imp_tot_real_result_op_ppal.withColumn("imp_tot_real_result_op_prev", when(isnull(col("imp_tot_real_result_op_ppal")), 0).otherwise(col("imp_tot_real_result_op_ppal"))
//      + when(isnull(col("imp_tot_mm_result_otras_soc")), 0).otherwise(col("imp_tot_mm_result_otras_soc")))
//      .withColumn("imp_tot_real_result_op_AJUSTE", when(col("id_escenario") === 3, col("imp_tot_real_result_op_prev") - col("imp_ebit_css_recurrente")).otherwise(0))
//      .withColumn("imp_tot_real_result_op", when(isnull(col("imp_ebit_css_recurrente")), col("imp_tot_real_result_op_prev")).otherwise(col("imp_tot_real_result_op_prev") - col("imp_tot_real_result_op_AJUSTE")))
//      .drop("imp_tot_real_result_op_prev")

// var imp_tot_real_result_net_ajust =  imp_tot_real_result_op.withColumn("imp_tot_real_result_net_ajust", when(isnull(col("imp_tot_real_result_op")), 0).otherwise(col("imp_tot_real_result_op"))
//     + when(isnull(col("imp_bfc_minoritarios")), 0).otherwise(col("imp_bfc_minoritarios"))
//     + when(isnull(col("imp_bfc_participadas")), 0).otherwise(col("imp_bfc_participadas"))
//     - when(isnull(col("imp_bfc_impuestos")), 0).otherwise(col("imp_bfc_impuestos")))

// var imp_tot_real_result_net =  imp_tot_real_result_net_ajust.withColumn("imp_tot_real_result_net", when(isnull(col("imp_tot_real_result_net_ajust")), 0).otherwise(col("imp_tot_real_result_net_ajust"))
//     + when(isnull(col("imp_bfc_resultado_esp_adi")), 0).otherwise(col("imp_bfc_resultado_esp_adi")))
//     //+ when(isnull(col("imp_tot_mm_result_net_ajust_2_ef_patrim_ddi")), 0).otherwise(col("imp_tot_mm_result_net_ajust_2_ef_patrim_ddi")))


// //PA
// var imp_tot_bfc_pa_serv_corp =  imp_tot_real_result_net.withColumn("imp_tot_bfc_pa_serv_corp", when(isnull(col("imp_ees_bfc_pa_serv_corp")), 0).otherwise(col("imp_ees_bfc_pa_serv_corp")))
    
// var imp_tot_pa_tot_cf_concep =  imp_tot_bfc_pa_serv_corp.withColumn("imp_tot_pa_tot_cf_concep", when(isnull(col("imp_tot_mm_pers")), 0).otherwise(col("imp_tot_mm_pers"))
//     + when(isnull(col("imp_tot_mm_serv_ext")), 0).otherwise(col("imp_tot_mm_serv_ext"))
//     + when(isnull(col("imp_tot_bfc_pa_serv_corp")), 0).otherwise(col("imp_tot_bfc_pa_serv_corp"))
//     + when(isnull(col("imp_tot_mm_serv_transv_DG")), 0).otherwise(col("imp_tot_mm_serv_transv_DG"))
//     + when(isnull(col("imp_tot_mm_otros_serv_DG")), 0).otherwise(col("imp_tot_mm_otros_serv_DG")))

// var imp_tot_pa_tot_cf_analit =  imp_tot_pa_tot_cf_concep.withColumn("imp_pa_mm_tot_cf_analit", when(isnull(col("imp_tot_mm_cf_ees")), 0).otherwise(col("imp_tot_mm_cf_ees"))
//     + when(isnull(col("imp_tot_mm_cf_estr")), 0).otherwise(col("imp_tot_mm_cf_estr"))
//     + when(isnull(col("imp_tot_mm_otros_gastos")), 0).otherwise(col("imp_tot_mm_otros_gastos")))
//   //  + when(isnull(col("imp_tot_bfc_pa_serv_corp")), 0).otherwise(col("imp_tot_bfc_pa_serv_corp")))

// var imp_tot_pa_result_op_ppal =  imp_tot_pa_tot_cf_analit.withColumn("imp_tot_pa_result_op_ppal", when(isnull(col("imp_tot_mm_mc_ees")), 0).otherwise(col("imp_tot_mm_mc_ees"))
//     + when(isnull(col("imp_tot_mm_otros_mc")), 0).otherwise(col("imp_tot_mm_otros_mc"))
//     - when(isnull(col("imp_pa_mm_tot_cf_analit")), 0).otherwise(col("imp_pa_mm_tot_cf_analit"))
//     - when(isnull(col("imp_tot_mm_amort")), 0).otherwise(col("imp_tot_mm_amort"))
//     - when(isnull(col("imp_tot_mm_provis_recur")), 0).otherwise(col("imp_tot_mm_provis_recur"))
//     + when(isnull(col("imp_tot_mm_otros_result")), 0).otherwise(col("imp_tot_mm_otros_result")))

// var imp_tot_pa_result_op =  imp_tot_pa_result_op_ppal.withColumn("imp_tot_pa_result_op_prev", when(isnull(col("imp_tot_pa_result_op_ppal")), 0).otherwise(col("imp_tot_pa_result_op_ppal"))
//      + when(isnull(col("imp_tot_mm_result_otras_soc")), 0).otherwise(col("imp_tot_mm_result_otras_soc")))
//     .withColumn("imp_tot_pa_result_op_AJUSTE", when(col("id_escenario") === 2, col("imp_tot_pa_result_op_prev") - col("imp_ebit_css_recurrente")).otherwise(0))
//      .withColumn("imp_tot_pa_result_op", when(isnull(col("imp_ebit_css_recurrente")), col("imp_tot_pa_result_op_prev")).otherwise(col("imp_tot_pa_result_op_prev") - col("imp_tot_pa_result_op_AJUSTE")))
//      .drop("imp_tot_pa_result_op_prev")

// var imp_tot_pa_result_net_ajust =  imp_tot_pa_result_op.withColumn("imp_tot_pa_result_net_ajust", when(isnull(col("imp_tot_pa_result_op")), 0).otherwise(col("imp_tot_pa_result_op"))
//     + when(isnull(col("imp_2049_imp_minoritarios")), 0).otherwise(col("imp_2049_imp_minoritarios"))
//     + when(isnull(col("imp_2049_imp_participadas")), 0).otherwise(col("imp_2049_imp_participadas"))
//     - when(isnull(col("imp_2049_imp_impuestos")), 0).otherwise(col("imp_2049_imp_impuestos")))

// var imp_tot_pa_result_net =  imp_tot_pa_result_net_ajust.withColumn("imp_tot_pa_result_net", when(isnull(col("imp_tot_pa_result_net_ajust")), 0).otherwise(col("imp_tot_pa_result_net_ajust"))
//     + when(isnull(col("imp_bfc_resultado_esp_adi")), 0).otherwise(col("imp_bfc_resultado_esp_adi"))
//     + when(isnull(col("imp_2049_imp_efecto_patri_adi")), 0).otherwise(col("imp_2049_imp_efecto_patri_adi")))

// var TOTAL_imp_minoritarios_participadas =  imp_tot_pa_result_net.withColumn("imp_2049_imp_minoritarios_part", when(isnull(col("imp_2049_imp_participadas")), 0).otherwise(col("imp_2049_imp_participadas"))
//     + when(isnull(col("imp_2049_imp_minoritarios")), 0).otherwise(col("imp_2049_imp_minoritarios")))

// //MEXICO EES MAYORISTA
// //REAL

// var imp_ees_mayor_real_tot_cf_c =  TOTAL_imp_minoritarios_participadas.withColumn("imp_ees_mayor_real_tot_cf_c", when(isnull(col("imp_ees_mayor_pers")), 0).otherwise(col("imp_ees_mayor_pers"))
//     + when(isnull(col("imp_ees_mayor_serv_ext")), 0).otherwise(col("imp_ees_mayor_serv_ext"))
//     + when(isnull(col("imp_ees_mayor_serv_corp")), 0).otherwise(col("imp_ees_mayor_serv_corp"))
//     + when(isnull(col("imp_ees_mayor_serv_transv_DG")), 0).otherwise(col("imp_ees_mayor_serv_transv_DG"))
//     + when(isnull(col("imp_ees_mayor_otros_serv_DG")), 0).otherwise(col("imp_ees_mayor_otros_serv_DG")))

// var imp_ees_mayor_real_tot_cf_ana =  imp_ees_mayor_real_tot_cf_c.withColumn("imp_ees_mayor_real_tot_cf_ana", when(isnull(col("imp_ees_mayor_cf_ees")), 0).otherwise(col("imp_ees_mayor_cf_ees"))
//     + when(isnull(col("imp_ees_mayor_otros_gastos")), 0).otherwise(col("imp_ees_mayor_otros_gastos")))
//    // + when(isnull(col("imp_ees_mayor_serv_corp")), 0).otherwise(col("imp_ees_mayor_serv_corp")))

// var imp_ees_mayor_real_resu_op_ppl =  imp_ees_mayor_real_tot_cf_ana.withColumn("imp_ees_mayor_real_resu_op_ppl", when(isnull(col("imp_ees_mayor_mc_ees")), 0).otherwise(col("imp_ees_mayor_mc_ees"))
//     - when(isnull(col("imp_ees_mayor_real_tot_cf_ana")), 0).otherwise(col("imp_ees_mayor_real_tot_cf_ana"))
//     - when(isnull(col("imp_ees_mayor_amort")), 0).otherwise(col("imp_ees_mayor_amort"))
//     - when(isnull(col("imp_ees_mayor_provis_recur")), 0).otherwise(col("imp_ees_mayor_provis_recur"))
//     + when(isnull(col("imp_ees_mayor_otros_result")), 0).otherwise(col("imp_ees_mayor_otros_result")))

// var imp_ees_mayor_real_result_op =  imp_ees_mayor_real_resu_op_ppl.withColumn("imp_ees_mayor_real_result_op", when(isnull(col("imp_ees_mayor_real_resu_op_ppl")), 0).otherwise(col("imp_ees_mayor_real_resu_op_ppl"))
//     + when(isnull(col("imp_ees_mayor_result_otras_soc")), 0).otherwise(col("imp_ees_mayor_result_otras_soc")))

// //PA


// var imp_ees_mayor_pa_tot_cf_c =  imp_ees_mayor_real_result_op.withColumn("imp_ees_mayor_pa_tot_cf_c", when(isnull(col("imp_ees_mayor_pers")), 0).otherwise(col("imp_ees_mayor_pers"))
//     + when(isnull(col("imp_ees_mayor_serv_ext")), 0).otherwise(col("imp_ees_mayor_serv_ext"))
//     + when(isnull(col("imp_ees_mayor_serv_corp")), 0).otherwise(col("imp_ees_mayor_serv_corp"))
//     + when(isnull(col("imp_ees_mayor_serv_transv_DG")), 0).otherwise(col("imp_ees_mayor_serv_transv_DG"))
//     + when(isnull(col("imp_ees_mayor_otros_serv_DG")), 0).otherwise(col("imp_ees_mayor_otros_serv_DG")))

// var imp_ees_mayor_pa_tot_cf_ana =  imp_ees_mayor_pa_tot_cf_c.withColumn("imp_ees_mayor_pa_tot_cf_ana", when(isnull(col("imp_ees_mayor_cf_ees")), 0).otherwise(col("imp_ees_mayor_cf_ees"))
//     + when(isnull(col("imp_ees_mayor_otros_gastos")), 0).otherwise(col("imp_ees_mayor_otros_gastos")))
//    // + when(isnull(col("imp_ees_mayor_serv_corp")), 0).otherwise(col("imp_ees_mayor_serv_corp")))

// var imp_ees_mayor_pa_result_op_ppl =  imp_ees_mayor_pa_tot_cf_ana.withColumn("imp_ees_mayor_pa_result_op_ppl", when(isnull(col("imp_ees_mayor_mc_ees")), 0).otherwise(col("imp_ees_mayor_mc_ees"))
//     - when(isnull(col("imp_ees_mayor_pa_tot_cf_ana")), 0).otherwise(col("imp_ees_mayor_pa_tot_cf_ana"))
//     - when(isnull(col("imp_ees_mayor_amort")), 0).otherwise(col("imp_ees_mayor_amort"))
//     - when(isnull(col("imp_ees_mayor_provis_recur")), 0).otherwise(col("imp_ees_mayor_provis_recur"))
//     + when(isnull(col("imp_ees_mayor_otros_result")), 0).otherwise(col("imp_ees_mayor_otros_result")))

// var imp_ees_mayor_pa_result_op =  imp_ees_mayor_pa_result_op_ppl.withColumn("imp_ees_mayor_pa_result_op", when(isnull(col("imp_ees_mayor_pa_result_op_ppl")), 0).otherwise(col("imp_ees_mayor_pa_result_op_ppl"))
//     + when(isnull(col("imp_ees_mayor_result_otras_soc")), 0).otherwise(col("imp_ees_mayor_result_otras_soc")))


// var DFfinal = imp_ees_mayor_pa_result_op.cache


# In[28]:


var DFfinal = n.coalesce(1).withColumn("imp_ctral_pers", when(isnull(col("imp_ctral_pers_2_otros_cost")), 0).otherwise(col("imp_ctral_pers_2_otros_cost"))
    + when(isnull(col("imp_ctral_pers_2_retrib")), 0).otherwise(col("imp_ctral_pers_2_retrib")))
    .withColumn("imp_ctral_ser_ext", when(isnull(col("imp_ctral_serv_ext_2_mant_rep")), 0).otherwise(col("imp_ctral_serv_ext_2_mant_rep"))
    + when(isnull(col("imp_ctral_sv_ext_2_otr_sv_ext")), 0).otherwise(col("imp_ctral_sv_ext_2_otr_sv_ext"))
    + when(isnull(col("imp_ctral_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_ctral_serv_ext_2_pub_rrpp"))
    + when(isnull(col("imp_ctral_serv_ext_2_seg")), 0).otherwise(col("imp_ctral_serv_ext_2_seg"))
    + when(isnull(col("imp_ctral_serv_ext_2_serv_prof")), 0).otherwise(col("imp_ctral_serv_ext_2_serv_prof"))
    + when(isnull(col("imp_ctral_serv_ext_2_sumin")), 0).otherwise(col("imp_ctral_serv_ext_2_sumin"))
    + when(isnull(col("imp_ctral_serv_ext_2_trib")), 0).otherwise(col("imp_ctral_serv_ext_2_trib"))
    + when(isnull(col("imp_ctral_serv_ext_2_arrycan")), 0).otherwise(col("imp_ctral_serv_ext_2_arrycan"))
    + when(isnull(col("imp_ctral_serv_ext_2_viajes")), 0).otherwise(col("imp_ctral_serv_ext_2_viajes")))
    .withColumn("imp_ctral_total_cf_concept", when(isnull(col("imp_ctral_pers")), 0).otherwise(col("imp_ctral_pers"))
    + when(isnull(col("imp_ctral_ser_ext")), 0).otherwise(col("imp_ctral_ser_ext"))
    + when(isnull(col("imp_ctral_serv_corp")), 0).otherwise(col("imp_ctral_serv_corp"))
    + when(isnull(col("imp_ctral_serv_transv_DG")), 0).otherwise(col("imp_ctral_serv_transv_DG"))
    + when(isnull(col("imp_ctral_otros_serv_DG")), 0).otherwise(col("imp_ctral_otros_serv_DG")))
    .withColumn("imp_ctral_cost_fij_estruct", when(isnull(col("imp_ctral_cf_est_2_com_rrpp")), 0).otherwise(col("imp_ctral_cf_est_2_com_rrpp"))
    + when(isnull(col("imp_ctral_cf_est_2_otros_serv")), 0).otherwise(col("imp_ctral_cf_est_2_otros_serv"))
    + when(isnull(col("imp_ctral_cf_est_2_pers")), 0).otherwise(col("imp_ctral_cf_est_2_pers"))
    + when(isnull(col("imp_ctral_cf_est_2_primas_seg")), 0).otherwise(col("imp_ctral_cf_est_2_primas_seg"))
    + when(isnull(col("imp_ctral_cf_est_2_pub_rrpp")), 0).otherwise(col("imp_ctral_cf_est_2_pub_rrpp"))
    + when(isnull(col("imp_ctral_cf_est_2_serv_banc")), 0).otherwise(col("imp_ctral_cf_est_2_serv_banc"))
    + when(isnull(col("imp_ctral_cf_est_2_sumin")), 0).otherwise(col("imp_ctral_cf_est_2_sumin"))
    + when(isnull(col("imp_ctral_cf_est_2_serv_prof")), 0).otherwise(col("imp_ctral_cf_est_2_serv_prof"))
    + when(isnull(col("imp_ctral_cf_est_2_viaj")), 0).otherwise(col("imp_ctral_cf_est_2_viaj"))
    )
    .withColumn("imp_ctral_total_cf_analitica", when(isnull(col("imp_ctral_cost_fij_estruct")), 0).otherwise(col("imp_ctral_cost_fij_estruct"))
    + when(isnull(col("imp_ctral_otros_gastos")), 0).otherwise(col("imp_ctral_otros_gastos")))
    .withColumn("imp_ctral_cost_fijos", when(isnull(col("imp_ctral_total_cf_analitica")), 0).otherwise(col("imp_ctral_total_cf_analitica")))
    .withColumn("imp_ctral_resu_ope", when(isnull(col("imp_ctral_mc")), 0).otherwise(col("imp_ctral_mc"))
    - when(isnull(col("imp_ctral_cost_fijos")), 0).otherwise(col("imp_ctral_cost_fijos"))
    - when(isnull(col("imp_ctral_amort")), 0).otherwise(col("imp_ctral_amort"))
    - when(isnull(col("imp_ctral_provis_recur")), 0).otherwise(col("imp_ctral_provis_recur"))
    + when(isnull(col("imp_ctral_otros_result")), 0).otherwise(col("imp_ctral_otros_result")))
.withColumn("imp_ees_result_otras_soc", when(isnull(col("imp_ees_EBIT_CCS_JV_mar_cortes")), 0).otherwise(col("imp_ees_EBIT_CCS_JV_mar_cortes")))
.withColumn("imp_ees_ventas_monterra", when(isnull(col("imp_ees_ventas_mont_reg_diesel")), 0).otherwise(col("imp_ees_ventas_mont_reg_diesel"))
    + when(isnull(col("imp_ees_ventas_mont_premium")), 0).otherwise(col("imp_ees_ventas_mont_premium"))
    + when(isnull(col("imp_ees_ventas_mont_regular")), 0).otherwise(col("imp_ees_ventas_mont_regular")))
.withColumn("imp_ees_ventas_olstor", when(isnull(col("imp_ees_ventas_olstor_rg_diesl")), 0).otherwise(col("imp_ees_ventas_olstor_rg_diesl"))
    + when(isnull(col("imp_ees_ventas_olstor_premium")), 0).otherwise(col("imp_ees_ventas_olstor_premium"))
    + when(isnull(col("imp_ees_ventas_olstor_regular")), 0).otherwise(col("imp_ees_ventas_olstor_regular")))
.withColumn("imp_ees_ventas", when(isnull(col("imp_ees_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_ees_ventas_go_a_reg_diesel"))
    + when(isnull(col("imp_ees_ventas_prem_92")), 0).otherwise(col("imp_ees_ventas_prem_92"))
    + when(isnull(col("imp_ees_ventas_reg_87")), 0).otherwise(col("imp_ees_ventas_reg_87"))
    + when(isnull(col("imp_ees_ventas_monterra")), 0).otherwise(col("imp_ees_ventas_monterra"))
    + when(isnull(col("imp_ees_ventas_olstor")), 0).otherwise(col("imp_ees_ventas_olstor")))
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
.withColumn("imp_ees_mc_unit_ees_2_extr_mon", when(isnull(col("imp_ees_mc_unit_ees_2_extr_mon")), 0).otherwise(col("imp_ees_mc_unit_ees_2_extr_mon")))
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
    + when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos")))
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
    - when(isnull(col("imp_ees_cost_log_dist_2_transp")), 0).otherwise(col("imp_ees_cost_log_dist_2_transp")))
.withColumn("imp_ees_cv_tot", when(isnull(col("imp_ees_coste_prod")), 0).otherwise(col("imp_ees_coste_prod"))
    + when(isnull(col("imp_ees_coste_logist_dist")), 0).otherwise(col("imp_ees_coste_logist_dist"))
    + when(isnull(col("imp_ees_coste_canal")), 0).otherwise(col("imp_ees_coste_canal")))
.withColumn("imp_ees_cv_unit", when(isnull(col("imp_ees_cv_tot")), 0).otherwise(col("imp_ees_cv_tot"))
    / when(isnull(col("imp_ees_ventas")), 0).otherwise(col("imp_ees_ventas")))
.withColumn("imp_ees_mayor_ventas", when(isnull(col("imp_ees_mayor_vta_go_reg_diesl")), 0).otherwise(col("imp_ees_mayor_vta_go_reg_diesl"))
    + when(isnull(col("imp_ees_mayor_ventas_prem_92")), 0).otherwise(col("imp_ees_mayor_ventas_prem_92"))
    + when(isnull(col("imp_ees_mayor_ventas_reg_87")), 0).otherwise(col("imp_ees_mayor_ventas_reg_87")))
.withColumn("imp_ees_mayor_mc_ees", when(isnull(col("imp_ees_mayor_mc_ees_2_sin_res")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_sin_res"))
    + when(isnull(col("imp_ees_mayor_mc_ees_2_res_est")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_res_est"))
    + when(isnull(col("imp_ees_mayor_mc_ees_2_ext_ols")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_ext_ols"))
    + when(isnull(col("imp_ees_mayor_mc_ees_2_ext_mon")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_ext_mon")))
.withColumn("imp_ees_mayor_mc_unit_ees", when(isnull(col("imp_ees_mayor_mc_ees")), 0).otherwise(col("imp_ees_mayor_mc_ees"))
    / when(isnull(col("imp_ees_mayor_ventas")), 0).otherwise(col("imp_ees_mayor_ventas")))
.withColumn("imp_mayor_mc_uni_ees_2_sres_et", when(isnull(col("imp_ees_mayor_mc_ees_2_sin_res")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_sin_res"))
    / when(isnull(col("imp_ees_mayor_ventas")), 0).otherwise(col("imp_ees_mayor_ventas")))
.withColumn("imp_mayo_mc_uni_ees_2_efres_et", when(isnull(col("imp_ees_mayor_mc_ees_2_res_est")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_res_est"))
    / when(isnull(col("imp_ees_mayor_ventas")), 0).otherwise(col("imp_ees_mayor_ventas")))
.withColumn("imp_mayor_mc_uni_ees_2_ext_ols", when(isnull(col("imp_ees_mayor_mc_ees_2_ext_ols")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_ext_ols"))
    / when(isnull(col("imp_ees_mayor_ventas")), 0).otherwise(col("imp_ees_mayor_ventas")))
.withColumn("imp_mayor_mc_uni_ees_2_ext_mon", when(isnull(col("imp_ees_mayor_mc_ees_2_ext_mon")), 0).otherwise(col("imp_ees_mayor_mc_ees_2_ext_mon"))
    / when(isnull(col("imp_ees_mayor_ventas")), 0).otherwise(col("imp_ees_mayor_ventas")))
.withColumn("imp_mayor_mc_uni_ees_2_ext_mon", when(isnull(col("imp_mayor_mc_uni_ees_2_ext_mon")), 0).otherwise(col("imp_mayor_mc_uni_ees_2_ext_mon")))
.withColumn("imp_ees_mayor_pers", when(isnull(col("imp_ees_mayor_pers_2_retrib")), 0).otherwise(col("imp_ees_mayor_pers_2_retrib"))
    + when(isnull(col("imp_ees_mayor_pers_2_otro_cost")), 0).otherwise(col("imp_ees_mayor_pers_2_otro_cost")))
.withColumn("imp_ees_mayor_serv_ext", when(isnull(col("imp_ees_mayor_sv_ext_2_arrycan")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_arrycan"))
    + when(isnull(col("imp_ees_mayor_sv_ext_2_pb_rrpp")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_pb_rrpp"))
    + when(isnull(col("imp_ees_mayor_serv_ext_2_sumin")), 0).otherwise(col("imp_ees_mayor_serv_ext_2_sumin"))
    + when(isnull(col("imp_ees_mayor_sv_ext_2_man_rep")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_man_rep"))
    + when(isnull(col("imp_ees_mayor_sv_ext_2_sv_prof")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_sv_prof"))
    + when(isnull(col("imp_ees_mayor_sv_ext_2_viajes")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_viajes"))
    + when(isnull(col("imp_ees_mayor_serv_ext_2_seg")), 0).otherwise(col("imp_ees_mayor_serv_ext_2_seg"))
    + when(isnull(col("imp_ees_mayor_serv_ext_2_trib")), 0).otherwise(col("imp_ees_mayor_serv_ext_2_trib"))
    + when(isnull(col("imp_ees_mayor_sv_ext_2_otro_sv")), 0).otherwise(col("imp_ees_mayor_sv_ext_2_otro_sv")))
.withColumn("imp_ees_mayor_tot_cf_concep", when(isnull(col("imp_ees_mayor_pers")), 0).otherwise(col("imp_ees_mayor_pers"))
    + when(isnull(col("imp_ees_mayor_serv_ext")), 0).otherwise(col("imp_ees_mayor_serv_ext"))
    + when(isnull(col("imp_ees_mayor_serv_corp")), 0).otherwise(col("imp_ees_mayor_serv_corp"))
    + when(isnull(col("imp_ees_mayor_serv_transv_DG")), 0).otherwise(col("imp_ees_mayor_serv_transv_DG"))
    + when(isnull(col("imp_ees_mayor_otros_serv_DG")), 0).otherwise(col("imp_ees_mayor_otros_serv_DG")))
.withColumn("imp_ees_mayor_cf_ees", when(isnull(col("imp_ees_mayor_cf_ees_2_mante")), 0).otherwise(col("imp_ees_mayor_cf_ees_2_mante"))
    + when(isnull(col("imp_ees_mayor_cf_ees_2_neotech")), 0).otherwise(col("imp_ees_mayor_cf_ees_2_neotech"))
    + when(isnull(col("imp_ees_mayor_cf_ees_2_sc_volu")), 0).otherwise(col("imp_ees_mayor_cf_ees_2_sc_volu"))
    + when(isnull(col("imp_ees_mayor_cf_ees_2_act_prm")), 0).otherwise(col("imp_ees_mayor_cf_ees_2_act_prm"))
    + when(isnull(col("imp_ees_mayor_cf_ees_2_otros")), 0).otherwise(col("imp_ees_mayor_cf_ees_2_otros")))
.withColumn("imp_ees_mayor_tot_cf_analit", when(isnull(col("imp_ees_mayor_cf_ees")), 0).otherwise(col("imp_ees_mayor_cf_ees"))
    + when(isnull(col("imp_ees_mayor_otros_gastos")), 0).otherwise(col("imp_ees_mayor_otros_gastos")))
.withColumn("imp_ees_mayor_cf", when(isnull(col("imp_ees_mayor_tot_cf_analit")), 0).otherwise(col("imp_ees_mayor_tot_cf_analit")))
.withColumn("imp_ees_mayor_result_op_ppal", when(isnull(col("imp_ees_mayor_mc_ees")), 0).otherwise(col("imp_ees_mayor_mc_ees"))
    - when(isnull(col("imp_ees_mayor_cf")), 0).otherwise(col("imp_ees_mayor_cf"))
    - when(isnull(col("imp_ees_mayor_amort")), 0).otherwise(col("imp_ees_mayor_amort"))
    - when(isnull(col("imp_ees_mayor_provis_recur")), 0).otherwise(col("imp_ees_mayor_provis_recur"))
    + when(isnull(col("imp_ees_mayor_otros_result")), 0).otherwise(col("imp_ees_mayor_otros_result")))
.withColumn("imp_ees_mayor_result_otras_soc", when(isnull(col("imp_ees_mayor_EBIT_CCS_JV_cort")), 0).otherwise(col("imp_ees_mayor_EBIT_CCS_JV_cort")))
.withColumn("imp_ees_mayor_result_op", when(isnull(col("imp_ees_mayor_result_op_ppal")), 0).otherwise(col("imp_ees_mayor_result_op_ppal"))
    + when(isnull(col("imp_ees_mayor_result_otras_soc")), 0).otherwise(col("imp_ees_mayor_result_otras_soc")))
.withColumn("imp_ees_minor_ventas", when(isnull(col("imp_ees_minor_vta_go_reg_diesl")), 0).otherwise(col("imp_ees_minor_vta_go_reg_diesl"))
    + when(isnull(col("imp_ees_minor_ventas_prem_92")), 0).otherwise(col("imp_ees_minor_ventas_prem_92"))
    + when(isnull(col("imp_ees_minor_ventas_reg_87")), 0).otherwise(col("imp_ees_minor_ventas_reg_87")))
.withColumn("imp_ees_minor_pers", when(isnull(col("imp_ees_minor_pers_2_retrib")), 0).otherwise(col("imp_ees_minor_pers_2_retrib"))
    + when(isnull(col("imp_ees_minor_pers_2_otro_cost")), 0).otherwise(col("imp_ees_minor_pers_2_otro_cost")))
.withColumn("imp_ees_minor_serv_ext", when(isnull(col("imp_ees_minor_sv_ext_2_arrycan")), 0).otherwise(col("imp_ees_minor_sv_ext_2_arrycan"))
    + when(isnull(col("imp_ees_minor_sv_ext_2_pb_rrpp")), 0).otherwise(col("imp_ees_minor_sv_ext_2_pb_rrpp"))
    + when(isnull(col("imp_ees_minor_serv_ext_2_sumin")), 0).otherwise(col("imp_ees_minor_serv_ext_2_sumin"))
    + when(isnull(col("imp_ees_minor_sv_ext_2_man_rep")), 0).otherwise(col("imp_ees_minor_sv_ext_2_man_rep"))
    + when(isnull(col("imp_ees_minor_sv_ext_2_sv_prof")), 0).otherwise(col("imp_ees_minor_sv_ext_2_sv_prof"))
    + when(isnull(col("imp_ees_minor_sv_ext_2_viajes")), 0).otherwise(col("imp_ees_minor_sv_ext_2_viajes"))
    + when(isnull(col("imp_ees_minor_serv_ext_2_seg")), 0).otherwise(col("imp_ees_minor_serv_ext_2_seg"))
    + when(isnull(col("imp_ees_minor_serv_ext_2_trib")), 0).otherwise(col("imp_ees_minor_serv_ext_2_trib"))
    + when(isnull(col("imp_ees_minor_sv_ext_2_otro_sv")), 0).otherwise(col("imp_ees_minor_sv_ext_2_otro_sv")))
.withColumn("imp_ees_minor_tot_cf_concep", when(isnull(col("imp_ees_minor_pers")), 0).otherwise(col("imp_ees_minor_pers"))
    + when(isnull(col("imp_ees_minor_serv_ext")), 0).otherwise(col("imp_ees_minor_serv_ext"))
    + when(isnull(col("imp_ees_minor_serv_corp")), 0).otherwise(col("imp_ees_minor_serv_corp"))
    + when(isnull(col("imp_ees_minor_serv_transv_DG")), 0).otherwise(col("imp_ees_minor_serv_transv_DG"))
    + when(isnull(col("imp_ees_minor_otros_serv_DG")), 0).otherwise(col("imp_ees_minor_otros_serv_DG")))
.withColumn("imp_ees_minor_cf_ees", when(isnull(col("imp_ees_minor_cf_ees_2_man_rep")), 0).otherwise(col("imp_ees_minor_cf_ees_2_man_rep"))
    + when(isnull(col("imp_ees_minor_cf_ees_2_neotech")), 0).otherwise(col("imp_ees_minor_cf_ees_2_neotech"))
    + when(isnull(col("imp_ees_minor_cf_ees_2_sc_volu")), 0).otherwise(col("imp_ees_minor_cf_ees_2_sc_volu"))
    + when(isnull(col("imp_ees_minor_cf_ees_2_act_prm")), 0).otherwise(col("imp_ees_minor_cf_ees_2_act_prm"))
    + when(isnull(col("imp_ees_minor_cf_ees_2_otro_cf")), 0).otherwise(col("imp_ees_minor_cf_ees_2_otro_cf")))
.withColumn("imp_ees_minor_tot_cf_analit", when(isnull(col("imp_ees_minor_cf_ees")), 0).otherwise(col("imp_ees_minor_cf_ees"))
    + when(isnull(col("imp_ees_minor_otros_gastos")), 0).otherwise(col("imp_ees_minor_otros_gastos")))
.withColumn("imp_ees_minor_cf", when(isnull(col("imp_ees_minor_tot_cf_analit")), 0).otherwise(col("imp_ees_minor_tot_cf_analit")))
.withColumn("imp_ees_minor_result_op", when(isnull(col("imp_ees_minor_otros_mc")), 0).otherwise(col("imp_ees_minor_otros_mc"))
    - when(isnull(col("imp_ees_minor_cf")), 0).otherwise(col("imp_ees_minor_cf"))
    - when(isnull(col("imp_ees_minor_amort")), 0).otherwise(col("imp_ees_minor_amort"))
    - when(isnull(col("imp_ees_minor_provis_recur")), 0).otherwise(col("imp_ees_minor_provis_recur"))
    + when(isnull(col("imp_ees_minor_otros_result")), 0).otherwise(col("imp_ees_minor_otros_result")))
.withColumn("imp_vvdd_ventas", when(isnull(col("imp_vvdd_ventas_go_a_reg_diesl")), 0).otherwise(col("imp_vvdd_ventas_go_a_reg_diesl"))
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
.withColumn("imp_vvdd_mc", when(isnull(col("imp_vvdd_mb")), 0).otherwise(col("imp_vvdd_mb")))
.withColumn("imp_vvdd_cv_tot", when(isnull(col("imp_vvdd_coste_prod")), 0).otherwise(col("imp_vvdd_coste_prod")))
.withColumn("imp_vvdd_cv_unit", when(isnull(col("imp_vvdd_cv_tot")), 0).otherwise(col("imp_vvdd_cv_tot"))
    / when(isnull(col("imp_vvdd_ventas")), 0).otherwise(col("imp_vvdd_ventas")))
.withColumn("imp_vvdd_tot_cf_analit", when(isnull(col("imp_vvdd_otros_gastos")), 0).otherwise(col("imp_vvdd_otros_gastos")))
.withColumn("imp_vvdd_cv_tot", when(isnull(col("imp_vvdd_coste_prod")), 0).otherwise(col("imp_vvdd_coste_prod"))).cache
.withColumn("imp_tot_ventas_go_a_reg_diesel", when(isnull(col("imp_vvdd_ventas_go_a_reg_diesl")), 0).otherwise(col("imp_vvdd_ventas_go_a_reg_diesl"))
    + when(isnull(col("imp_ees_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_ees_ventas_go_a_reg_diesel")))
.withColumn("imp_tot_ventas_prem_92", when(isnull(col("imp_vvdd_ventas_prem_92")), 0).otherwise(col("imp_vvdd_ventas_prem_92"))
    + when(isnull(col("imp_ees_ventas_prem_92")), 0).otherwise(col("imp_ees_ventas_prem_92")))
.withColumn("imp_tot_ventas_reg_87", when(isnull(col("imp_vvdd_ventas_reg_87")), 0).otherwise(col("imp_vvdd_ventas_reg_87"))
    + when(isnull(col("imp_ees_ventas_reg_87")), 0).otherwise(col("imp_ees_ventas_reg_87")))
.withColumn("imp_total_ventas", when(isnull(col("imp_tot_ventas_go_a_reg_diesel")), 0).otherwise(col("imp_tot_ventas_go_a_reg_diesel"))
    + when(isnull(col("imp_tot_ventas_prem_92")), 0).otherwise(col("imp_tot_ventas_prem_92"))
    + when(isnull(col("imp_tot_ventas_reg_87")), 0).otherwise(col("imp_tot_ventas_reg_87")))
.withColumn("imp_tot_mm_ing_net", when(isnull(col("imp_ees_ing_net")), 0).otherwise(col("imp_ees_ing_net")))
.withColumn("imp_tot_mm_coste_prod_2_m_prim", when(isnull(col("imp_ees_coste_prod_2_mat_prima")), 0).otherwise(col("imp_ees_coste_prod_2_mat_prima")))
.withColumn("imp_tot_mm_coste_prod_2_adt", when(isnull(col("imp_ees_coste_prod_2_aditivos")), 0).otherwise(col("imp_ees_coste_prod_2_aditivos")))
.withColumn("imp_tot_mm_coste_prod_2_mermas", when(isnull(col("imp_ees_coste_prod_2_mermas")), 0).otherwise(col("imp_ees_coste_prod_2_mermas")))
.withColumn("imp_tot_mm_coste_prod_2_desc", when(isnull(col("imp_ees_coste_prod_2_desc_comp")), 0).otherwise(col("imp_ees_coste_prod_2_desc_comp")))
.withColumn("imp_tot_mm_coste_prod", when(isnull(col("imp_tot_mm_coste_prod_2_m_prim")), 0).otherwise(col("imp_tot_mm_coste_prod_2_m_prim"))
    + when(isnull(col("imp_tot_mm_coste_prod_2_adt")), 0).otherwise(col("imp_tot_mm_coste_prod_2_adt"))
    + when(isnull(col("imp_tot_mm_coste_prod_2_mermas")), 0).otherwise(col("imp_tot_mm_coste_prod_2_mermas"))
    + when(isnull(col("imp_tot_mm_coste_prod_2_desc")), 0).otherwise(col("imp_tot_mm_coste_prod_2_desc")))
.withColumn("imp_tot_mm_mb", when(isnull(col("imp_tot_mm_ing_net")), 0).otherwise(col("imp_tot_mm_ing_net"))
    - when(isnull(col("imp_tot_mm_coste_prod")), 0).otherwise(col("imp_tot_mm_coste_prod")))
.withColumn("imp_tot_mm_cost_log_dist_2_tsp", when(isnull(col("imp_ees_coste_prod_2_desc_comp")), 0).otherwise(col("imp_ees_coste_prod_2_desc_comp")))
.withColumn("imp_tot_mm_coste_logist_dist", when(isnull(col("imp_tot_mm_cost_log_dist_2_tsp")), 0).otherwise(col("imp_tot_mm_cost_log_dist_2_tsp")))
.withColumn("imp_tot_mm_extram_olstor", when(isnull(col("imp_ees_extram_olstor")), 0).otherwise(col("imp_ees_extram_olstor")))
.withColumn("imp_tot_mm_extram_monterra", when(isnull(col("imp_ees_extram_monterra")), 0).otherwise(col("imp_ees_extram_monterra")))
.withColumn("imp_tot_mm_coste_canal", when(isnull(col("imp_tot_mm_extram_olstor")), 0).otherwise(col("imp_tot_mm_extram_olstor"))
    + when(isnull(col("imp_tot_mm_extram_monterra")), 0).otherwise(col("imp_tot_mm_extram_monterra")))
.withColumn("imp_tot_mm_mc", when(isnull(col("imp_tot_mm_mb")), 0).otherwise(col("imp_tot_mm_mb"))
    - when(isnull(col("imp_tot_mm_coste_logist_dist")), 0).otherwise(col("imp_tot_mm_coste_logist_dist"))
    - when(isnull(col("imp_tot_mm_coste_canal")), 0).otherwise(col("imp_tot_mm_coste_canal")))
.withColumn("imp_tot_mm_mb_sin_desc_cv", when(isnull(col("imp_ees_mb_sin_des_ing_c_prima")), 0).otherwise(col("imp_ees_mb_sin_des_ing_c_prima")))
.withColumn("imp_tot_mm_cv_tot", when(isnull(col("imp_tot_mm_coste_prod")), 0).otherwise(col("imp_tot_mm_coste_prod"))
    + when(isnull(col("imp_tot_mm_coste_logist_dist")), 0).otherwise(col("imp_tot_mm_coste_logist_dist"))
    + when(isnull(col("imp_tot_mm_coste_canal")), 0).otherwise(col("imp_tot_mm_coste_canal")))
.withColumn("imp_tot_mm_cv_unit", when(isnull(col("imp_tot_mm_cv_tot")), 0).otherwise(col("imp_tot_mm_cv_tot"))
    / when(isnull(col("imp_vvdd_ventas")), 0).otherwise(col("imp_vvdd_ventas")))
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
.withColumn("imp_tot_mm_cf", when(isnull(col("imp_ees_cf")), 0).otherwise(col("imp_ees_cf"))
    + when(isnull(col("imp_ctral_cost_fijos")), 0).otherwise(col("imp_ctral_cost_fijos")))
.withColumn("imp_tot_mm_amort", when(isnull(col("imp_ees_amort")), 0).otherwise(col("imp_ees_amort"))
    + when(isnull(col("imp_vvdd_amort")), 0).otherwise(col("imp_vvdd_amort"))
    + when(isnull(col("imp_ctral_amort")), 0).otherwise(col("imp_ctral_amort")))
.withColumn("imp_tot_mm_provis_recur", when(isnull(col("imp_ees_provis_recur")), 0).otherwise(col("imp_ees_provis_recur"))
    + when(isnull(col("imp_ctral_provis_recur")), 0).otherwise(col("imp_ctral_provis_recur"))
    + when(isnull(col("imp_vvdd_provis_recur")), 0).otherwise(col("imp_vvdd_provis_recur"))
)
.withColumn("imp_tot_mm_otros_result", when(isnull(col("imp_ees_otros_result")), 0).otherwise(col("imp_ees_otros_result"))
    + when(isnull(col("imp_vvdd_otros_result")), 0).otherwise(col("imp_vvdd_otros_result"))
    + when(isnull(col("imp_ctral_otros_result")), 0).otherwise(col("imp_ctral_otros_result"))
)
.withColumn("imp_tot_mm_result_op_ppal", when(isnull(col("imp_tot_mm_mc_ees")), 0).otherwise(col("imp_tot_mm_mc_ees"))
    + when(isnull(col("imp_tot_mm_otros_mc")), 0).otherwise(col("imp_tot_mm_otros_mc"))
    - when(isnull(col("imp_tot_mm_cf")), 0).otherwise(col("imp_tot_mm_cf"))
    - when(isnull(col("imp_tot_mm_amort")), 0).otherwise(col("imp_tot_mm_amort"))
    - when(isnull(col("imp_tot_mm_provis_recur")), 0).otherwise(col("imp_tot_mm_provis_recur"))
    + when(isnull(col("imp_tot_mm_otros_result")), 0).otherwise(col("imp_tot_mm_otros_result")))
.withColumn("imp_tot_mm_result_otras_soc", when(isnull(col("imp_ees_EBIT_CCS_JV_mar_cortes")),0).otherwise(col("imp_ees_EBIT_CCS_JV_mar_cortes")))
.withColumn("imp_tot_mm_result_op", when(isnull(col("imp_tot_mm_result_op_ppal")), 0).otherwise(col("imp_tot_mm_result_op_ppal"))
     + when(isnull(col("imp_tot_mm_result_otras_soc")), 0).otherwise(col("imp_tot_mm_result_otras_soc")))
.withColumn("imp_tot_mm_pers_2_retrib", when(isnull(col("imp_ctral_pers_2_retrib")), 0).otherwise(col("imp_ctral_pers_2_retrib"))
     + when(isnull(col("imp_vvdd_pers_2_retrib")), 0).otherwise(col("imp_vvdd_pers_2_retrib"))
     + when(isnull(col("imp_ees_pers_2_retrib")), 0).otherwise(col("imp_ees_pers_2_retrib")))
.withColumn("imp_tot_mm_pers_2_otro_cost", when(isnull(col("imp_ctral_pers_2_otros_cost")), 0).otherwise(col("imp_ctral_pers_2_otros_cost"))
     + when(isnull(col("imp_ees_pers_2_otros_cost_pers")), 0).otherwise(col("imp_ees_pers_2_otros_cost_pers"))
     + when(isnull(col("imp_vvdd_pers_2_otro_cost_pers")), 0).otherwise(col("imp_vvdd_pers_2_otro_cost_pers")))
.withColumn("imp_tot_mm_pers", when(isnull(col("imp_tot_mm_pers_2_retrib")), 0).otherwise(col("imp_tot_mm_pers_2_retrib"))
    + when(isnull(col("imp_tot_mm_pers_2_otro_cost")), 0).otherwise(col("imp_tot_mm_pers_2_otro_cost")))
.withColumn("imp_tot_mm_serv_ext_2_arrycan", when(isnull(col("imp_ctral_serv_ext_2_arrycan")), 0).otherwise(col("imp_ctral_serv_ext_2_arrycan"))
     + when(isnull(col("imp_vvdd_serv_ext_2_arrend_can")), 0).otherwise(col("imp_vvdd_serv_ext_2_arrend_can"))
     + when(isnull(col("imp_ees_serv_ext_2_arrend_can")), 0).otherwise(col("imp_ees_serv_ext_2_arrend_can"))//añadido
)
.withColumn("imp_tot_mm_serv_ext_2_pub_rrpp", when(isnull(col("imp_ctral_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_ctral_serv_ext_2_pub_rrpp"))
     + when(isnull(col("imp_vvdd_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_vvdd_serv_ext_2_pub_rrpp"))
     + when(isnull(col("imp_ees_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_ees_serv_ext_2_pub_rrpp"))//añadido
)
.withColumn("imp_tot_mm_serv_ext_2_sumin", when(isnull(col("imp_ctral_serv_ext_2_sumin")), 0).otherwise(col("imp_ctral_serv_ext_2_sumin"))
     + when(isnull(col("imp_ees_serv_ext_2_sumin")), 0).otherwise(col("imp_ees_serv_ext_2_sumin"))
     + when(isnull(col("imp_vvdd_serv_ext_2_sumin")), 0).otherwise(col("imp_vvdd_serv_ext_2_sumin"))//añadido
)
.withColumn("imp_tot_mm_serv_ext_2_mant_rep", when(isnull(col("imp_ctral_serv_ext_2_mant_rep")), 0).otherwise(col("imp_ctral_serv_ext_2_mant_rep"))
     + when(isnull(col("imp_vvdd_serv_ext_2_mant_rep")), 0).otherwise(col("imp_vvdd_serv_ext_2_mant_rep"))
     + when(isnull(col("imp_ees_serv_ext_2_mant_rep")), 0).otherwise(col("imp_ees_serv_ext_2_mant_rep"))//añadido
)
.withColumn("imp_tot_mm_serv_ext_2_sv_prof", when(isnull(col("imp_ctral_serv_ext_2_serv_prof")), 0).otherwise(col("imp_ctral_serv_ext_2_serv_prof"))
     + when(isnull(col("imp_ees_serv_ext_2_serv_prof")), 0).otherwise(col("imp_ees_serv_ext_2_serv_prof"))
     + when(isnull(col("imp_vvdd_serv_ext_2_serv_prof")), 0).otherwise(col("imp_vvdd_serv_ext_2_serv_prof"))//añadido
)
.withColumn("imp_tot_mm_serv_ext_2_viajes", when(isnull(col("imp_ctral_serv_ext_2_viajes")), 0).otherwise(col("imp_ctral_serv_ext_2_viajes"))
     + when(isnull(col("imp_ees_serv_ext_2_viajes")), 0).otherwise(col("imp_ees_serv_ext_2_viajes"))
     + when(isnull(col("imp_vvdd_serv_ext_2_viajes")), 0).otherwise(col("imp_vvdd_serv_ext_2_viajes"))//añadido
)
.withColumn("imp_tot_mm_serv_ext_2_seg", when(isnull(col("imp_ctral_serv_ext_2_seg")), 0).otherwise(col("imp_ctral_serv_ext_2_seg"))
     + when(isnull(col("imp_vvdd_serv_ext_2_seg")), 0).otherwise(col("imp_vvdd_serv_ext_2_seg"))
     + when(isnull(col("imp_ees_serv_ext_2_seg")), 0).otherwise(col("imp_ees_serv_ext_2_seg"))//añadido
)
.withColumn("imp_tot_mm_serv_ext_2_trib", when(isnull(col("imp_ctral_serv_ext_2_trib")), 0).otherwise(col("imp_ctral_serv_ext_2_trib"))
     + when(isnull(col("imp_ees_serv_ext_2_trib")), 0).otherwise(col("imp_ees_serv_ext_2_trib"))
     + when(isnull(col("imp_vvdd_serv_ext_2_trib")), 0).otherwise(col("imp_vvdd_serv_ext_2_trib"))//añadido
)
.withColumn("imp_tot_mm_serv_ext_2_otro_sv", when(isnull(col("imp_ctral_sv_ext_2_otr_sv_ext")), 0).otherwise(col("imp_ctral_sv_ext_2_otr_sv_ext"))
     + when(isnull(col("imp_vvdd_sv_ext_2_otros_sv_ext")), 0).otherwise(col("imp_vvdd_sv_ext_2_otros_sv_ext"))
     + when(isnull(col("imp_ees_sv_ext_2_otros_sv_ext")), 0).otherwise(col("imp_ees_sv_ext_2_otros_sv_ext"))//añadido
)
.withColumn("imp_tot_mm_serv_ext", when(isnull(col("imp_tot_mm_serv_ext_2_arrycan")), 0).otherwise(col("imp_tot_mm_serv_ext_2_arrycan"))
    + when(isnull(col("imp_tot_mm_serv_ext_2_pub_rrpp")), 0).otherwise(col("imp_tot_mm_serv_ext_2_pub_rrpp"))
    + when(isnull(col("imp_tot_mm_serv_ext_2_sumin")), 0).otherwise(col("imp_tot_mm_serv_ext_2_sumin"))
    + when(isnull(col("imp_tot_mm_serv_ext_2_mant_rep")), 0).otherwise(col("imp_tot_mm_serv_ext_2_mant_rep"))
    + when(isnull(col("imp_tot_mm_serv_ext_2_sv_prof")), 0).otherwise(col("imp_tot_mm_serv_ext_2_sv_prof"))
    + when(isnull(col("imp_tot_mm_serv_ext_2_viajes")), 0).otherwise(col("imp_tot_mm_serv_ext_2_viajes"))
    + when(isnull(col("imp_tot_mm_serv_ext_2_seg")), 0).otherwise(col("imp_tot_mm_serv_ext_2_seg"))
    + when(isnull(col("imp_tot_mm_serv_ext_2_trib")), 0).otherwise(col("imp_tot_mm_serv_ext_2_trib"))
    + when(isnull(col("imp_tot_mm_serv_ext_2_otro_sv")), 0).otherwise(col("imp_tot_mm_serv_ext_2_otro_sv")))
.withColumn("imp_tot_mm_serv_corp", when(isnull(col("imp_ctral_serv_corp")), 0).otherwise(col("imp_ctral_serv_corp"))
     + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp"))
     + when(isnull(col("imp_vvdd_serv_corp")), 0).otherwise(col("imp_vvdd_serv_corp"))//añadido
)
.withColumn("imp_tot_mm_serv_transv_DG", when(isnull(col("imp_ctral_serv_transv_DG")), 0).otherwise(col("imp_ctral_serv_transv_DG"))
     + when(isnull(col("imp_ees_serv_transv_DG")), 0).otherwise(col("imp_ees_serv_transv_DG"))
     + when(isnull(col("imp_vvdd_serv_transv_DG")), 0).otherwise(col("imp_vvdd_serv_transv_DG"))//añadido
)
.withColumn("imp_tot_mm_otros_serv_DG", when(isnull(col("imp_ctral_otros_serv_DG")), 0).otherwise(col("imp_ctral_otros_serv_DG"))
     + when(isnull(col("imp_vvdd_otros_serv_DG")), 0).otherwise(col("imp_vvdd_otros_serv_DG"))
     + when(isnull(col("imp_ees_otros_serv_DG")), 0).otherwise(col("imp_ees_otros_serv_DG"))//añadido
)
.withColumn("imp_tot_mm_tot_cf_concep", when(isnull(col("imp_tot_mm_pers")), 0).otherwise(col("imp_tot_mm_pers"))
    + when(isnull(col("imp_tot_mm_serv_ext")), 0).otherwise(col("imp_tot_mm_serv_ext"))
    + when(isnull(col("imp_tot_mm_serv_corp")), 0).otherwise(col("imp_tot_mm_serv_corp"))
    + when(isnull(col("imp_tot_mm_serv_transv_DG")), 0).otherwise(col("imp_tot_mm_serv_transv_DG"))
    + when(isnull(col("imp_tot_mm_otros_serv_DG")), 0).otherwise(col("imp_tot_mm_otros_serv_DG")))
.withColumn("imp_tot_mm_cf_ees_2_mant_rep", when(isnull(col("imp_ees_cf_ees_2_mant_rep")), 0).otherwise(col("imp_ees_cf_ees_2_mant_rep")))
.withColumn("imp_tot_mm_cf_ees_2_neotech", when(isnull(col("imp_ees_cf_ees_2_tec_neotech")), 0).otherwise(col("imp_ees_cf_ees_2_tec_neotech")))
.withColumn("imp_tot_mm_cf_ees_2_ctrl_volu", when(isnull(col("imp_ees_cf_ees_2_sis_ctrl_volu")), 0).otherwise(col("imp_ees_cf_ees_2_sis_ctrl_volu")))
.withColumn("imp_tot_mm_cf_ees_2_act_promo", when(isnull(col("imp_ees_cf_ees_2_act_promo")), 0).otherwise(col("imp_ees_cf_ees_2_act_promo")))
.withColumn("imp_tot_mm_cf_ees_2_otro_cf", when(isnull(col("imp_ees_cf_ees_2_otros_cf_ees")), 0).otherwise(col("imp_ees_cf_ees_2_otros_cf_ees")))
.withColumn("imp_tot_mm_cf_ees", when(isnull(col("imp_tot_mm_cf_ees_2_mant_rep")), 0).otherwise(col("imp_tot_mm_cf_ees_2_mant_rep"))
    + when(isnull(col("imp_tot_mm_cf_ees_2_neotech")), 0).otherwise(col("imp_tot_mm_cf_ees_2_neotech"))
    + when(isnull(col("imp_tot_mm_cf_ees_2_ctrl_volu")), 0).otherwise(col("imp_tot_mm_cf_ees_2_ctrl_volu"))
    + when(isnull(col("imp_tot_mm_cf_ees_2_act_promo")), 0).otherwise(col("imp_tot_mm_cf_ees_2_act_promo"))
    + when(isnull(col("imp_tot_mm_cf_ees_2_otro_cf")), 0).otherwise(col("imp_tot_mm_cf_ees_2_otro_cf")))
.withColumn("imp_tot_mm_cf_estr_2_pers", when(isnull(col("imp_ctral_cf_est_2_pers")), 0).otherwise(col("imp_ctral_cf_est_2_pers")))
.withColumn("imp_tot_mm_cf_estr_2_viajes", when(isnull(col("imp_ctral_serv_ext_2_viajes")), 0).otherwise(col("imp_ctral_serv_ext_2_viajes")))
.withColumn("imp_tot_mm_cf_estr_2_com_rrpp", when(isnull(col("imp_ctral_cf_est_2_com_rrpp")), 0).otherwise(col("imp_ctral_cf_est_2_com_rrpp")))
.withColumn("imp_tot_mm_cf_estr_2_serv_prof", when(isnull(col("imp_ctral_cf_est_2_serv_prof")), 0).otherwise(col("imp_ctral_cf_est_2_serv_prof")))
.withColumn("imp_tot_mm_cf_estr_2_prima_seg", when(isnull(col("imp_ctral_cf_est_2_primas_seg")), 0).otherwise(col("imp_ctral_cf_est_2_primas_seg")))
.withColumn("imp_tot_mm_cf_estr_2_sv_banc_s", when(isnull(col("imp_ctral_cf_est_2_serv_banc")), 0).otherwise(col("imp_ctral_cf_est_2_serv_banc")))
.withColumn("imp_tot_mm_cf_estr_2_pub_rrpp", when(isnull(col("imp_ctral_cf_est_2_pub_rrpp")), 0).otherwise(col("imp_ctral_cf_est_2_pub_rrpp")))
.withColumn("imp_tot_mm_cf_estr_2_sumin", when(isnull(col("imp_ctral_cf_est_2_sumin")), 0).otherwise(col("imp_ctral_cf_est_2_sumin")))
.withColumn("imp_tot_mm_cf_estr_2_otros_sv", when(isnull(col("imp_ctral_cf_est_2_otros_serv")), 0).otherwise(col("imp_ctral_cf_est_2_otros_serv")))
.withColumn("imp_tot_mm_cf_estr", when(isnull(col("imp_tot_mm_cf_estr_2_pers")), 0).otherwise(col("imp_tot_mm_cf_estr_2_pers"))
    + when(isnull(col("imp_tot_mm_cf_estr_2_viajes")), 0).otherwise(col("imp_tot_mm_cf_estr_2_viajes"))
    + when(isnull(col("imp_tot_mm_cf_estr_2_com_rrpp")), 0).otherwise(col("imp_tot_mm_cf_estr_2_com_rrpp"))
    + when(isnull(col("imp_tot_mm_cf_estr_2_serv_prof")), 0).otherwise(col("imp_tot_mm_cf_estr_2_serv_prof"))
    + when(isnull(col("imp_tot_mm_cf_estr_2_prima_seg")), 0).otherwise(col("imp_tot_mm_cf_estr_2_prima_seg"))
    + when(isnull(col("imp_tot_mm_cf_estr_2_sv_banc_s")), 0).otherwise(col("imp_tot_mm_cf_estr_2_sv_banc_s"))
    + when(isnull(col("imp_tot_mm_cf_estr_2_pub_rrpp")), 0).otherwise(col("imp_tot_mm_cf_estr_2_pub_rrpp"))
    + when(isnull(col("imp_tot_mm_cf_estr_2_sumin")), 0).otherwise(col("imp_tot_mm_cf_estr_2_sumin"))
    + when(isnull(col("imp_tot_mm_cf_estr_2_otros_sv")), 0).otherwise(col("imp_tot_mm_cf_estr_2_otros_sv")))
.withColumn("imp_tot_mm_otros_gastos", when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos"))
    + when(isnull(col("imp_vvdd_otros_gastos")), 0).otherwise(col("imp_vvdd_otros_gastos"))
    + when(isnull(col("imp_ctral_otros_gastos")), 0).otherwise(col("imp_ctral_otros_gastos")))
.withColumn("imp_tot_mm_tot_cf_analit", when(isnull(col("imp_tot_mm_cf_ees")), 0).otherwise(col("imp_tot_mm_cf_ees"))
    + when(isnull(col("imp_tot_mm_cf_estr")), 0).otherwise(col("imp_tot_mm_cf_estr"))
    + when(isnull(col("imp_tot_mm_otros_gastos")), 0).otherwise(col("imp_tot_mm_otros_gastos"))
    + when(isnull(col("imp_tot_mm_serv_corp")), 0).otherwise(col("imp_tot_mm_serv_corp")))
.withColumn("imp_tot_mm_tot_cf_an_upa_est", when(isnull(col("imp_ctral_cost_fij_estruct")), 0).otherwise(col("imp_ctral_cost_fij_estruct"))
    + when(isnull(col("imp_tot_mm_otros_gastos")), 0).otherwise(col("imp_tot_mm_otros_gastos"))
    + when(isnull(col("imp_tot_mm_cf_ees")), 0).otherwise(col("imp_tot_mm_cf_ees")))
.withColumn("imp_ees_bfc_real_serv_corp",when(isnull(col("imp_2049_imp_pyo")), 0).otherwise(col("imp_2049_imp_pyo"))
    + when(isnull(col("imp_2049_imp_patrimonial_seg")), 0).otherwise(col("imp_2049_imp_patrimonial_seg"))
    + when(isnull(col("imp_2049_imp_comunicacion")), 0).otherwise(col("imp_2049_imp_comunicacion"))
    + when(isnull(col("imp_2049_imp_techlab")), 0).otherwise(col("imp_2049_imp_techlab"))
    + when(isnull(col("imp_2049_imp_ti")), 0).otherwise(col("imp_2049_imp_ti"))
    + when(isnull(col("imp_2049_imp_juridico")), 0).otherwise(col("imp_2049_imp_juridico"))
    + when(isnull(col("imp_2049_imp_ingenieria")), 0).otherwise(col("imp_2049_imp_ingenieria"))
    + when(isnull(col("imp_2049_imp_ser_corporativos")), 0).otherwise(col("imp_2049_imp_ser_corporativos"))
    + when(isnull(col("imp_2049_imp_digitalizacion")), 0).otherwise(col("imp_2049_imp_digitalizacion"))
    + when(isnull(col("imp_2049_imp_sostenibilidad")), 0).otherwise(col("imp_2049_imp_sostenibilidad"))
    + when(isnull(col("imp_2049_imp_seguros")), 0).otherwise(col("imp_2049_imp_seguros"))
    + when(isnull(col("imp_2049_imp_auditoria")), 0).otherwise(col("imp_2049_imp_auditoria"))
    + when(isnull(col("imp_2049_imp_planif_control")), 0).otherwise(col("imp_2049_imp_planif_control"))
    + when(isnull(col("imp_2049_imp_compras")), 0).otherwise(col("imp_2049_imp_compras"))
    + when(isnull(col("imp_2049_imp_financiero")), 0).otherwise(col("imp_2049_imp_financiero"))
    + when(isnull(col("imp_2049_imp_otros")), 0).otherwise(col("imp_2049_imp_otros"))
    + when(isnull(col("imp_2049_imp_fiscal")), 0).otherwise(col("imp_2049_imp_fiscal"))
    + when(isnull(col("imp_2049_imp_econom_admin")), 0).otherwise(col("imp_2049_imp_econom_admin"))
    + when(isnull(col("imp_2049_imp_serv_globales")), 0).otherwise(col("imp_2049_imp_serv_globales")).cast(DecimalType(30,10)))
.withColumn("imp_ees_bfc_real_tot_cf_concep", when(isnull(col("imp_ees_pers")), 0).otherwise(col("imp_ees_pers"))
    + when(isnull(col("imp_ees_serv_ext")), 0).otherwise(col("imp_ees_serv_ext"))
    + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp")) // usar ser crp del fichero
    + when(isnull(col("imp_ees_serv_transv_DG")), 0).otherwise(col("imp_ees_serv_transv_DG"))
    + when(isnull(col("imp_ees_otros_serv_DG")), 0).otherwise(col("imp_ees_otros_serv_DG")))
.withColumn("imp_ees_bfc_real_tot_cf_analit", when(isnull(col("imp_ees_cf_ees")), 0).otherwise(col("imp_ees_cf_ees"))
    + when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos")))
.withColumn("imp_ees_bfc_real_result_op_ppl", when(isnull(col("imp_ees_mc_ees")), 0).otherwise(col("imp_ees_mc_ees"))
    + when(isnull(col("imp_ees_otros_mc")), 0).otherwise(col("imp_ees_otros_mc"))
    - when(isnull(col("imp_ees_bfc_real_tot_cf_analit")), 0).otherwise(col("imp_ees_bfc_real_tot_cf_analit"))
    - when(isnull(col("imp_ees_amort")), 0).otherwise(col("imp_ees_amort"))
    - when(isnull(col("imp_ees_provis_recur")), 0).otherwise(col("imp_ees_provis_recur"))
    + when(isnull(col("imp_ees_otros_result")), 0).otherwise(col("imp_ees_otros_result")))
.withColumn("imp_ees_bfc_real_result_op", when(isnull(col("imp_ees_bfc_real_result_op_ppl")), 0).otherwise(col("imp_ees_bfc_real_result_op_ppl"))
    + when(isnull(col("imp_ees_result_otras_soc")), 0).otherwise(col("imp_ees_result_otras_soc")))
.withColumn("imp_ees_bfc_pa_serv_corp",when(isnull(col("imp_2049_imp_pyo")), 0).otherwise(col("imp_2049_imp_pyo"))
    + when(isnull(col("imp_2049_imp_patrimonial_seg")), 0).otherwise(col("imp_2049_imp_patrimonial_seg"))
    + when(isnull(col("imp_2049_imp_comunicacion")), 0).otherwise(col("imp_2049_imp_comunicacion"))
    + when(isnull(col("imp_2049_imp_techlab")), 0).otherwise(col("imp_2049_imp_techlab"))
    + when(isnull(col("imp_2049_imp_ti")), 0).otherwise(col("imp_2049_imp_ti"))
    //+ when(isnull(col("imp_2049_imp_ti_as_a_service")), 0).otherwise(col("imp_2049_imp_ti_as_a_service"))
    + when(isnull(col("imp_2049_imp_juridico")), 0).otherwise(col("imp_2049_imp_juridico"))
    + when(isnull(col("imp_2049_imp_ingenieria")), 0).otherwise(col("imp_2049_imp_ingenieria"))
    + when(isnull(col("imp_2049_imp_ser_corporativos")), 0).otherwise(col("imp_2049_imp_ser_corporativos"))
    + when(isnull(col("imp_2049_imp_digitalizacion")), 0).otherwise(col("imp_2049_imp_digitalizacion"))
    + when(isnull(col("imp_2049_imp_sostenibilidad")), 0).otherwise(col("imp_2049_imp_sostenibilidad"))
    + when(isnull(col("imp_2049_imp_seguros")), 0).otherwise(col("imp_2049_imp_seguros"))
    + when(isnull(col("imp_2049_imp_auditoria")), 0).otherwise(col("imp_2049_imp_auditoria"))
    + when(isnull(col("imp_2049_imp_planif_control")), 0).otherwise(col("imp_2049_imp_planif_control"))
    + when(isnull(col("imp_2049_imp_compras")), 0).otherwise(col("imp_2049_imp_compras"))
    + when(isnull(col("imp_2049_imp_financiero")), 0).otherwise(col("imp_2049_imp_financiero"))
    + when(isnull(col("imp_2049_imp_otros")), 0).otherwise(col("imp_2049_imp_otros"))
    + when(isnull(col("imp_2049_imp_fiscal")), 0).otherwise(col("imp_2049_imp_fiscal"))
    + when(isnull(col("imp_2049_imp_econom_admin")), 0).otherwise(col("imp_2049_imp_econom_admin"))
    + when(isnull(col("imp_2049_imp_serv_globales")), 0).otherwise(col("imp_2049_imp_serv_globales")))
.withColumn("imp_ees_bfc_pa_tot_cf_concep", when(isnull(col("imp_ees_pers")), 0).otherwise(col("imp_ees_pers"))
    + when(isnull(col("imp_ees_serv_ext")), 0).otherwise(col("imp_ees_serv_ext"))
    + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp"))
    + when(isnull(col("imp_ees_serv_transv_DG")), 0).otherwise(col("imp_ees_serv_transv_DG"))
    + when(isnull(col("imp_ees_otros_serv_DG")), 0).otherwise(col("imp_ees_otros_serv_DG")))
.withColumn("imp_ees_bfc_pa_tot_cf_analit", when(isnull(col("imp_ees_cf_ees")), 0).otherwise(col("imp_ees_cf_ees"))
    + when(isnull(col("imp_ees_otros_gastos")), 0).otherwise(col("imp_ees_otros_gastos"))
    + when(isnull(col("imp_ees_serv_corp")), 0).otherwise(col("imp_ees_serv_corp")))
.withColumn("imp_ees_bfc_pa_result_op_ppal", when(isnull(col("imp_ees_mc_ees")), 0).otherwise(col("imp_ees_mc_ees"))
    + when(isnull(col("imp_ees_otros_mc")), 0).otherwise(col("imp_ees_otros_mc"))
    - when(isnull(col("imp_ees_bfc_pa_tot_cf_analit")), 0).otherwise(col("imp_ees_bfc_pa_tot_cf_analit"))
    - when(isnull(col("imp_ees_amort")), 0).otherwise(col("imp_ees_amort"))
    - when(isnull(col("imp_ees_provis_recur")), 0).otherwise(col("imp_ees_provis_recur"))
    + when(isnull(col("imp_ees_otros_result")), 0).otherwise(col("imp_ees_otros_result")))
.withColumn("imp_ees_bfc_pa_result_op", when(isnull(col("imp_ees_bfc_pa_result_op_ppal")), 0).otherwise(col("imp_ees_bfc_pa_result_op_ppal"))
    + when(isnull(col("imp_ees_result_otras_soc")), 0).otherwise(col("imp_ees_result_otras_soc")))
.withColumn("imp_ees_mino_bfc_real_tot_cf_c", when(isnull(col("imp_ees_minor_pers")), 0).otherwise(col("imp_ees_minor_pers"))
    + when(isnull(col("imp_ees_minor_serv_ext")), 0).otherwise(col("imp_ees_minor_serv_ext"))
    + when(isnull(col("imp_ees_minor_serv_corp")), 0).otherwise(col("imp_ees_minor_serv_corp"))
    + when(isnull(col("imp_ees_minor_serv_transv_DG")), 0).otherwise(col("imp_ees_minor_serv_transv_DG"))
    + when(isnull(col("imp_ees_minor_otros_serv_DG")), 0).otherwise(col("imp_ees_minor_otros_serv_DG")))
.withColumn("imp_ees_minor_bfc_real_resu_op", when(isnull(col("imp_ees_minor_otros_mc")), 0).otherwise(col("imp_ees_minor_otros_mc"))
    - when(isnull(col("imp_ees_minor_tot_cf_analit")), 0).otherwise(col("imp_ees_minor_tot_cf_analit"))
    - when(isnull(col("imp_ees_minor_amort")), 0).otherwise(col("imp_ees_minor_amort"))
    - when(isnull(col("imp_ees_minor_provis_recur")), 0).otherwise(col("imp_ees_minor_provis_recur"))
    + when(isnull(col("imp_ees_minor_otros_result")), 0).otherwise(col("imp_ees_minor_otros_result")))
.withColumn("imp_ees_minor_bfc_pa_tot_cf_c", when(isnull(col("imp_ees_minor_pers")), 0).otherwise(col("imp_ees_minor_pers"))
    + when(isnull(col("imp_ees_minor_serv_ext")), 0).otherwise(col("imp_ees_minor_serv_ext"))
    + when(isnull(col("imp_ees_minor_serv_corp")), 0).otherwise(col("imp_ees_minor_serv_corp"))
    + when(isnull(col("imp_ees_minor_serv_transv_DG")), 0).otherwise(col("imp_ees_minor_serv_transv_DG"))
    + when(isnull(col("imp_ees_minor_otros_serv_DG")), 0).otherwise(col("imp_ees_minor_otros_serv_DG")))
.withColumn("imp_ees_minor_bfc_pa_result_op", when(isnull(col("imp_ees_minor_otros_mc")), 0).otherwise(col("imp_ees_minor_otros_mc"))
    - when(isnull(col("imp_ees_minor_tot_cf_analit")), 0).otherwise(col("imp_ees_minor_tot_cf_analit"))
    - when(isnull(col("imp_ees_minor_amort")), 0).otherwise(col("imp_ees_minor_amort"))
    - when(isnull(col("imp_ees_minor_provis_recur")), 0).otherwise(col("imp_ees_minor_provis_recur"))
    + when(isnull(col("imp_ees_minor_otros_result")), 0).otherwise(col("imp_ees_minor_otros_result")))
.withColumn("imp_ctral_real_otros_serv_DG", when(isnull(col("imp_otros_serv_dg_crc")), 0).otherwise(col("imp_otros_serv_dg_crc"))
    + when(isnull(col("imp_otros_serv_dg_e_commerce")), 0).otherwise(col("imp_otros_serv_dg_e_commerce"))
    + when(isnull(col("imp_otros_serv_dg_fidel_global")), 0).otherwise(col("imp_otros_serv_dg_fidel_global"))
    + when(isnull(col("imp_otros_serv_dg_int_cliente")), 0).otherwise(col("imp_otros_serv_dg_int_cliente"))
    + when(isnull(col("imp_otros_serv_dg_mkt_cloud")), 0).otherwise(col("imp_otros_serv_dg_mkt_cloud")))
.withColumn("imp_ctral_real_serv_transv_DG", when(isnull(col("imp_serv_trans_otros_ser_trans")), 0).otherwise(col("imp_serv_trans_otros_ser_trans"))
    + when(isnull(col("imp_serv_transv_pyc_cliente")), 0).otherwise(col("imp_serv_transv_pyc_cliente")) 
    + when(isnull(col("imp_serv_transv_pyo_cliente")), 0).otherwise(col("imp_serv_transv_pyo_cliente"))
    + when(isnull(col("imp_serv_transv_sost_cliente")), 0).otherwise(col("imp_serv_transv_sost_cliente")))
.withColumn("imp_2049_imp_impuestos", when(isnull(col("imp_2049_imp_impuestos_prev")), 0).otherwise(col("imp_2049_imp_impuestos_prev") * -1))
    .drop("imp_2049_imp_impuestos_prev")
    .withColumn("imp_bfc_impuestos", when(isnull(col("imp_2049_imp_impuestos")), 0).otherwise(col("imp_2049_imp_impuestos")))
.withColumn("imp_bfc_resultado_esp_adi", when(isnull(col("imp_2049_imp_resul_espec_adi")), 0).otherwise(col("imp_2049_imp_resul_espec_adi")))
.withColumn("imp_bfc_minoritarios", when(isnull(col("imp_2049_imp_minoritarios")), 0).otherwise(col("imp_2049_imp_minoritarios")))
.withColumn("imp_bfc_participadas", when(isnull(col("imp_2049_imp_participadas")), 0).otherwise(col("imp_2049_imp_participadas")))
.withColumn("imp_bfc_otros", when(isnull(col("imp_2049_imp_otros")), 0).otherwise(col("imp_2049_imp_otros")))
.withColumn("imp_bfc_pyo", when(isnull(col("imp_2049_imp_pyo")), 0).otherwise(col("imp_2049_imp_pyo")))
.withColumn("imp_bfc_ti", when(isnull(col("imp_2049_imp_ti")), 0).otherwise(col("imp_2049_imp_ti")))
.withColumn("imp_bfc_real_serv_corp", when(isnull(col("imp_2049_imp_pyo")), 0).otherwise(col("imp_2049_imp_pyo"))
    + when(isnull(col("imp_2049_imp_patrimonial_seg")), 0).otherwise(col("imp_2049_imp_patrimonial_seg"))
    + when(isnull(col("imp_2049_imp_comunicacion")), 0).otherwise(col("imp_2049_imp_comunicacion"))
    + when(isnull(col("imp_2049_imp_techlab")), 0).otherwise(col("imp_2049_imp_techlab"))
    + when(isnull(col("imp_2049_imp_ti")), 0).otherwise(col("imp_2049_imp_ti"))
    //+ when(isnull(col("imp_2049_imp_ti_as_a_service")), 0).otherwise(col("imp_2049_imp_ti_as_a_service"))
    + when(isnull(col("imp_2049_imp_juridico")), 0).otherwise(col("imp_2049_imp_juridico"))
    + when(isnull(col("imp_2049_imp_ingenieria")), 0).otherwise(col("imp_2049_imp_ingenieria"))
    + when(isnull(col("imp_2049_imp_ser_corporativos")), 0).otherwise(col("imp_2049_imp_ser_corporativos"))
    + when(isnull(col("imp_2049_imp_digitalizacion")), 0).otherwise(col("imp_2049_imp_digitalizacion"))
    + when(isnull(col("imp_2049_imp_sostenibilidad")), 0).otherwise(col("imp_2049_imp_sostenibilidad"))
    + when(isnull(col("imp_2049_imp_seguros")), 0).otherwise(col("imp_2049_imp_seguros"))
    + when(isnull(col("imp_2049_imp_auditoria")), 0).otherwise(col("imp_2049_imp_auditoria"))
    + when(isnull(col("imp_2049_imp_planif_control")), 0).otherwise(col("imp_2049_imp_planif_control"))
    + when(isnull(col("imp_2049_imp_compras")), 0).otherwise(col("imp_2049_imp_compras"))
    + when(isnull(col("imp_2049_imp_financiero")), 0).otherwise(col("imp_2049_imp_financiero"))
    + when(isnull(col("imp_2049_imp_otros")), 0).otherwise(col("imp_2049_imp_otros"))
    + when(isnull(col("imp_2049_imp_fiscal")), 0).otherwise(col("imp_2049_imp_fiscal"))
    + when(isnull(col("imp_2049_imp_econom_admin")), 0).otherwise(col("imp_2049_imp_econom_admin"))
    + when(isnull(col("imp_2049_imp_serv_globales")), 0).otherwise(col("imp_2049_imp_serv_globales")))
.withColumn("imp_ctral_total_bfc_real_cf_c", when(isnull(col("imp_ctral_pers")), 0).otherwise(col("imp_ctral_pers"))
    + when(isnull(col("imp_ctral_ser_ext")), 0).otherwise(col("imp_ctral_ser_ext"))
    + when(isnull(col("imp_bfc_real_serv_corp")), 0).otherwise(col("imp_bfc_real_serv_corp"))
    + when(isnull(col("imp_ctral_real_serv_transv_DG")), 0).otherwise(col("imp_ctral_real_serv_transv_DG"))
    + when(isnull(col("imp_ctral_real_otros_serv_DG")), 0).otherwise(col("imp_ctral_real_otros_serv_DG")))
.withColumn("imp_c_real_total_cf_analitica", when(isnull(col("imp_ctral_cost_fij_estruct")), 0).otherwise(col("imp_ctral_cost_fij_estruct"))
    //+ when(isnull(col("imp_ctral_otros_gastos")), 0).otherwise(col("imp_ctral_otros_gastos")))
    + when(isnull(col("imp_bfc_real_serv_corp")), 0).otherwise(col("imp_bfc_real_serv_corp"))) 
.withColumn("imp_real_ctral_resu_ope", when(isnull(col("imp_ctral_mc")), 0).otherwise(col("imp_ctral_mc"))
    - when(isnull(col("imp_c_real_total_cf_analitica")), 0).otherwise(col("imp_c_real_total_cf_analitica"))
    - when(isnull(col("imp_ctral_amort")), 0).otherwise(col("imp_ctral_amort"))
    - when(isnull(col("imp_ctral_provis_recur")), 0).otherwise(col("imp_ctral_provis_recur"))
    + when(isnull(col("imp_ctral_otros_result")), 0).otherwise(col("imp_ctral_otros_result")))
.withColumn("imp_ctral_total_bfc_pa_cf_c", when(isnull(col("imp_ctral_pers")), 0).otherwise(col("imp_ctral_pers"))
    + when(isnull(col("imp_ctral_ser_ext")), 0).otherwise(col("imp_ctral_ser_ext"))
    + when(isnull(col("imp_ees_bfc_pa_serv_corp")), 0).otherwise(col("imp_ees_bfc_pa_serv_corp"))
    + when(isnull(col("imp_ctral_serv_transv_DG")), 0).otherwise(col("imp_ctral_serv_transv_DG"))
    + when(isnull(col("imp_ctral_otros_serv_DG")), 0).otherwise(col("imp_ctral_otros_serv_DG")))
.withColumn("imp_c_pa_total_cf_analitica", when(isnull(col("imp_ctral_cost_fij_estruct")), 0).otherwise(col("imp_ctral_cost_fij_estruct"))
    + when(isnull(col("imp_ees_bfc_pa_serv_corp")), 0).otherwise(col("imp_ees_bfc_pa_serv_corp")))
.withColumn("imp_pa_ctral_resu_ope", when(isnull(col("imp_ctral_mc")), 0).otherwise(col("imp_ctral_mc"))
    - when(isnull(col("imp_c_pa_total_cf_analitica")), 0).otherwise(col("imp_c_pa_total_cf_analitica"))
    - when(isnull(col("imp_ctral_amort")), 0).otherwise(col("imp_ctral_amort"))
    - when(isnull(col("imp_ctral_provis_recur")), 0).otherwise(col("imp_ctral_provis_recur"))
    + when(isnull(col("imp_ctral_otros_result")), 0).otherwise(col("imp_ctral_otros_result")))
.withColumn("imp_real_vvdd_tot_cf_concep", when(isnull(col("imp_vvdd_pers")), 0).otherwise(col("imp_vvdd_pers"))
    + when(isnull(col("imp_vvdd_serv_ext")), 0).otherwise(col("imp_vvdd_serv_ext"))
    + when(isnull(col("imp_vvdd_serv_corp")), 0).otherwise(col("imp_vvdd_serv_corp"))
    + when(isnull(col("imp_vvdd_serv_transv_DG")), 0).otherwise(col("imp_vvdd_serv_transv_DG"))
    + when(isnull(col("imp_vvdd_otros_serv_DG")), 0).otherwise(col("imp_vvdd_otros_serv_DG")))
.withColumn("imp_real_vvdd_result_op", when(isnull(col("imp_vvdd_otros_mc")), 0).otherwise(col("imp_vvdd_otros_mc"))
    - when(isnull(col("imp_vvdd_tot_cf_analit")), 0).otherwise(col("imp_vvdd_tot_cf_analit"))
    - when(isnull(col("imp_vvdd_amort")), 0).otherwise(col("imp_vvdd_amort"))
    - when(isnull(col("imp_vvdd_provis_recur")), 0).otherwise(col("imp_vvdd_provis_recur"))
    + when(isnull(col("imp_vvdd_otros_result")), 0).otherwise(col("imp_vvdd_otros_result")))
.withColumn("imp_pa_vvdd_tot_cf_concep", when(isnull(col("imp_vvdd_pers")), 0).otherwise(col("imp_vvdd_pers"))
    + when(isnull(col("imp_vvdd_serv_ext")), 0).otherwise(col("imp_vvdd_serv_ext"))
    + when(isnull(col("imp_vvdd_serv_corp")), 0).otherwise(col("imp_vvdd_serv_corp"))
    + when(isnull(col("imp_vvdd_serv_transv_DG")), 0).otherwise(col("imp_vvdd_serv_transv_DG"))
    + when(isnull(col("imp_vvdd_otros_serv_DG")), 0).otherwise(col("imp_vvdd_otros_serv_DG")))
.withColumn("imp_pa_vvdd_result_op", when(isnull(col("imp_vvdd_otros_mc")), 0).otherwise(col("imp_vvdd_otros_mc"))
    - when(isnull(col("imp_vvdd_tot_cf_analit")), 0).otherwise(col("imp_vvdd_tot_cf_analit"))
    - when(isnull(col("imp_vvdd_amort")), 0).otherwise(col("imp_vvdd_amort"))
    - when(isnull(col("imp_vvdd_provis_recur")), 0).otherwise(col("imp_vvdd_provis_recur"))
    + when(isnull(col("imp_vvdd_otros_result")), 0).otherwise(col("imp_vvdd_otros_result")))
.withColumn("imp_tot_bfc_real_serv_corp", when(isnull(col("imp_bfc_real_serv_corp")), 0).otherwise(col("imp_bfc_real_serv_corp")))
.withColumn("imp_tot_real_tot_cf_concep", when(isnull(col("imp_tot_mm_pers")), 0).otherwise(col("imp_tot_mm_pers"))
    + when(isnull(col("imp_tot_mm_serv_ext")), 0).otherwise(col("imp_tot_mm_serv_ext"))
    + when(isnull(col("imp_tot_bfc_real_serv_corp")), 0).otherwise(col("imp_tot_bfc_real_serv_corp"))
    + when(isnull(col("imp_ctral_real_serv_transv_DG")), 0).otherwise(col("imp_ctral_real_serv_transv_DG"))
    + when(isnull(col("imp_ctral_real_otros_serv_DG")), 0).otherwise(col("imp_ctral_real_otros_serv_DG")))
.withColumn("imp_real_mm_tot_cf_analit", when(isnull(col("imp_tot_mm_cf_ees")), 0).otherwise(col("imp_tot_mm_cf_ees"))
    + when(isnull(col("imp_tot_mm_cf_estr")), 0).otherwise(col("imp_tot_mm_cf_estr"))
    + when(isnull(col("imp_ees_bfc_real_serv_corp")), 0).otherwise(col("imp_ees_bfc_real_serv_corp")))
.withColumn("imp_tot_real_serv_transv_DG", when(isnull(col("imp_ctral_real_serv_transv_DG")), 0).otherwise(col("imp_ctral_real_serv_transv_DG")))
.withColumn("imp_tot_real_otros_transv_DG", when(isnull(col("imp_ctral_real_otros_serv_DG")), 0).otherwise(col("imp_ctral_real_otros_serv_DG")))
.withColumn("imp_tot_real_result_op_ppal", when(isnull(col("imp_tot_mm_mc_ees")), 0).otherwise(col("imp_tot_mm_mc_ees"))
    + when(isnull(col("imp_tot_mm_otros_mc")), 0).otherwise(col("imp_tot_mm_otros_mc"))
    - when(isnull(col("imp_real_mm_tot_cf_analit")), 0).otherwise(col("imp_real_mm_tot_cf_analit"))
    - when(isnull(col("imp_tot_mm_amort")), 0).otherwise(col("imp_tot_mm_amort"))
    - when(isnull(col("imp_tot_mm_provis_recur")), 0).otherwise(col("imp_tot_mm_provis_recur"))
    + when(isnull(col("imp_tot_mm_otros_result")), 0).otherwise(col("imp_tot_mm_otros_result")))
.withColumn("imp_tot_real_result_op_prev", when(isnull(col("imp_tot_real_result_op_ppal")), 0).otherwise(col("imp_tot_real_result_op_ppal"))
     + when(isnull(col("imp_tot_mm_result_otras_soc")), 0).otherwise(col("imp_tot_mm_result_otras_soc")))
     .withColumn("imp_tot_real_result_op_AJUSTE", when(col("id_escenario") === 3, col("imp_tot_real_result_op_prev") - col("imp_ebit_css_recurrente")).otherwise(0))
     .withColumn("imp_tot_real_result_op", when(isnull(col("imp_ebit_css_recurrente")), col("imp_tot_real_result_op_prev")).otherwise(col("imp_tot_real_result_op_prev") - col("imp_tot_real_result_op_AJUSTE")))
     .drop("imp_tot_real_result_op_prev")
.withColumn("imp_tot_real_result_net_ajust", when(isnull(col("imp_tot_real_result_op")), 0).otherwise(col("imp_tot_real_result_op"))
    + when(isnull(col("imp_bfc_minoritarios")), 0).otherwise(col("imp_bfc_minoritarios"))
    + when(isnull(col("imp_bfc_participadas")), 0).otherwise(col("imp_bfc_participadas"))
    - when(isnull(col("imp_bfc_impuestos")), 0).otherwise(col("imp_bfc_impuestos")))
.withColumn("imp_tot_real_result_net", when(isnull(col("imp_tot_real_result_net_ajust")), 0).otherwise(col("imp_tot_real_result_net_ajust"))
    + when(isnull(col("imp_bfc_resultado_esp_adi")), 0).otherwise(col("imp_bfc_resultado_esp_adi")))
.withColumn("imp_tot_bfc_pa_serv_corp", when(isnull(col("imp_ees_bfc_pa_serv_corp")), 0).otherwise(col("imp_ees_bfc_pa_serv_corp")))
.withColumn("imp_tot_pa_tot_cf_concep", when(isnull(col("imp_tot_mm_pers")), 0).otherwise(col("imp_tot_mm_pers"))
    + when(isnull(col("imp_tot_mm_serv_ext")), 0).otherwise(col("imp_tot_mm_serv_ext"))
    + when(isnull(col("imp_tot_bfc_pa_serv_corp")), 0).otherwise(col("imp_tot_bfc_pa_serv_corp"))
    + when(isnull(col("imp_tot_mm_serv_transv_DG")), 0).otherwise(col("imp_tot_mm_serv_transv_DG"))
    + when(isnull(col("imp_tot_mm_otros_serv_DG")), 0).otherwise(col("imp_tot_mm_otros_serv_DG")))
.withColumn("imp_pa_mm_tot_cf_analit", when(isnull(col("imp_tot_mm_cf_ees")), 0).otherwise(col("imp_tot_mm_cf_ees"))
    + when(isnull(col("imp_tot_mm_cf_estr")), 0).otherwise(col("imp_tot_mm_cf_estr"))
    + when(isnull(col("imp_tot_mm_otros_gastos")), 0).otherwise(col("imp_tot_mm_otros_gastos")))
.withColumn("imp_tot_pa_result_op_ppal", when(isnull(col("imp_tot_mm_mc_ees")), 0).otherwise(col("imp_tot_mm_mc_ees"))
    + when(isnull(col("imp_tot_mm_otros_mc")), 0).otherwise(col("imp_tot_mm_otros_mc"))
    - when(isnull(col("imp_pa_mm_tot_cf_analit")), 0).otherwise(col("imp_pa_mm_tot_cf_analit"))
    - when(isnull(col("imp_tot_mm_amort")), 0).otherwise(col("imp_tot_mm_amort"))
    - when(isnull(col("imp_tot_mm_provis_recur")), 0).otherwise(col("imp_tot_mm_provis_recur"))
    + when(isnull(col("imp_tot_mm_otros_result")), 0).otherwise(col("imp_tot_mm_otros_result")))
.withColumn("imp_tot_pa_result_op_prev", when(isnull(col("imp_tot_pa_result_op_ppal")), 0).otherwise(col("imp_tot_pa_result_op_ppal"))
     + when(isnull(col("imp_tot_mm_result_otras_soc")), 0).otherwise(col("imp_tot_mm_result_otras_soc")))
    .withColumn("imp_tot_pa_result_op_AJUSTE", when(col("id_escenario") === 2, col("imp_tot_pa_result_op_prev") - col("imp_ebit_css_recurrente")).otherwise(0))
     .withColumn("imp_tot_pa_result_op", when(isnull(col("imp_ebit_css_recurrente")), col("imp_tot_pa_result_op_prev")).otherwise(col("imp_tot_pa_result_op_prev") - col("imp_tot_pa_result_op_AJUSTE")))
     .drop("imp_tot_pa_result_op_prev")
.withColumn("imp_tot_pa_result_net_ajust", when(isnull(col("imp_tot_pa_result_op")), 0).otherwise(col("imp_tot_pa_result_op"))
    + when(isnull(col("imp_2049_imp_minoritarios")), 0).otherwise(col("imp_2049_imp_minoritarios"))
    + when(isnull(col("imp_2049_imp_participadas")), 0).otherwise(col("imp_2049_imp_participadas"))
    - when(isnull(col("imp_2049_imp_impuestos")), 0).otherwise(col("imp_2049_imp_impuestos")))
.withColumn("imp_tot_pa_result_net", when(isnull(col("imp_tot_pa_result_net_ajust")), 0).otherwise(col("imp_tot_pa_result_net_ajust"))
    + when(isnull(col("imp_bfc_resultado_esp_adi")), 0).otherwise(col("imp_bfc_resultado_esp_adi"))
    + when(isnull(col("imp_2049_imp_efecto_patri_adi")), 0).otherwise(col("imp_2049_imp_efecto_patri_adi")))
.withColumn("imp_2049_imp_minoritarios_part", when(isnull(col("imp_2049_imp_participadas")), 0).otherwise(col("imp_2049_imp_participadas"))
    + when(isnull(col("imp_2049_imp_minoritarios")), 0).otherwise(col("imp_2049_imp_minoritarios")))
.withColumn("imp_ees_mayor_real_tot_cf_c", when(isnull(col("imp_ees_mayor_pers")), 0).otherwise(col("imp_ees_mayor_pers"))
    + when(isnull(col("imp_ees_mayor_serv_ext")), 0).otherwise(col("imp_ees_mayor_serv_ext"))
    + when(isnull(col("imp_ees_mayor_serv_corp")), 0).otherwise(col("imp_ees_mayor_serv_corp"))
    + when(isnull(col("imp_ees_mayor_serv_transv_DG")), 0).otherwise(col("imp_ees_mayor_serv_transv_DG"))
    + when(isnull(col("imp_ees_mayor_otros_serv_DG")), 0).otherwise(col("imp_ees_mayor_otros_serv_DG")))
.withColumn("imp_ees_mayor_real_tot_cf_ana", when(isnull(col("imp_ees_mayor_cf_ees")), 0).otherwise(col("imp_ees_mayor_cf_ees"))
    + when(isnull(col("imp_ees_mayor_otros_gastos")), 0).otherwise(col("imp_ees_mayor_otros_gastos")))
.withColumn("imp_ees_mayor_real_resu_op_ppl", when(isnull(col("imp_ees_mayor_mc_ees")), 0).otherwise(col("imp_ees_mayor_mc_ees"))
    - when(isnull(col("imp_ees_mayor_real_tot_cf_ana")), 0).otherwise(col("imp_ees_mayor_real_tot_cf_ana"))
    - when(isnull(col("imp_ees_mayor_amort")), 0).otherwise(col("imp_ees_mayor_amort"))
    - when(isnull(col("imp_ees_mayor_provis_recur")), 0).otherwise(col("imp_ees_mayor_provis_recur"))
    + when(isnull(col("imp_ees_mayor_otros_result")), 0).otherwise(col("imp_ees_mayor_otros_result")))
.withColumn("imp_ees_mayor_real_result_op", when(isnull(col("imp_ees_mayor_real_resu_op_ppl")), 0).otherwise(col("imp_ees_mayor_real_resu_op_ppl"))
    + when(isnull(col("imp_ees_mayor_result_otras_soc")), 0).otherwise(col("imp_ees_mayor_result_otras_soc")))
.withColumn("imp_ees_mayor_pa_tot_cf_c", when(isnull(col("imp_ees_mayor_pers")), 0).otherwise(col("imp_ees_mayor_pers"))
    + when(isnull(col("imp_ees_mayor_serv_ext")), 0).otherwise(col("imp_ees_mayor_serv_ext"))
    + when(isnull(col("imp_ees_mayor_serv_corp")), 0).otherwise(col("imp_ees_mayor_serv_corp"))
    + when(isnull(col("imp_ees_mayor_serv_transv_DG")), 0).otherwise(col("imp_ees_mayor_serv_transv_DG"))
    + when(isnull(col("imp_ees_mayor_otros_serv_DG")), 0).otherwise(col("imp_ees_mayor_otros_serv_DG")))
.withColumn("imp_ees_mayor_pa_tot_cf_ana", when(isnull(col("imp_ees_mayor_cf_ees")), 0).otherwise(col("imp_ees_mayor_cf_ees"))
    + when(isnull(col("imp_ees_mayor_otros_gastos")), 0).otherwise(col("imp_ees_mayor_otros_gastos")))
.withColumn("imp_ees_mayor_pa_result_op_ppl", when(isnull(col("imp_ees_mayor_mc_ees")), 0).otherwise(col("imp_ees_mayor_mc_ees"))
    - when(isnull(col("imp_ees_mayor_pa_tot_cf_ana")), 0).otherwise(col("imp_ees_mayor_pa_tot_cf_ana"))
    - when(isnull(col("imp_ees_mayor_amort")), 0).otherwise(col("imp_ees_mayor_amort"))
    - when(isnull(col("imp_ees_mayor_provis_recur")), 0).otherwise(col("imp_ees_mayor_provis_recur"))
    + when(isnull(col("imp_ees_mayor_otros_result")), 0).otherwise(col("imp_ees_mayor_otros_result")))
.withColumn("imp_ees_mayor_pa_result_op", when(isnull(col("imp_ees_mayor_pa_result_op_ppl")), 0).otherwise(col("imp_ees_mayor_pa_result_op_ppl"))
    + when(isnull(col("imp_ees_mayor_result_otras_soc")), 0).otherwise(col("imp_ees_mayor_result_otras_soc"))).cache

DFfinal.limit(1).count()


# In[29]:


spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

val ds_output = s"cli0010_tb_aux_m_pivkpi_mmex/"
val ds_output_temp = ds_output.concat("temp")
val pathWriteTemp = s"$edwPath/$business/$ds_output_temp"
val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

DFfinal.coalesce(1).write.partitionBy("cod_periodo").mode("overwrite").format("parquet").save(parquet_path_temp)


# In[30]:


// DFfinal.
//     write.
//     format("com.microsoft.sqlserver.jdbc.spark").
//     mode("overwrite"). //append
//     option("url", url).
//     option("dbtable", s"sch_anl.cli0010_tb_aux_m_pivkpi_mmex").
//     option("mssqlIsolationLevel", "READ_UNCOMMITTED").
//     option("truncate", "true").
//     option("tableLock","false").
//     option("reliabilityLevel","BEST_EFFORT").
//     option("numPartitions","1").
//     option("batchsize","1000000").
//     option("accessToken", token).
//     save()


#!/usr/bin/env python
# coding: utf-8

# ## cli0010_nb_prc_anl_t_mmex_manual_calc_H2



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

def look_column_else_zero0(tabla: DataFrame, columna: String): DataFrame = {
    if (tabla.columns.map(_.toUpperCase).contains(columna.toUpperCase)) {
        return tabla
    } else {
        return tabla.withColumn(columna, lit(0).cast(DecimalType(24,10)))
    }
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


// In[50]:


//Paths del LakeHouse
val lakehousePath = "CLI0010/trn"
val edwPath = "CLI0010/edw"
val container_output = "lakehouse"

val linked_service_name = "DL_COM"
val my_container = "processed"
val cont_lakehouse = "lakehouse"
val my_account = conexion("Endpoint").toString.substring(8)


// In[51]:


val business="AM_COM_Vista_Cliente"
val ds_output = s"cli0010_tb_fac_m_mod_mmex_icv/"
val pathWriteTemp = s"$edwPath/$business/$ds_output"
val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

var t_Pool_fac_SQLPOOL = spark.read.parquet(parquet_path_temp)


// In[52]:


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


// In[53]:


var t_Pool_kpi = readFromSQLPool("sch_anl","cli0010_tb_dim_m_kpi_mmex_h2", token).cache

var t_Pool_fac = t_Pool_fac_SQLPOOL.dropDuplicates("num_periodo", "id_escenario"
    , "val_unidadnegocio", "val_origen", "val_producto", "val_pais", "val_sociedad", "val_unidadmedida", "val_canal", 
    "id_kpi", "val_kpi", "cod_kpi", "num_periodo_mensacum", "val_flag_calculado")


// In[54]:


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

//display(df_resultados_generados2.where( col("des_kpi").like("%Mon%"))


// In[55]:


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
,"Central Movilidad Mexico_Servicios externos##
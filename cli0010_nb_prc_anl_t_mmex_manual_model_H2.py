#!/usr/bin/env python
# coding: utf-8

# ## cli0010_nb_prc_anl_t_mmex_manual_model_H2
# 
# 
# 

# In[1]:


val v_year = "2023"
val v_month  = "01"
val escenario = "REAL" // {ALL / PA-UPA / EST / REAL / PA / UPA}
val applicationName: String = ""
val parentUid:String = "N/A"
val uuid:String = "N/A"


# In[2]:


get_ipython().run_line_magic('run', 'cli0010/util/cli0010_nb_prc_app_obj_module_properties')


# In[3]:


get_ipython().run_line_magic('run', 'cli0010/util/cli0010_nb_prc_app_obj_module_library')


# In[4]:


import org.apache.spark.sql.expressions.Window

def mergeMap(map:Map[String,String],newMap:Map[String,String]):Map[String,String]={
    var retorno=map 
    newMap.foreach(
        nuevo=>{
            var map_match = retorno.get(nuevo._1);
            if( map_match == None || nuevo._2 > map_match.get){
                
                retorno = retorno + nuevo
            }
        }
    ) 
    return retorno
}

def findSubDirectoriesLike (path: String,partitionName:String,partitionValueMin:String): Map[String,String] = {
    var retorno: Map[String,String]=Map()
    try{
        mssparkutils.fs.ls(path).foreach( 
            file =>{
            if (file.isDir){
                /* Si encontramos en el path la particion*/
                if (file.name contains partitionName){
                    var partitionValue= file.name.split("=")(1)
                    /*en caso de que sea copuesto xxxx-nnnnn, nos quedamos con la segunda parte*/
                    if (partitionValue contains "-"){
                        partitionValue=partitionValue.split("-")(1)
                    }
                    if (partitionValueMin == partitionValue){
                        val fullpath=path+"/"+file.name                        
                        var new_path =Map(partitionValue->fullpath);
                        retorno= retorno ++ new_path
                    }
                }
                else{
                    var recursivo:Map[String,String]= findSubDirectoriesLike(path+"/"+file.name,partitionName,partitionValueMin);
                    retorno= mergeMap(retorno,recursivo);
                }
            }
        }
        )
        return retorno
    } 
    catch {
        case ex: Throwable =>{
            println(ex);
            return retorno
        }
    } 
}


def f_delta_dataframe(tabla_origen:DataFrame, tabla_destino_pool:DataFrame, id:String, campo:String) : DataFrame = 
{
    if ( tabla_destino_pool.isEmpty ) 
    { 
        val windowSpec  = Window.orderBy(campo)
        return tabla_origen.withColumn(id,row_number.over(windowSpec))
    }
    else 
    {
        var col_valorMax = tabla_destino_pool.select(max(id))

        var max_val = col_valorMax.collectAsList()
        var i = max_val.get(0).toString().replace("[","").replace("]","").trim.toInt

        var n = tabla_origen.join(tabla_destino_pool, trim(tabla_origen(campo)) === trim(tabla_destino_pool(campo)), "left").
            where(tabla_destino_pool(campo).isNull).select(tabla_origen(campo))

        val windowSpec  = Window.orderBy(campo)
        val o = n.withColumn(id,row_number.over(windowSpec)+i)
        return o
    }
}

def look_column_else_zero( tabla : DataFrame , columna : String) : DataFrame =
{
    if ( tabla.columns.map(_.toUpperCase).contains(columna.toUpperCase) ) {
     return tabla }
    else  {
        return tabla.withColumn(columna, lit(0).cast(DoubleType)) }
}


# In[5]:


//Paths del LakeHouse
val lakehousePath = "CLI0010/trn"
val edwPath = "CLI0010/edw"
val container_output = "lakehouse"


# In[6]:


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

val linked_service_name = "DL_COM"
val my_container = "processed"
val cont_lakehouse = "lakehouse"
val my_account = conexion("Endpoint").toString.substring(8)


# In[7]:


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import java.io.IOException
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

var aperiodo = v_year.concat(v_month)
var periodo = v_year.concat(v_month).toInt


# In[8]:


val business="AM_COM_Vista_Cliente"

// Path raiz de la ruta del maestro de real de cuenta resultados del gen2
val my_account = conexion("Endpoint").toString.substring(8)


# In[9]:


val maestro_manual_est_cr_path = s"abfss://$my_container@$my_account/$business/vc_manual_mm_est_cr"

val maestro_manual_est_cr = findSubDirectoriesLike(maestro_manual_est_cr_path,"ingestion_data",aperiodo).get(aperiodo).getOrElse(maestro_manual_est_cr_path)


# In[10]:


val maestro_manual_pa_cr_path = s"abfss://$my_container@$my_account/$business/vc_manual_mm_pa_cr"

val maestro_manual_pa_cr = findSubDirectoriesLike(maestro_manual_pa_cr_path,"ingestion_data",aperiodo).get(aperiodo).getOrElse(maestro_manual_pa_cr_path)


# In[11]:


val maestro_manual_re_cr_path = s"abfss://$my_container@$my_account/$business/vc_manual_mm_re_cr"

val maestro_manual_re_cr = findSubDirectoriesLike(maestro_manual_re_cr_path,"ingestion_data",aperiodo).get(aperiodo).getOrElse(maestro_manual_re_cr_path)


# **Se crean los DataFrames correspondientes a los maestros de gen2**

# In[12]:


var total3 = spark.emptyDataFrame
if (escenario == "ALL"){
    val df_ret_est_cr = spark.read.parquet(maestro_manual_est_cr).cache
    val df_ret_pa_cr = spark.read.parquet(maestro_manual_pa_cr).cache
    val df_ret_re_cr = spark.read.parquet(maestro_manual_re_cr).cache

    var mm_re_cr_prt = df_ret_re_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida", "periodo_mensacum")
    var mm_est_cr_prt = df_ret_est_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida").withColumn("periodo_mensacum", lit("undefined"))
    var mm_pa_cr_prt = df_ret_pa_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida", "periodo_mensacum")

    total3 = mm_re_cr_prt.unionByName(mm_pa_cr_prt,true).unionByName(mm_est_cr_prt,true).where(col("periodo") === aperiodo).cache
}else if (escenario == "REAL"){
    val df_ret_re_cr = spark.read.parquet(maestro_manual_re_cr).cache

    total3 = df_ret_re_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida", "periodo_mensacum")

}else if (escenario == "PA-UPA" || escenario =="PA" || escenario == "UPA"){
    val df_ret_pa_cr = spark.read.parquet(maestro_manual_pa_cr).cache

    total3 = df_ret_pa_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida", "periodo_mensacum")
}else if (escenario == "EST"){
    val df_ret_est_cr = spark.read.parquet(maestro_manual_est_cr).cache
    
    total3 = df_ret_est_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida").withColumn("periodo_mensacum", lit("undefined"))
}


# In[13]:


var totalRename = total3.withColumnRenamed("unidadnegocio","unidadnegocio2")


# In[14]:


var total2 = totalRename.select(col("escenario"),
    col("periodo"),
    col("unidadnegocio2"),
    col("sociedad"),
    col("pais"), 
    col("producto"),
    col("origen"), 
    col("canal"),
    col("proceso"),
    col("nombrekpi"),
    col("kpi"),
    col("valorkpi"),
    col("unidadmedida"),
    col("periodo_mensacum")).withColumn("unidadnegocio", regexp_replace($"unidadnegocio2", ",", ".")).drop("unidadnegocio2")


# In[15]:


def f_delta_dataframe(tabla_origen:DataFrame, tabla_destino_pool:DataFrame, id:String, campo:String) : DataFrame = 
{
    if ( tabla_destino_pool.isEmpty ) 
    { 
        val windowSpec  = Window.orderBy(campo)
        return tabla_origen.withColumn(id,row_number.over(windowSpec))
    }
    else 
    {
        var col_valorMax = tabla_destino_pool.select(max(id))

        var max_val = col_valorMax.collectAsList()
        var i = max_val.get(0).toString().replace("[","").replace("]","").trim.toInt

        var n = tabla_origen.join(tabla_destino_pool, trim(tabla_origen(campo)) === trim(tabla_destino_pool(campo)), "left").
            where(tabla_destino_pool(campo).isNull).select(tabla_origen(campo))

        val windowSpec  = Window.orderBy(campo)
        val o = n.withColumn(id,row_number.over(windowSpec)+i)
        return o
    }
}


# In[16]:


var t_escenario = total2.select(col("escenario").as("nom_escenario")).distinct().where(col("escenario").isNotNull)
var total = total2.withColumn("des_kpi", concat(
            col("unidadnegocio"),lit("_"),
            col("nombrekpi"),
            (when(isnull(col("producto")), "").otherwise(col("producto")))).as("des_kpi")).dropDuplicates.where(col("nombrekpi").isNotNull)

var t_kpi = total.select(col("des_kpi").as("des_kpi")).distinct().where(col("des_kpi").isNotNull)


# In[17]:


var t_Pool_kpi = readFromSQLPool("sch_anl","cli0010_tb_dim_m_kpi_mmex_h2", token)


# In[18]:


def makeAllColumnsNullable(inputDF: DataFrame): DataFrame = {
    val schema = inputDF.schema.map { field =>
      StructField(field.name, field.dataType, nullable = true)
    }

    val newSchema = StructType(schema)
    inputDF.sqlContext.createDataFrame(inputDF.rdd, newSchema)
  }


# In[19]:


var kpi = f_delta_dataframe(t_kpi, t_Pool_kpi, "id_kpi", "des_kpi")

kpi = makeAllColumnsNullable(kpi)


# In[20]:


kpi.
    write. 
    format("com.microsoft.sqlserver.jdbc.spark").
    mode("append"). //append
    option("url", url). 
    option("dbtable", s"sch_anl.cli0010_tb_dim_m_kpi_mmex_h2").
    option("mssqlIsolationLevel", "READ_UNCOMMITTED").
    option("truncate", "true").
    option("tableLock","false").
    option("reliabilityLevel","BEST_EFFORT").
    option("numPartitions","1").
    option("batchsize","1000000").
    option("accessToken", token).
    save()


# Comienza el cambio de nomenclaturas

# In[21]:


var join_kpi = total2.join(t_Pool_kpi, concat(col("unidadnegocio"),lit("_"),col("nombrekpi"),(when(isnull(col("producto")), "").otherwise(col("producto")))) === t_Pool_kpi("des_kpi"), "left")

var T_escritura = join_kpi.select(
    lit(1).as("id_registro"),
    col("periodo").cast(IntegerType).as("num_periodo"),
    when(col("Escenario") === "RE", 1).when(col("Escenario") === "PA", 2).when(col("Escenario") === "UPA", 3).otherwise(4).as("id_escenario"),
    col("unidadnegocio").as("val_unidadnegocio"),
    col("producto").as("val_producto"),
    col("origen").as("val_origen"),
    // when( col("proceso") === "Costes Fijos", 1) 
        // .when( col("proceso") === "Márgenes, Ventas y Costes Variables", 2)
        // .when( col("proceso") === "Cuenta de resultados", 3)
        // .when( col("proceso") === "Indicadores operativos", 4)
        // .when( col("proceso") === "Resultados generados", 5)
        // .otherwise(6).as("val_proceso"),
    col("id_kpi").cast(IntegerType).as("id_kpi"),
    col("pais").as("val_pais"),
    col("canal").as("val_canal"),
    col("kpi").as("cod_kpi"),
    col("unidadmedida").as("val_unidadmedida"),
    col("periodo_mensacum").as("num_periodo_mensacum"),
    col("sociedad").as("val_sociedad"),
    col("valorkpi").cast(DecimalType(24,10)).as("val_kpi"), //estaba así
    lit(0).as("val_flag_calculado")
    ).where(col("num_periodo") === aperiodo).dropDuplicates.cache


# In[25]:


// display(T_escritura)


# In[33]:


spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
val ds_output = s"cli0010_tb_fac_m_mod_mmex_icv/"

val ds_output_temp = ds_output
val pathWriteTemp = s"$edwPath/$business/$ds_output_temp"
val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

// dfreal.coalesce(1).write.partitionBy("cod_fecha").mode("overwrite").format("parquet").save(parquet_path)
T_escritura.coalesce(1).write.partitionBy("id_escenario","num_periodo").mode("overwrite").format("parquet").save(parquet_path_temp)
T_escritura.count


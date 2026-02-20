#!/usr/bin/env python
# coding: utf-8

# ## cli0010_nb_prc_anl_t_mmex_manual_model
# 
# 
# 

# In[46]:


val v_year = "2023"
val v_month  = "01"
val escenario = "PA" // {ALL / PA-UPA / EST / REAL / PA / UPA}
val applicationName: String = ""
val parentUid:String = "N/A"
val uuid:String = "N/A"


# In[47]:


get_ipython().run_line_magic('run', 'cli0010/util/cli0010_nb_prc_app_obj_module_properties')


# In[48]:


get_ipython().run_line_magic('run', 'cli0010/util/cli0010_nb_prc_app_obj_module_library')


# In[49]:


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

// 
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
            where(tabla_destino_pool(campo).isNull).select(tabla_origen(campo)).cache

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


# In[50]:


//Paths del LakeHouse
val lakehousePath = "CLI0010/trn"
val edwPath = "CLI0010/edw"
val container_output = "lakehouse"


# In[51]:


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


# In[52]:


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import java.io.IOException
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

var aperiodo = v_year.concat(v_month)
var periodo = v_year.concat(v_month).toInt


# In[53]:


val business="AM_COM_Vista_Cliente"

// Path raiz de la ruta del maestro de estimado de cuenta resultados del gen2
val my_account = conexion("Endpoint").toString.substring(8)

val maestro_manual_est_cr_path = s"abfss://$my_container@$my_account/$business/vc_manual_mm_est_cr"

val maestro_manual_est_cr = findSubDirectoriesLike(maestro_manual_est_cr_path,"ingestion_data",aperiodo).get(aperiodo).getOrElse(maestro_manual_est_cr_path)


# In[54]:


val business="AM_COM_Vista_Cliente"

// Path raiz de la ruta del maestro de presupuesto y upa de cuenta resultados del gen2
val my_account = conexion("Endpoint").toString.substring(8)

val maestro_manual_pa_cr_path = s"abfss://$my_container@$my_account/$business/vc_manual_mm_pa_cr"

val maestro_manual_pa_cr = findSubDirectoriesLike(maestro_manual_pa_cr_path,"ingestion_data",aperiodo).get(aperiodo).getOrElse(maestro_manual_pa_cr_path)


# In[55]:


val business="AM_COM_Vista_Cliente"

// Path raiz de la ruta del maestro de real de cuenta resultados del gen2
val my_account = conexion("Endpoint").toString.substring(8)

val maestro_manual_re_cr_path = s"abfss://$my_container@$my_account/$business/vc_manual_mm_re_cr"

val maestro_manual_re_cr = findSubDirectoriesLike(maestro_manual_re_cr_path,"ingestion_data",aperiodo).get(aperiodo).getOrElse(maestro_manual_re_cr_path)


# **Se crean los DataFrames correspondientes a los maestros de gen2**

# In[56]:


var df_ret_est_cr = spark.emptyDataFrame
if (escenario == "ALL" || escenario == "EST"){
   df_ret_est_cr = spark.read.parquet(maestro_manual_est_cr).cache

   var mm_est_cr = df_ret_est_cr.select(
      col("escenario").cast(VarcharType(30)).as("cod_escenario"),
      col("periodo").cast(IntegerType).as("num_periodo"),
      col("unidadnegocio").cast(VarcharType(150)).as("val_unidadnegocio"),
      col("sociedad").cast(VarcharType(150)).as("val_sociedad"),
      col("pais").cast(VarcharType(150)).as("val_pais"),
      col("producto").cast(VarcharType(150)).as("val_producto"),
      col("origen").cast(VarcharType(150)).as("val_origen"),
      col("canal").cast(VarcharType(150)).as("val_canal"),
      col("proceso").cast(VarcharType(150)).as("val_proceso"),
      col("nombrekpi").cast(VarcharType(150)).as("nom_nombrekpi"),
      col("kpi").cast(VarcharType(150)).as("cod_kpi"),
      col("valorkpi").cast(DecimalType(24,10)).as("val_kpi"),
      col("unidadmedida").cast(VarcharType(50)).as("val_unidadmedida"),
      translate(col("ARiATimestampLoad"), "T"," ").cast(VarcharType(50)).as("tst_system_date") // Quitamos el caracter 'T' parapoder convertirlo a timestamp
   ).cache
   if (maestro_manual_est_cr contains "ingestion_data"){
      mm_est_cr
         .write.
            format("com.microsoft.sqlserver.jdbc.spark").
            mode("overwrite"). //appens
            option("url", url).
            option("dbtable", s"sch_anl.cli0010_tb_dim_m_mn_st_cr_mmex").
            option("mssqlIsolationLevel", "READ_UNCOMMITTED").
            option("truncate", "true").
            option("tableLock","false").
            option("reliabilityLevel","BEST_EFFORT").
            option("numPartitions","1").
            option("batchsize","1000000").
            option("accessToken", token).
            save()
   }
}


# In[57]:


var df_ret_pa_cr = spark.emptyDataFrame
if (escenario == "ALL" || escenario == "PA-UPA" || escenario == "PA" || escenario == "UPA"){
   df_ret_pa_cr = spark.read.parquet(maestro_manual_pa_cr).cache

   var mm_pa_cr = df_ret_pa_cr.select(
      col("escenario").cast(VarcharType(30)).as("cod_escenario"),
      col("periodo").cast(IntegerType).as("num_periodo"),
      col("periodo_mensacum").cast(VarcharType(30)).as("ind_periodo_mensacum"),
      col("version").cast(VarcharType(30)).as("val_version"),
      col("unidadnegocio").cast(VarcharType(150)).as("val_unidadnegocio"),
      col("sociedad").cast(VarcharType(150)).as("val_sociedad"),
      col("pais").cast(VarcharType(150)).as("val_pais"),
      col("producto").cast(VarcharType(150)).as("val_producto"),
      col("origen").cast(VarcharType(150)).as("val_origen"),
      col("canal").cast(VarcharType(150)).as("val_canal"),
      col("proceso").cast(VarcharType(150)).as("val_proceso"),
      col("nombrekpi").cast(VarcharType(150)).as("nom_nombrekpi"),
      col("kpi").cast(VarcharType(150)).as("cod_kpi"),
      col("valorkpi").cast(DecimalType(24,10)).as("val_kpi"),
      col("unidadmedida").cast(VarcharType(50)).as("val_unidadmedida"),
      translate(col("ARiATimestampLoad"), "T"," ").cast(VarcharType(50)).as("tst_system_date") // Quitamos el caracter 'T' parapoder convertirlo a timestamp
   ).cache
   if (maestro_manual_pa_cr contains "ingestion_data"){
      mm_pa_cr
      .write.
         format("com.microsoft.sqlserver.jdbc.spark").
         mode("overwrite"). //appens
         option("url", url).
         option("dbtable", s"sch_anl.cli0010_tb_dim_m_mn_pa_cr_mmex").
         option("mssqlIsolationLevel", "READ_UNCOMMITTED").
         option("truncate", "true").
         option("tableLock","false").
         option("reliabilityLevel","BEST_EFFORT").
         option("numPartitions","1").
         option("batchsize","1000000").
         option("accessToken", token).
         save()
   }
}


# In[58]:


var df_ret_re_cr = spark.emptyDataFrame
if (escenario == "ALL" || escenario == "REAL"){   
   df_ret_re_cr = spark.read.parquet(maestro_manual_re_cr)

   var mm_re_cr = df_ret_re_cr.select(
      col("escenario").cast(VarcharType(30)).as("cod_escenario"),
      col("periodo").cast(IntegerType).as("num_periodo"),
      col("periodo_mensacum").cast(VarcharType(30)).as("ind_periodo_mensacum"),
      col("unidadnegocio").cast(VarcharType(150)).as("val_unidadnegocio"),
      col("sociedad").cast(VarcharType(150)).as("val_sociedad"),
      col("pais").cast(VarcharType(150)).as("val_pais"),
      col("producto").cast(VarcharType(150)).as("val_producto"),
      col("origen").cast(VarcharType(150)).as("val_origen"),
      col("canal").cast(VarcharType(150)).as("val_canal"),
      col("proceso").cast(VarcharType(150)).as("val_proceso"),
      col("nombrekpi").cast(VarcharType(150)).as("nom_nombrekpi"),
      col("kpi").cast(VarcharType(150)).as("cod_kpi"),
      col("valorkpi").cast(DecimalType(24,10)).as("val_kpi"),
      col("unidadmedida").cast(VarcharType(50)).as("val_unidadmedida"),
      translate(col("ARiATimestampLoad"), "T"," ").cast(VarcharType(50)).as("tst_system_date") // Quitamos el caracter 'T' parapoder convertirlo a timestamp
   ).cache
   if (maestro_manual_re_cr contains "ingestion_data"){
      mm_re_cr
         .write.
            format("com.microsoft.sqlserver.jdbc.spark").
            mode("overwrite"). //appens
            option("url", url).
            option("dbtable", s"sch_anl.cli0010_tb_dim_m_mn_re_cr_mmex").
            option("mssqlIsolationLevel", "READ_UNCOMMITTED").
            option("truncate", "true").
            option("tableLock","false").
            option("reliabilityLevel","BEST_EFFORT").
            option("numPartitions","1").
            option("batchsize","1000000").
            option("accessToken", token).
            save()
   }
}


# In[59]:


var total3 = spark.emptyDataFrame
if (escenario == "EST"){   
    total3 = df_ret_est_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida").withColumn("periodo_mensacum", lit("undefined"))
    .where(col("periodo") === aperiodo).cache
}else if (escenario == "PA-UPA" || escenario =="PA" || escenario == "UPA"){
    total3 = df_ret_pa_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida", "periodo_mensacum")
    .where(col("periodo") === aperiodo).cache
}else if (escenario == "REAL"){
    total3 = df_ret_re_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida", "periodo_mensacum")
    .where(col("periodo") === aperiodo).cache
}else{
    var mm_re_cr_prt = df_ret_re_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida", "periodo_mensacum")
    var mm_est_cr_prt = df_ret_est_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida").withColumn("periodo_mensacum", lit("undefined"))
    var mm_pa_cr_prt = df_ret_pa_cr.select("escenario", "periodo", "unidadnegocio", "sociedad", "pais", "producto", "origen", "canal", "proceso", "nombrekpi","kpi" , "valorkpi", "unidadmedida", "periodo_mensacum")
    total3 = mm_re_cr_prt.unionByName(mm_pa_cr_prt,true).unionByName(mm_est_cr_prt,true).where(col("periodo") === aperiodo).cache
}


# In[60]:


var totalRename = total3.withColumnRenamed("unidadnegocio","unidadnegocio2")


# In[61]:


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


# In[62]:


var t_escenario = total2.select(col("escenario").as("nom_escenario")).distinct().where(col("escenario").isNotNull)
var total = total2.withColumn("des_kpi", concat(
            col("unidadnegocio"),lit("_"),
            col("nombrekpi"),
            (when(isnull(col("producto")), "").otherwise(col("producto")))).as("des_kpi")).dropDuplicates.where(col("nombrekpi").isNotNull).cache

var t_kpi = total.select(col("des_kpi").as("des_kpi")).distinct().where(col("des_kpi").isNotNull).cache

total.limit(1).count()


# In[63]:


var t_Pool_kpi = readFromSQLPool("sch_anl","cli0010_tb_dim_m_kpi_mmex", token).cache


# In[64]:


def makeAllColumnsNullable(inputDF: DataFrame): DataFrame = {
    val schema = inputDF.schema.map { field =>
      StructField(field.name, field.dataType, nullable = true)
    }

    val newSchema = StructType(schema)
    inputDF.sqlContext.createDataFrame(inputDF.rdd, newSchema)
  }


# In[65]:


var kpi = f_delta_dataframe(t_kpi, t_Pool_kpi, "id_kpi", "des_kpi")

kpi = makeAllColumnsNullable(kpi)

kpi.
    write. 
    format("com.microsoft.sqlserver.jdbc.spark").
    mode("append"). //append
    option("url", url). 
    option("dbtable", s"sch_anl.cli0010_tb_dim_m_kpi_mmex").
    option("mssqlIsolationLevel", "READ_UNCOMMITTED").
    option("truncate", "true").
    option("tableLock","false").
    option("reliabilityLevel","BEST_EFFORT").
    option("numPartitions","1").
    option("batchsize","1000000").
    option("accessToken", token).
    save()


# In[66]:


var a_periodo = total.withColumn("anio", substring(total("periodo"),0,4)).withColumn("mes", substring(total("periodo"),5,2)).select("periodo","anio","mes")
//var periodo = a_periodo.withColumn("mes_anio", concat(a_periodo("mes"), a_periodo("anio")))


# Comienza el cambio de nomenclaturas

# In[67]:


var join_kpi = total2.join(t_Pool_kpi, concat(col("unidadnegocio"),lit("_"),col("nombrekpi"),(when(isnull(col("producto")), "").otherwise(col("producto")))) === t_Pool_kpi("des_kpi"), "left").cache

var T_escritura = join_kpi.select(
    lit(1).as("id_registro"),
    col("periodo").cast(IntegerType).as("num_periodo"),
    when(col("Escenario") === "RE", 3).when(col("Escenario") === "PA", 2).when(col("Escenario") === "UPA", 4).otherwise(1).as("id_escenario"),
    col("unidadnegocio").as("val_unidadnegocio"),
    col("producto").as("val_producto"),
    col("origen").as("val_origen"),
    when( col("proceso") === "Costes Fijos", 1) 
        .when( col("proceso") === "Márgenes, Ventas y Costes Variables", 2)
        .when( col("proceso") === "Cuenta de resultados", 3)
        .when( col("proceso") === "Indicadores operativos", 4)
        .when( col("proceso") === "Resultados generados", 5)
        .otherwise(6).as("val_proceso"),
    col("id_kpi").cast(IntegerType).as("id_kpi"),
    col("pais").as("val_pais"),
    col("canal").as("val_canal"),
    col("kpi").as("cod_kpi"),
    col("unidadmedida").as("val_unidadmedida"),
    col("periodo_mensacum").as("num_periodo_mensacum"),
    col("sociedad").as("val_sociedad"),
    col("valorkpi").cast(DecimalType(16,2)).as("val_kpi"), //estaba así
    lit(0).as("val_flag_calculado")
    ).where(col("num_periodo") === aperiodo).dropDuplicates


# In[68]:


var t_Final_Escritura = T_escritura.select(col("id_registro"), 
    col("num_periodo").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    col("val_producto"),
    col("val_origen"),
    col("val_proceso"),
    col("id_kpi").cast(IntegerType),
    col("val_pais"),
    col("val_canal"),
    col("cod_kpi"),
    col("val_unidadmedida"),
    col("num_periodo_mensacum"),
    col("val_sociedad"),
    col("val_unidadnegocio"),
    col("val_kpi").cast(DecimalType(24,10)),
    col("val_flag_calculado")
).dropDuplicates("num_periodo", "id_escenario", "val_unidadnegocio", "val_pais", "val_proceso", "id_kpi", "val_unidadmedida", "val_kpi").cache

t_Final_Escritura.limit(1).count()


# In[69]:


spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
val ds_output = s"cli0010_tb_fac_m_mod_mmex/"

val ds_output_temp = ds_output.concat("temp")
val pathWriteTemp = s"$edwPath/$business/$ds_output_temp"
val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

t_Final_Escritura.coalesce(1).write.partitionBy("num_periodo").mode("overwrite").format("parquet").save(parquet_path_temp)
t_Final_Escritura.limit(1).count()


# Acá  terminan los cambios del modelo

# In[70]:


def look_columns_else_null(tabla: DataFrame, columns: List[String]): DataFrame = {
  columns.foldLeft(tabla) { (df, column) =>
    if (df.columns.map(_.toUpperCase).contains(column.toUpperCase)) {
      df
    } else {
      df.withColumn(column, lit(null).cast(DecimalType(24, 10)))
    }
  }
}

val columnsToEnsure = List(
"2049_imp_impuestos",
"2049_imp_juridico",
"2049_imp_minoritarios",
"2049_imp_otros",
"2049_imp_pyo",
"2049_imp_resultado_especifico_adi",
"2049_imp_ti",
"2049_imp_ti_as_a_service",
"2049_imp_compras",
"2049_imp_econom_admin",
"2049_imp_financiero",
"2049_imp_fiscal",
"2049_imp_servicios_globales",
"2049_imp_efecto_patrimonial_adi",
"2049_imp_participadas",
"2049_imp_seguros",
"2049_imp_digitalizacion",
"2049_imp_sostenibilidad",
"2049_imp_auditoria",
"2049_imp_planif_control",
"2049_imp_patrimonial_seg",
"2049_imp_comunicacion",
"2049_imp_techlab",
"2049_imp_ingenieria",
"2049_imp_ser_corporativos")


# In[72]:


var bfcperiodo = aperiodo

var df_33_1 = spark.emptyDataFrame
if (!(escenario == "EST") && !(escenario == "UPA")){
    var bfc_path = s"abfss://$cont_lakehouse@$my_account/CLI0010/edw/EdD/tbl_fact_bfc_bpc_vc"
    var bfc_table = spark.read.parquet(bfc_path).cache
    
    // Se joinea dataset vacio con periodo por el caso de no tener info en BFC
    var df = Seq((bfcperiodo,1),(bfcperiodo,2)).toDF("periodo", "escenario")

    var t_bfc = bfc_table.where(
        col("id_periodo") === bfcperiodo &&
        (
            (
                (col("cod_kpi") === "imp_participadas" ||
                col("cod_kpi") === "imp_minoritarios" ||
                col("cod_kpi") === "imp_efecto_patrimonial_adi" ||
                col("cod_kpi") === "imp_resultado_especifico_adi" ||
                col("cod_kpi") === "imp_impuestos")
                && (col("id_businessunit") === 2049 || col("id_businessunit") === 2050)
            ) ||
            (
                (
                    col("cod_kpi") === "imp_patrimonial_seg" ||
                    col("cod_kpi") === "imp_pyo" ||
                    col("cod_kpi") === "imp_servicios_globales" ||
                    col("cod_kpi") === "imp_comunicacion" ||
                    col("cod_kpi") === "imp_techlab" ||
                    col("cod_kpi") === "imp_ti" ||
                    col("cod_kpi") === "imp_ti_as_a_service" ||
                    col("cod_kpi") === "imp_compras" ||
                    col("cod_kpi") === "imp_juridico" ||
                    col("cod_kpi") === "imp_financiero" ||
                    col("cod_kpi") === "imp_fiscal" ||
                    col("cod_kpi") === "imp_econom_admin" ||
                    col("cod_kpi") === "imp_ingenieria" ||
                    col("cod_kpi") === "imp_apoyo_gestion" ||
                    col("cod_kpi") === "imp_otros" ||
                    col("cod_kpi") === "imp_seguros" ||
                    col("cod_kpi") === "imp_digitalizacion" ||
                    col("cod_kpi") === "imp_sostenibilidad" ||
                    col("cod_kpi") === "imp_auditoria" ||
                    col("cod_kpi") === "imp_planif_control" ||
                    col("cod_kpi") === "imp_ser_corporativos"
                )
                && (
                    col("id_businessunit") === 2049 ||
                    col("id_businessunit") === 2050
                )
                && (
                    col("id_reportunit") === "0919" ||
                    col("id_reportunit") === "1657" ||
                    (col("id_escenario") === 2 && col("id_reportunit") === "TOTAL")
                )
            )
        ) &&
        col("id_tipodato") === 1
    ).withColumn("anio", lit(col("id_periodo").substr(0, 4)))
    .groupBy("id_periodo", "cod_kpi", "id_escenario", "id_businessunit", "des_kpi")
    .sum("val_valor")
    .withColumnRenamed("sum(val_valor)", "val_valor")

    var t_bfc_t = df.join(t_bfc, df("periodo") === t_bfc("id_periodo") && df("escenario") === t_bfc("id_escenario"), "left").cache

    var c = t_bfc_t.drop(col("id_escenario")).drop(col("id_periodo"))
    var b = c.withColumn("id_escenario_bfc", when(isnull(col("escenario")), 0).otherwise(col("escenario"))).drop(col("escenario"))

    var ConcatBfc = b.withColumn("des_kpi", concat(
                col("id_businessunit"),lit("_"),
                col("cod_kpi")))

    var df_1 = ConcatBfc.groupBy("periodo", "id_escenario_bfc").pivot("des_kpi").sum("val_valor")

    df_33_1 = look_columns_else_null(df_1, columnsToEnsure).cache
}


# **CÁLCULO EBIT PERIODO PESOS**

# In[73]:


import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, DoubleType, StringType}

var df_33_2 = spark.emptyDataFrame
if (!(escenario == "EST") && !(escenario == "UPA")){
    var bfc_path = s"abfss://$cont_lakehouse@$my_account/CLI0010/edw/EdD/tbl_fact_bfc_bpc_vc"
    var bfc_table = spark.read.parquet(bfc_path).cache

    var v_periodo_ytd = v_year.toString().concat("00").toInt

    var bfcperiodo = aperiodo

    var bfc_read_ebit = bfc_table.where(
            col("id_periodo") <= bfcperiodo && col("id_periodo") >= v_periodo_ytd
            &&
            (
                (
                    col("cod_kpi") === "imp_ebit_css_recurrente" && (col("id_businessunit") === 2049 || col("id_businessunit") === 2050)
                )
            ) 
            && col("id_tipodato") === 1
        ).withColumn("anio", lit(col("id_periodo").substr(0, 4)))
        .groupBy("id_periodo", "cod_kpi", "id_escenario", "id_businessunit", "des_kpi")
        .sum("val_valor")
        .withColumnRenamed("sum(val_valor)", "val_valor")

    var bfc_read_ebit_rename = bfc_read_ebit.withColumnRenamed("id_periodo", "periodo").withColumnRenamed("id_escenario", "escenario")
        .drop(col("id_escenario")).drop(col("id_periodo"))

    var b = bfc_read_ebit_rename.withColumn("id_escenario_bfc", when(isnull(col("escenario")), 0).otherwise(col("escenario"))).drop(col("escenario"))

    var ConcatBfc = b.withColumn("des_kpi", concat(
                col("id_businessunit"),lit("_"),
                col("cod_kpi"),lit("_pre")))

    var pivot = ConcatBfc.groupBy("periodo", "id_escenario_bfc").pivot("des_kpi").sum("val_valor")

    var df_ebit = look_column_else_zero(pivot, "2049_imp_ebit_css_recurrente_pre")

    // coger divisa

    val pre_cambio_div_pa = spark.read.parquet(maestro_manual_pa_cr_path)
        .where(col("periodo") <= bfcperiodo && col("periodo") >= v_periodo_ytd && col("kpi") ==="MMCCRREAL700")
        .select("periodo","valorkpi","data_load_part")
        .dropDuplicates.cache

    val pre_cambio_div_real = spark.read.parquet(maestro_manual_re_cr_path).cache
        .where(col("periodo") <= bfcperiodo && col("periodo") >= v_periodo_ytd && col("kpi") ==="MMCCRREAL700")
        .select("periodo","valorkpi","data_load_part")
        .dropDuplicates.cache

    val windowSpec = Window.orderBy(col("data_load_part").desc)
    
    var cambio_div_pa = spark.emptyDataFrame
    var cambio_div_real = spark.emptyDataFrame

    if (!pre_cambio_div_pa.isEmpty){
        val maxDataloadpart_pa = pre_cambio_div_pa.select(max("data_load_part")).first().getInt(0)

        cambio_div_pa = pre_cambio_div_pa.filter(col("data_load_part") === maxDataloadpart_pa && col("valorkpi") >= 0.1)
            .withColumnRenamed("periodo", "id_periodo")
            .withColumn("imp_cambio_divisa", bround(col("valorkpi"), 2)).drop("valorkpi")
            .drop("data_load_part")
            .withColumn("id_escenario", lit(2))
    }

    if (!pre_cambio_div_real.isEmpty){
        val maxDataloadpart_real = pre_cambio_div_real.select(max("data_load_part")).first().getInt(0)

        cambio_div_real = pre_cambio_div_real.filter(col("data_load_part") === maxDataloadpart_real && col("valorkpi") >= 0.1)
            .withColumnRenamed("periodo", "id_periodo")
            .withColumn("imp_cambio_divisa", bround(col("valorkpi"), 2)).drop("valorkpi")
            .drop("data_load_part")
            .withColumn("id_escenario", lit(1))
    }

    var cambio_div = spark.emptyDataFrame

    if ((!cambio_div_pa.isEmpty) && (!cambio_div_real.isEmpty)){
        cambio_div = cambio_div_pa.unionByName(cambio_div_real, true).cache
    }else if(!cambio_div_pa.isEmpty){
        cambio_div = cambio_div_pa.cache
    }else if(!cambio_div_real.isEmpty){
        cambio_div = cambio_div_real.cache
    }

    if (!cambio_div.isEmpty){
        val df_ebit_join = df_ebit.join(cambio_div,df_ebit("periodo") ===  cambio_div("id_periodo") 
            && df_ebit("id_escenario_bfc") ===  cambio_div("id_escenario"),"fullouter").drop("id_escenario").drop("periodo").cache

        val DFwithYear = df_ebit_join.withColumn("anio",col("id_periodo").substr(0, 4))

        val windowSpecfAgg = Window.partitionBy("anio", "id_escenario_bfc").orderBy("id_periodo")

        val aggDF_L = DFwithYear.withColumn("acum", sum(col("2049_imp_ebit_css_recurrente_pre")).over(windowSpecfAgg)).cache

        val pre_pivot = aggDF_L.withColumn("2049_imp_ebit_css_recurrente", col("acum") * col("imp_cambio_divisa"))
            .select("id_escenario_bfc", "id_periodo", "2049_imp_ebit_css_recurrente").where(col("id_periodo") === periodo || col("id_periodo") === (periodo-1)).cache

        val pivotDF = pre_pivot.groupBy("id_escenario_bfc").pivot("id_periodo").sum("2049_imp_ebit_css_recurrente")

        var v_periodo_min = v_year.toString().concat("01")
        print(v_periodo_min)

        var unPivotDF = spark.emptyDataFrame
        if(bfcperiodo == v_periodo_min){

            unPivotDF = pivotDF
            .withColumn("id_periodo", lit(bfcperiodo))
            .withColumnRenamed(bfcperiodo, "2049_imp_ebit_css_recurrente")
            .withColumnRenamed("id_escenario_bfc", "id_escenario").cache
            // unPivotDF.show()

        }else{
            val pivotDF_select = pivotDF.withColumn("ebit",col(periodo.toString) - col((periodo-1).toString))

            unPivotDF = pivotDF_select.select($"id_escenario_bfc",
            expr(s"stack(1, $bfcperiodo, ebit) as (id_periodo,2049_imp_ebit_css_recurrente)"))
            .where("ebit is not null")
            .withColumnRenamed("id_escenario_bfc", "id_escenario").cache
            // unPivotDF.show()
        }   

        df_33_2 = unPivotDF
    }
}

if (df_33_2.isEmpty){
    // Define the schema
    val schema = StructType(Seq(
    StructField("id_escenario", IntegerType, nullable = true),
    StructField("2049_imp_ebit_css_recurrente", DoubleType, nullable = true),
    StructField("id_periodo", StringType, nullable = false)
    ))

    // Create Rows for the DataFrame
    val row1 = Row(1, null, aperiodo)
    val row2 = Row(2, null, aperiodo)

    // Create DataFrame
    val data = Seq(row1, row2)
    df_33_2 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
}

df_33_2.limit(1).count()


# In[74]:


// display(df_33_2)


# In[76]:


var df_33 = spark.emptyDataFrame
if (!(escenario == "EST") && !(escenario == "UPA")){
        df_33 = df_33_1.join(df_33_2,df_33_1("periodo") ===  df_33_2("id_periodo") 
        && df_33_1("id_escenario_bfc") ===  df_33_2("id_escenario"),"fullouter").drop("id_escenario").drop("id_periodo").cache
}


# **servicios**

# In[77]:


val columnsToEnsureDG = List(
"Otros servicios DG##CRC",
"Otros servicios DG##E-Commerce",
"Otros servicios DG##Fidelización Global",
"Otros servicios DG##Inteligencia de Cliente",
"Otros servicios DG##Marketing Cloud",
"Otros servicios DG##Marketing, Fidelización y Eventos",
"Otros servicios DG##Otros servicios",
"Servicios transversales DG##Otros servicios transversales DG",
"Servicios transversales DG##P&C Cliente",
"Servicios transversales DG##PyO Cliente",
"Servicios transversales DG##Sostenibilidad Cliente")


# In[78]:


var cast_DG1 = spark.emptyDataFrame

if (escenario == "ALL" || escenario == "REAL"){
    
    var path_dg = s"abfss://$my_container@$my_account/AM_COM_Vista_Cliente/vc_manual_serviciosdg_real/"

    val tabla_destino = spark.read.parquet(path_dg).cache

    var serviciosDG_t = tabla_destino.where( ( col("unidadnegocio") === "Central Movilidad Mexico" ) && col("periodo") === bfcperiodo ) // NN o CG

    var df = Seq((bfcperiodo,"",0,"")).toDF("periodo","nombrekpi", "valorkpi", "unidadnegocio")
    var df_servicios_DG = df.join(serviciosDG_t, df("periodo") === serviciosDG_t("periodo"), "left")
        .select( df("periodo"), serviciosDG_t("nombrekpi"), serviciosDG_t("valorkpi").cast(DecimalType(38,2)), serviciosDG_t("unidadnegocio")).cache

    //display(serviciosDG_t.select("periodo").distinct)
    var pivot_DG =  df_servicios_DG.groupBy("periodo", "unidadnegocio").pivot("nombrekpi").sum("valorkpi")

    var df_11 = look_columns_else_null(pivot_DG, columnsToEnsureDG)

    cast_DG1 = df_11.select(
        col("periodo").cast(IntegerType).as("periodo_dg"),
        //col("unidadnegocio"),
        col("Otros servicios DG##CRC").cast(DecimalType(38,2)).as("otros_serv_dg_crc"),
        col("Otros servicios DG##E-Commerce").cast(DecimalType(38,2)).as("otros_serv_dg_e_commerce"),
        col("Otros servicios DG##Fidelización Global").cast(DecimalType(38,2)).as("otros_serv_dg_fidelizacion_global"),
        col("Otros servicios DG##Inteligencia de Cliente").cast(DecimalType(38,2)).as("otros_serv_dg_inteligencia_cliente"),
        col("Otros servicios DG##Marketing Cloud").cast(DecimalType(38,2)).as("otros_serv_dg_marketing_cloud"),
        col("Otros servicios DG##Marketing, Fidelización y Eventos").cast(DecimalType(38,2)).as("otros_serv_dg_marketing_fid_eventos"),
        col("Otros servicios DG##Otros servicios").cast(DecimalType(38,2)).as("otros_serv_dg_otros_servicios"),
        col("Servicios transversales DG##Otros servicios transversales DG").cast(DecimalType(38,2)).as("serv_transv_otros_serv_transversales"),
        col("Servicios transversales DG##P&C Cliente").cast(DecimalType(38,2)).as("serv_transv_pyc_cliente"),
        col("Servicios transversales DG##PyO Cliente").cast(DecimalType(38,2)).as("serv_transv_pyo_cliente"),
        col("Servicios transversales DG##Sostenibilidad Cliente").cast(DecimalType(38,2)).as("serv_transv_sostenibilidad_cliente")
        ).withColumn("id_escenario_dg", lit(1)).cache
}		



# In[79]:


var cast_DG2 = spark.emptyDataFrame
if (escenario == "ALL" || escenario == "PA-UPA" || escenario == "PA"){
    val path = s"abfss://$my_container@$my_account/AM_COM_Vista_Cliente/vc_manual_serviciosdg_pa/"
    val carga_pa = mssparkutils.fs.ls(path).filter( _.isDir).map( file => file.name).max
    val V_tabla_servicios_dg_pa = s"$path$carga_pa"
    val serviciosDG_pa = spark.read.parquet(V_tabla_servicios_dg_pa).cache

    var serviciosDG_t = serviciosDG_pa.where( ( col("unidadnegocio") === "Central Movilidad Mexico" ) && col("periodo") === bfcperiodo ) // NN o CG

    var df = Seq((bfcperiodo,"",0,"")).toDF("periodo","nombrekpi", "valorkpi", "unidadnegocio")
    var df_servicios_DG = df.join(serviciosDG_t, df("periodo") === serviciosDG_t("periodo"), "left")
        .select( df("periodo"), serviciosDG_t("nombrekpi"), serviciosDG_t("valorkpi").cast(DecimalType(38,2)), serviciosDG_t("unidadnegocio")).cache

    //display(serviciosDG_t.select("periodo").distinct)
    var pivot_DG =  df_servicios_DG.groupBy("periodo", "unidadnegocio").pivot("nombrekpi").sum("valorkpi")

    var df_11 = look_columns_else_null(pivot_DG, columnsToEnsureDG)

    cast_DG2 = df_11.select(
        col("periodo").cast(IntegerType).as("periodo_dg"),
        //col("unidadnegocio"),
        col("Otros servicios DG##CRC").cast(DecimalType(38,2)).as("otros_serv_dg_crc"),
        col("Otros servicios DG##E-Commerce").cast(DecimalType(38,2)).as("otros_serv_dg_e_commerce"),
        col("Otros servicios DG##Fidelización Global").cast(DecimalType(38,2)).as("otros_serv_dg_fidelizacion_global"),
        col("Otros servicios DG##Inteligencia de Cliente").cast(DecimalType(38,2)).as("otros_serv_dg_inteligencia_cliente"),
        col("Otros servicios DG##Marketing Cloud").cast(DecimalType(38,2)).as("otros_serv_dg_marketing_cloud"),
        col("Otros servicios DG##Marketing, Fidelización y Eventos").cast(DecimalType(38,2)).as("otros_serv_dg_marketing_fid_eventos"),
        col("Otros servicios DG##Otros servicios").cast(DecimalType(38,2)).as("otros_serv_dg_otros_servicios"),
        col("Servicios transversales DG##Otros servicios transversales DG").cast(DecimalType(38,2)).as("serv_transv_otros_serv_transversales"),
        col("Servicios transversales DG##P&C Cliente").cast(DecimalType(38,2)).as("serv_transv_pyc_cliente"),
        col("Servicios transversales DG##PyO Cliente").cast(DecimalType(38,2)).as("serv_transv_pyo_cliente"),
        col("Servicios transversales DG##Sostenibilidad Cliente").cast(DecimalType(38,2)).as("serv_transv_sostenibilidad_cliente")
        ).withColumn("id_escenario_dg", lit(2)).cache
}


# In[82]:


var finaliz = spark.emptyDataFrame
var cast_DG = spark.emptyDataFrame
if (!(escenario == "EST") && !(escenario == "UPA")){
    if (escenario == "ALL"){
        cast_DG = cast_DG1.union(cast_DG2)
    }else if (escenario == "REAL"){
        cast_DG = cast_DG1
    }else if (escenario == "PA-UPA" || escenario == "PA"){
        cast_DG = cast_DG2
    }
    finaliz = df_33.join(cast_DG, df_33("id_escenario_bfc") === cast_DG("id_escenario_dg") && df_33("periodo") === cast_DG("periodo_dg"), "left").dropDuplicates.cache

}


# Cambié los as de las variables eurosPA y eurosReal

# In[83]:


var eurosReal = spark.emptyDataFrame
if (escenario == "ALL" || escenario == "REAL"){
    var tasaReal = total.select(col("valorkpi")).where(col("des_kpi") === "Central Movilidad Mexico_Tipo de Cambio MXN/EUR mensual" && col("periodo") === aperiodo && col("escenario") === "RE")
    var tasaRealDec = tasaReal.select(bround(tasaReal("valorkpi"), 2).as("valorkpi"))
    var real = tasaRealDec.join(finaliz).where(col("id_escenario_bfc") === 1).cache

    eurosReal = real.select(
        lit("3").cast(IntegerType).as("id_escenario_bfc"),
        col("periodo").cast(IntegerType).as("id_periodo"),
        lit(col("2049_imp_ebit_css_recurrente").cast(DecimalType(24,10))).cast(DecimalType(24,10)).as("imp_2049_imp_ebit_css_recurrente"),
        lit(col("2049_imp_juridico").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_juridico"),
        lit(col("2049_imp_compras").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_compras"),
        lit(col("2049_imp_econom_admin").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_econom_admin"),
        lit(col("2049_imp_financiero").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_financiero"),
        lit(col("2049_imp_fiscal").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_fiscal"),
        lit(col("2049_imp_impuestos").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_impuestos"),
        lit(col("2049_imp_minoritarios").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_minoritarios"),
        lit(col("2049_imp_otros").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_otros"),
        lit(col("2049_imp_pyo").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_pyo"),
        lit(col("2049_imp_resultado_especifico_adi").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_resul_espec_adi"),
        lit(col("2049_imp_servicios_globales").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_serv_globales"),
        lit(col("2049_imp_ti").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_ti"),
        lit(col("2049_imp_ti_as_a_service").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_ti_as_a_service"),
        lit(col("2049_imp_efecto_patrimonial_adi").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_efecto_patri_adi"),
        lit(col("2049_imp_participadas").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_participadas"), //desde acá
        lit(col("2049_imp_seguros").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_seguros"),
        lit(col("2049_imp_digitalizacion").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_digitalizacion"),
        lit(col("2049_imp_sostenibilidad").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_sostenibilidad"),
        lit(col("2049_imp_auditoria").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_auditoria"),
        lit(col("2049_imp_planif_control").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_planif_control"),
        lit(col("2049_imp_ser_corporativos").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_ser_corporativos"), //acá
        lit(col("otros_serv_dg_crc").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_crc"),
        lit(col("otros_serv_dg_e_commerce").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_e_commerce"),
        lit(col("otros_serv_dg_fidelizacion_global").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_fidel_global"),
        lit(col("otros_serv_dg_inteligencia_cliente").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_int_cliente"),
        lit(col("otros_serv_dg_marketing_cloud").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_mkt_cloud"),
        lit(col("otros_serv_dg_marketing_fid_eventos").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_mkt_fid_evt"),
        lit(col("otros_serv_dg_otros_servicios").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_otros_serv"),
        lit(col("serv_transv_otros_serv_transversales").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_serv_trans_otros_ser_trans"),
        lit(col("serv_transv_pyc_cliente").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_serv_transv_pyc_cliente"),
        lit(col("serv_transv_pyo_cliente").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_serv_transv_pyo_cliente"),
        lit(col("serv_transv_sostenibilidad_cliente").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_serv_transv_sost_cliente"), //nuevo
        lit(col("2049_imp_patrimonial_seg").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_patrimonial_seg"),
        lit(col("2049_imp_comunicacion").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_comunicacion"),
        lit(col("2049_imp_techlab").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_techlab"),
        lit(col("2049_imp_ingenieria").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_ingenieria")).cache
    }


# In[84]:


var eurosPA = spark.emptyDataFrame
if (escenario == "ALL" || escenario == "PA-UPA" || escenario == "PA"){
    var tasaPA = total.select(col("valorkpi")).where(col("des_kpi")==="Central Movilidad Mexico_Tipo de Cambio MXN/EUR mensual" && col("periodo") === aperiodo && col("escenario") === "PA")

    var tasaPADec = tasaPA.select(bround(tasaPA("valorkpi"), 2).as("valorkpi"))

    var pa = tasaPADec.join(finaliz).where(col("id_escenario_bfc") === 2).cache

    eurosPA = pa.select(
        lit("2").cast(IntegerType).as("id_escenario_bfc"),
        col("periodo").cast(IntegerType).as("id_periodo"),
        lit(col("2049_imp_ebit_css_recurrente").cast(DecimalType(24,10))).cast(DecimalType(24,10)).as("imp_2049_imp_ebit_css_recurrente"),
        lit(col("2049_imp_juridico").cast(DecimalType(24,10)) * col("valorkpi") * -1).cast(DecimalType(24,10)).as("imp_2049_imp_juridico"),
        lit(col("2049_imp_compras").cast(DecimalType(24,10)) * col("valorkpi") * -1).cast(DecimalType(24,10)).as("imp_2049_imp_compras"),
        lit(col("2049_imp_econom_admin").cast(DecimalType(24,10)) * col("valorkpi") * -1).cast(DecimalType(24,10)).as("imp_2049_imp_econom_admin"),
        lit(col("2049_imp_financiero").cast(DecimalType(24,10)) * col("valorkpi") * -1).cast(DecimalType(24,10)).as("imp_2049_imp_financiero"),
        lit(col("2049_imp_fiscal").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_fiscal"),
        lit(col("2049_imp_impuestos").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_impuestos"),
        lit(col("2049_imp_minoritarios").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_minoritarios"),
        lit(col("2049_imp_otros").cast(DecimalType(24,10)) * col("valorkpi") * -1).cast(DecimalType(24,10)).as("imp_2049_imp_otros"),
        lit(col("2049_imp_pyo").cast(DecimalType(24,10)) * col("valorkpi") * -1).cast(DecimalType(24,10)).as("imp_2049_imp_pyo"),
        lit(col("2049_imp_resultado_especifico_adi").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_resul_espec_adi"),
        lit(col("2049_imp_servicios_globales").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_serv_globales"),
        lit(col("2049_imp_ti").cast(DecimalType(24,10)) * col("valorkpi") * -1).cast(DecimalType(24,10)).as("imp_2049_imp_ti"),
        lit(col("2049_imp_ti_as_a_service").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_ti_as_a_service"),
        lit(col("2049_imp_efecto_patrimonial_adi").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_efecto_patri_adi"),
        lit(col("2049_imp_participadas").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_participadas"), //se agregó
        lit(col("2049_imp_seguros").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_seguros"),
        lit(col("2049_imp_digitalizacion").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_digitalizacion"),
        lit(col("2049_imp_sostenibilidad").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_sostenibilidad"),
        lit(col("2049_imp_auditoria").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_auditoria"),
        lit(col("2049_imp_planif_control").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_planif_control"),
        lit(col("2049_imp_ser_corporativos").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_ser_corporativos"), //hasta acá
        lit(col("otros_serv_dg_crc").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_crc"),
        lit(col("otros_serv_dg_e_commerce").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_e_commerce"),
        lit(col("otros_serv_dg_fidelizacion_global").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_fidel_global"),
        lit(col("otros_serv_dg_inteligencia_cliente").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_int_cliente"),
        lit(col("otros_serv_dg_marketing_cloud").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_mkt_cloud"),
        lit(col("otros_serv_dg_marketing_fid_eventos").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_mkt_fid_evt"),
        lit(col("otros_serv_dg_otros_servicios").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_otros_serv_dg_otros_serv"),
        lit(col("serv_transv_otros_serv_transversales").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_serv_trans_otros_ser_trans"),
        lit(col("serv_transv_pyc_cliente").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_serv_transv_pyc_cliente"),
        lit(col("serv_transv_pyo_cliente").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_serv_transv_pyo_cliente"),
        lit(col("serv_transv_sostenibilidad_cliente").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_serv_transv_sost_cliente"),//
        lit(col("2049_imp_patrimonial_seg").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_patrimonial_seg"),
        lit(col("2049_imp_comunicacion").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_comunicacion"),
        lit(col("2049_imp_techlab").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_techlab"),
        lit(col("2049_imp_ingenieria").cast(DecimalType(24,10)) * col("valorkpi")).cast(DecimalType(24,10)).as("imp_2049_imp_ingenieria")).cache
    }


# In[85]:


var finalizado = spark.emptyDataFrame

if (escenario == "ALL" ){  
    finalizado = eurosReal.union(eurosPA).cache
}else if (escenario == "REAL"){
    finalizado = eurosReal
}else if (escenario == "PA-UPA" || escenario == "PA"){
    finalizado = eurosPA
}

if(!(finalizado.isEmpty)){
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

    val ds_output = s"cli0010_tb_aux_m_bfc_dg_mmex/"
    val ds_output_temp = ds_output.concat("temp")
    val pathWriteTemp = s"$edwPath/$business/$ds_output_temp"
    val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

    finalizado.coalesce(1).write.partitionBy("id_periodo").mode("overwrite").format("parquet").save(parquet_path_temp)
    finalizado.count
}


# **Ingesta y Carga de maestros para PBI**

# In[86]:


val business="AP_Vista_Cliente"

// Path raiz de la ruta de los maestros del gen2
val my_account = conexion("Endpoint").toString.substring(8)

val manual_vc_pbi_conceptos_MM_path = s"abfss://$my_container@$my_account/sanited/$business/GLB/manual_vc_pbi_conceptos_MM"
val manual_vc_pbi_dim1_MM_path = s"abfss://$my_container@$my_account/sanited/$business/GLB/manual_vc_pbi_dim1_MM"
val manual_vc_pbi_dim2_MM_path = s"abfss://$my_container@$my_account/sanited/$business/GLB/manual_vc_pbi_dim2_MM"
val manual_vc_pbi_negocio_MM_path = s"abfss://$my_container@$my_account/sanited/$business/GLB/manual_vc_pbi_negocio_MM"


# In[87]:


val manual_vc_pbi_conceptos_MM = spark.read.parquet(manual_vc_pbi_conceptos_MM_path)
val s_pbi_conceptos_MM = manual_vc_pbi_conceptos_MM.select(
        col("ID_Concepto").as("id_concepto").cast(IntegerType),
        col("ID_Divisor").as("id_divisor").cast(IntegerType),
        col("Visible").as("val_visible").cast(IntegerType),
        col("Orden").as("val_orden").cast(IntegerType),
        col("Tipo_Calculo").as("val_tipo_calculo").cast(IntegerType),
        col("Signo_Waterfall").as("val_signo_waterfall").cast(IntegerType),
        col("ID_Dividendo").as("id_dividendo").cast(IntegerType),
        col("Concepto").as("des_concepto"),
        col("Concepto_Tabla").as("nom_concepto_tabla"),
        translate(col("ARiATimestampLoad"), "T"," ").as("tst_system_date"))


# In[88]:


s_pbi_conceptos_MM
    .write.
        format("com.microsoft.sqlserver.jdbc.spark").
        mode("overwrite").
        option("url", url).
        option("dbtable", s"sch_anl.cli0010_tb_aux_x_mncon_mmex").
        option("mssqlIsolationLevel", "READ_UNCOMMITTED").
        option("truncate", "true").
        option("tableLock","false").
        option("reliabilityLevel","BEST_EFFORT").
        option("numPartitions","1").
        option("batchsize","1000000").
        option("accessToken", token).
        save()


# In[89]:


val manual_vc_pbi_dim1_MM = spark.read.parquet(manual_vc_pbi_dim1_MM_path)
val s_pbi_dim1_MM = manual_vc_pbi_dim1_MM.select(
        col("ID_Dim1").cast(IntegerType).as("id_dim1"),
        col("Orden").as("val_orden"),
        col("Valor").cast(VarcharType(130)).as("val_valor"),
        translate(col("ARiATimestampLoad"), "T"," ").cast(VarcharType(130)).as("tst_system_date"))


# In[90]:


s_pbi_dim1_MM
    .write.
        format("com.microsoft.sqlserver.jdbc.spark").
        mode("overwrite").
        option("url", url).
        option("dbtable", s"sch_anl.cli0010_tb_aux_x_pbidim1_mmex").
        option("mssqlIsolationLevel", "READ_UNCOMMITTED").
        option("truncate", "true").
        option("tableLock","false").
        option("reliabilityLevel","BEST_EFFORT").
        option("numPartitions","1").
        option("batchsize","1000000").
        option("accessToken", token).
        save()


# In[91]:


val manual_vc_pbi_dim2_MM = spark.read.parquet(manual_vc_pbi_dim2_MM_path)
val s_pbi_dim2_MM = manual_vc_pbi_dim2_MM.select(
        col("ID_Dim2").as("id_dim2"),
        col("Valor").as("val_valor"),
        col("Orden").as("val_orden"),
        translate(col("ARiATimestampLoad"), "T"," ").cast(StringType).as("tst_system_date"))


# In[92]:


s_pbi_dim2_MM
    .write.
        format("com.microsoft.sqlserver.jdbc.spark").
        mode("overwrite").
        option("url", url).
        option("dbtable", s"sch_anl.cli0010_tb_aux_x_pbidim2_mmex").
        option("mssqlIsolationLevel", "READ_UNCOMMITTED").
        option("truncate", "true").
        option("tableLock","false").
        option("reliabilityLevel","BEST_EFFORT").
        option("numPartitions","1").
        option("batchsize","1000000").
        option("accessToken", token).
        save()


# In[93]:


val manual_vc_pbi_negocio_MM = spark.read.parquet(manual_vc_pbi_negocio_MM_path)
val s_pbi_negocio_MM = manual_vc_pbi_negocio_MM.select(
        col("ID_Negocio").cast(IntegerType).as("id_negocio"),
        col("OrdenNegocio").cast(IntegerType).as("id_orden_negocio"),
        col("OrdenSubnegocio").cast(IntegerType).as("id_orden_subnegocio"),
        col("ID_Subnegocio").cast(VarcharType(130)).as("id_subnegocio"),
        col("Subnegocio").cast(VarcharType(130)).as("val_subnegocio"),
        col("PK").cast(VarcharType(130)).as("cod_pk"),
        col("Negocio").cast(VarcharType(130)).as("val_negocio"),
        col("Area").cast(VarcharType(130)).as("val_area"),
        translate(col("ARiATimestampLoad"), "T"," ").cast(VarcharType(130)).as("tst_system_date")).cache


# In[94]:


s_pbi_negocio_MM
    .write.
        format("com.microsoft.sqlserver.jdbc.spark").
        mode("overwrite").
        option("url", url).
        option("dbtable", s"sch_anl.cli0010_tb_aux_x_pbineg_mmex").
        option("mssqlIsolationLevel", "READ_UNCOMMITTED").
        option("truncate", "true").
        option("tableLock","true").
        option("reliabilityLevel","BEST_EFFORT").
        option("numPartitions","1").
        option("batchsize","1000000").
        option("accessToken", token).
        save()


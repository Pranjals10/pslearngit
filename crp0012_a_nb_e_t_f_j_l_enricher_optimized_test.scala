// Databricks notebook source
// MAGIC %md
// MAGIC %md
// MAGIC # Plantilla Notebook para desarrollo a medida
// MAGIC
// MAGIC #### Objetivo
// MAGIC Este notebook una plantilla para implementar un desarrollo a medida
// MAGIC
// MAGIC #### Uso
// MAGIC Para su hay que definir los siguiente parámetros desde la pipeline que se invoque
// MAGIC
// MAGIC | Parámetro | Valor por defecto | Descripcion |
// MAGIC |---|---|---|
// MAGIC |applicationName||Nombre de la aplicación|
// MAGIC |uuid||Identificador único de la ejecución|
// MAGIC |puid||Identificador único del padre|
// MAGIC |name||Nombre para el trazado|
// MAGIC |step| 0 |Paso en la ejecución de la pipeline|
// MAGIC |uniqueKey| |Identificador de la ejecución|
// MAGIC |referenceDate||Fecha de ejecución del orquestador|
// MAGIC |digitalCase| |Código del caso digital|
// MAGIC |optimizeDelta|false |Indica si se ejecuta _vacuum_ y _optimize_ en la tabla delta|
// MAGIC |sources||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>    <tr>          </tr>    <tr>         </tr>    <tr>      <td>source_object</td>      <td>Ruta en la que se encuentran los datos</td>    </tr>    <tr>      <td>source_format</td>      <td>Formato del objeto a leer</td>    </tr>  <tr>      <td>source_format_options</td>      <td>Opciones del formato</td>    </tr> <tr>      <td>source_alias</td>      <td>Identificador del dataset</td>    </tr> </tbody></table>|
// MAGIC |target||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>    <tr>        </tr>    <tr>        </tr>    <tr>      <td>target_object</td>      <td>Un objeto en el que se almacenan los datos resultantes</td>    </tr>    <tr>      <td>target_format</td>      <td>Formato de la ruta a escribir</td>    </tr>   <tr> <td>target_dl_name_tags</td> <td>Si se utiliza el modo de sobreescritura dinámica, proporcione los nombres de las columnas que se deben usar para determinar qué particiones se deben sobrescribir</td> </tr> <tr> <td>target_write_mode</td> <td>Especifica el tipo de operación de escritura que se debe realizar en la tabla de destino (por ejemplo: overwrite, dynamic, scd1)</td> </tr> <td>target_operation_name</td>      <td>la operación objetivo que debe realizarse utilizando writeWrapper</td>    </tr> </tbody></table>|
// MAGIC |name||Nombre para el trazado|
// MAGIC
// MAGIC #### Nota
// MAGIC
// MAGIC En las rutas en formato delta se permite evolución de esquema.
// MAGIC

// COMMAND ----------

dbutils.widgets.text("applicationName", "")
var applicationName: String = dbutils.widgets.get("applicationName")

dbutils.widgets.text("uuid", "N/A")
val uuid: String = dbutils.widgets.get("uuid")

dbutils.widgets.text("parentUid", "N/A")
val puid: String = dbutils.widgets.get("parentUid")

dbutils.widgets.text("sources", "[{}]")
var sources: String = dbutils.widgets.get("sources")

dbutils.widgets.text("target", "{}")
val target: String = dbutils.widgets.get("target")

dbutils.widgets.text("name", "")
val name: String = dbutils.widgets.get("name")

dbutils.widgets.text("auxParams", "{}")
val auxParams: String = dbutils.widgets.get("auxParams")

dbutils.widgets.text("params", "{}")
val params: String = dbutils.widgets.get("params")

dbutils.widgets.text("sparkProperties", """{}""")
val sparkProperties: String = dbutils.widgets.get("sparkProperties")

dbutils.widgets.text("referenceDate", "")
val referenceDate: String = dbutils.widgets.get("referenceDate")

dbutils.widgets.text("digitalCase", "")
val digitalCase: String = dbutils.widgets.get("digitalCase")

dbutils.widgets.text("uniqueKey", "")
var uniqueKey: String = dbutils.widgets.get("uniqueKey")

dbutils.widgets.text("optimizeDelta", "false")
val optimizeDelta: Boolean = dbutils.widgets.get("optimizeDelta").toBoolean

// dbutils.widgets.text("targets", "{}")
// val targets: String = dbutils.widgets.get("targets")

// COMMAND ----------

sources = """
[
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_temp_orchestrator",
                            "source_format": "parquet",
                            "source_deep_partition": 0,
                            "source_num_partitions": 0,
                            "source_alias": "crp0012_tb_temp_orchestrator"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_business_unt/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_business_unt"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_core_company/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_core_company"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_fiscl_period/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_fiscl_period"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_itfm_cst_mod/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_itfm_cst_mod"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_sys_user/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_sys_user"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_u_cost_chrgs/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_u_cost_chrgs"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_business_unt/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_business_unt"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_core_company/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_core_company"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_fiscl_period/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_fiscl_period"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_itfm_cst_mod/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_itfm_cst_mod"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_sys_user/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_sys_user"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_u_cost_chrgs/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_u_cost_chrgs"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_business_unt/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_business_unt"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_cmdb_bs_cpby/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_cmdb_bs_cpby"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_cmdb_ci_srvc/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_cmdb_ci_srvc"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_core_company/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_core_company"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_fiscl_period/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_fiscl_period"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_itfm_cst_mod/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_itfm_cst_mod"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_mc_bu_dg/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_mc_bu_dg"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_mc_dg_vp/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_mc_dg_vp"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_u_cost_chrgs/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_u_cost_chrgs"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_cmdb_bs_cpby/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_cmdb_bs_cpby"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_fiscl_period/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_fiscl_period"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_itfm_cst_mod/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_itfm_cst_mod"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_u_cost_chrgs/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_u_cost_chrgs"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_cmdb_bs_cpby/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_cmdb_bs_cpby"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_cmn_location/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_cmn_location"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_fiscl_period/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_fiscl_period"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_itfm_cst_mod/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_itfm_cst_mod"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_sys_user/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_sys_user"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_u_cost_chrgs/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_u_cost_chrgs"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_cmdb_bs_cpby/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_cmdb_bs_cpby"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_cmn_location/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_cmn_location"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_fiscl_period/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_fiscl_period"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_itfm_cst_mod/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_itfm_cst_mod"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_mc_ct_srvics/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_mc_ct_srvics"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_sys_user/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_sys_user"
                        },
                        {
                            "source_object": "/Volumes/datahub01taucdatagold/config_crp0012/tests/sources/destination/crp0012_tb_fact_x_u_cost_chrgs/",
                            "source_format": "parquet",
                            "source_num_partitions": 0,
                            "source_deep_partition": 0,
                            "source_alias": "crp0012_tb_fact_x_u_cost_chrgs"
                        }
                    ]
"""

// COMMAND ----------

val targets = """
[
                        {
                            "target_object": "/Volumes/datahub01%env%aucdatagold/config_crp0012/tests/tmp_output_test/destination/crp0012_tb_fact_x_mc_dg_vp/",
                            "target_format": "parquet",
                            "target_write_mode": "overwrite",
                            "target_operation_name": "direct_write_using_writeModes",
                            "target_dl_name_tags": [],
                            "target_enforce_schema": "false"
                        },
                        {
                            "target_object": "/Volumes/datahub01%env%aucdatagold/config_crp0012/tests/tmp_output_test/destination/crp0012_tb_fact_x_mc_bu_dg/",
                            "target_format": "parquet",
                            "target_write_mode": "overwrite",
                            "target_operation_name": "direct_write_using_writeModes",
                            "target_dl_name_tags": [],
                            "target_enforce_schema": "false"
                        },
                        {
                            "target_object": "/Volumes/datahub01%env%aucdatagold/config_crp0012/tests/tmp_output_test/destination/crp0012_tb_fact_x_mc_cost_chrg/",
                            "target_format": "parquet",
                            "target_write_mode": "overwrite",
                            "target_operation_name": "direct_write_using_writeModes",
                            "target_dl_name_tags": [],
                            "target_enforce_schema": "false"
                        },
                        {
                            "target_object": "/Volumes/datahub01%env%aucdatagold/config_crp0012/tests/tmp_output_test/destination/crp0012_tb_fact_x_mc_sl_srvics/",
                            "target_format": "parquet",
                            "target_write_mode": "overwrite",
                            "target_operation_name": "direct_write_using_writeModes",
                            "target_dl_name_tags": [],
                            "target_enforce_schema": "false"
                        },
                        {
                            "target_object": "/Volumes/datahub01%env%aucdatagold/config_crp0012/tests/tmp_output_test/destination/crp0012_tb_fact_x_mc_ct_srvics/",
                            "target_format": "parquet",
                            "target_write_mode": "overwrite",
                            "target_operation_name": "direct_write_using_writeModes",
                            "target_dl_name_tags": [],
                            "target_enforce_schema": "false"
                        },
                        {
                            "target_object": "/Volumes/datahub01%env%aucdatagold/config_crp0012/tests/tmp_output_test/destination/crp0012_tb_fact_x_mc_ct_sb_svcs/",
                            "target_format": "parquet",
                            "target_write_mode": "overwrite",
                            "target_operation_name": "direct_write_using_writeModes",
                            "target_dl_name_tags": [],
                            "target_enforce_schema": "false"
                        }
                    ]
""""

// COMMAND ----------

// MAGIC %run /Shared/DAB-CRP0012/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

implicit val spark1:SparkSession=spark

// COMMAND ----------

implicit val formats = DefaultFormats

// COMMAND ----------

var msg = List[String]()
var savedTables = List[String]()
var error:String = ""  

// COMMAND ----------

var sourceArray: List[Map[String, Any]] = Nil
var targetArray: List[Map[String, Any]] = Nil
var aux_params: Map[String, Any] = Map.empty
var masterMap: List[Map[String, Any]] = Nil
var prevTable: Map[String, Any] = Map()
var modelName: String = ""

try {
  sourceArray = parse(sources).extract[List[Map[String, Any]]]
  targetArray = parse(targets).extract[List[Map[String, Any]]]
  aux_params = parse(auxParams).extract[Map[String, Any]]

  // Access values
  masterMap = aux_params("master_map").asInstanceOf[List[Map[String, Any]]]
  modelName = aux_params("modelName").toString
  prevTable = sourceArray(0)
} catch {
  case e: Exception =>
    println(e.getMessage)
    error = e.getMessage
}

// COMMAND ----------

var orchDF: DataFrame = null
try {
  orchDF = getDataFromSource(prevTable)
  }
catch {
  case e: Exception =>
    println(e.getMessage)
    error = e.getMessage
}
// Filter for RUNNING status
val runningDF = orchDF.filter(col("status") === "RUNNING")

// Get the "values" column
val runningValues = runningDF.select("values").as[String].collect()

// Check for presence of "Model Name"
val continueExecution = runningValues.exists { value =>
  value.split(",").map(_.trim).contains(s"$modelName")  
}

if (continueExecution) {
  println(s"$modelName is in RUNNING state. Continuing notebook execution.")
} else {
  println(s"$modelName is NOT in RUNNING state. Stopping notebook execution.")
  dbutils.notebook.exit(s"STOP: $modelName not running.")
}


// COMMAND ----------

def convertSchemaToTarget(df: DataFrame, tableName: String)
                         (implicit spark: SparkSession): DataFrame = {

  // Get target schema from table
  val targetDF = getDataFromSource(Map("source_format" -> "Table", "source_object" -> tableName))
  val targetSchema = targetDF.schema

  // Step 1: Select only matching columns
  val selectedCols = targetSchema.fieldNames.map(col)
  val dfSelected = df.select(selectedCols: _*)

  // Step 2: Cast columns to target schema
  val castedCols = targetSchema.fields.map(f => col(f.name).cast(f.dataType).alias(f.name))
  dfSelected.select(castedCols: _*)
}


// COMMAND ----------

// MAGIC %md
// MAGIC Control Start

// COMMAND ----------

// // Set application name
// applicationName = modelName
// implicit val uid_app = (uuid, puid, applicationName)

// // Set custom digital case if provided
// if (digitalCase.trim.nonEmpty) {
//   println(s"Using Digital Case: $digitalCase")
//   ControlHelper().setCustomDigitalCase(digitalCase)
// }

// // Log start of the process
// LogHelper().logStart()

// // Debug info
// println(
//   s"""
//      |applicationName = $applicationName
//      |uuid            = $uuid
//      |puid            = $puid
//      |uniqueKey       = $uniqueKey
//      |sources         = $sources
//      |target          = $target
//      |auxParams       = $auxParams
//      |params          = $params
//      |name            = $name
//      |optimizeDelta   = $optimizeDelta
//      |""".stripMargin
// )

// // Choose pipeline name based on model
// val pipelineName = if (modelName == "Billing" || modelName == "Project Execution")
//   "crp0012_a_pl_orchestator_pexec"
// else
//   "crp0012_a_pl_orchestator"

// // Define control parameters
// val timeToLive   = 60
// val dataSetIn    = "Tablas origen"
// val dataSetOut   = applicationName
// val parameters   = s"$targets"
// val steps        = 0

// // Initialize control table and update uniqueKey
// import com.repsol.datalake.control._
// uniqueKey = ControlHelper().initControlTable(
//   spark, applicationName, pipelineName, parameters, steps,
//   timeToLive = timeToLive, dataSetIN = dataSetIn, dataSetOUT = dataSetOut
// )


// COMMAND ----------

// MAGIC %md
// MAGIC Spark Properties

// COMMAND ----------

val jsonMap = Seq(sparkProperties.replaceAll("[\t]", "").replaceAll("\"\\s*\"", "\", \"")).toDF("json_col")
  .select(from_json($"json_col", MapType(StringType, StringType)).as("map"))
  .as[Map[String, String]]
  .head()

jsonMap.foreach { case (k, v) =>
  spark.conf.set(k, v)
  println(s"$k : ${spark.conf.get(k)}")
}

// COMMAND ----------

// MAGIC %md
// MAGIC Main Function

// COMMAND ----------

masterMap.foreach { entry =>
  try {
    val target_table_index = entry("target_table_index").toString.toInt
    val source_start_index = entry("source_start_index").toString.toInt
    val source_end_index   = entry("source_end_index").toString.toInt
    val sql_file_name      = entry("sql_file_name").toString

    // Process source tables
    for (i <- source_start_index to source_end_index) {
      val sourceTable = sourceArray(i)
      getDataFromSource(sourceTable)
    }

    // Read and execute SQL
    val sql_query = readFile(s"config_$digitalCase/config_files/sql/destination/$sql_file_name")
    var dftrans = spark.sql(sql_query) 

    // Get target table metadata
    val targetTable = targetArray(target_table_index)

    val target_format = targetTable.getOrElse("target_format", "").toString
    val target_operation_name = targetTable.getOrElse("target_operation_name", "").toString
    val target_object = targetTable.getOrElse("target_object", "").toString
    val target_write_mode = targetTable.getOrElse("target_write_mode", "").toString
    val target_enforce_schema = targetTable.get("target_enforce_schema") match {
      case Some(value) => value.toString.toBoolean
      case None        => true
    }

    // Apply schema if required
    val finalDFwithImposedSchema = {
      if (target_enforce_schema) {
        val targetObjStr = getObject(target_object).toString
        convertSchemaToTarget(dftrans, targetObjStr)
      } else {
        dftrans
      }
    }

    // Write data
    writeWrapper(
      finalDFwithImposedSchema,
      conf = targetTable,
      optimize = optimizeDelta,
      operationName = targetTable("target_operation_name").toString,
      prefix = "target"
    )

    // Collect results
    val rowCount = finalDFwithImposedSchema.count()
    val tableNameOnly = target_object.split("\\.").last
    val tableData = s"$tableNameOnly=$rowCount"
    msg = msg :+ tableData
    savedTables = savedTables :+ s"$tableNameOnly"

  } catch {
    case e: Exception =>
      println(e.getMessage)
      error = e.getMessage
  }
}


// COMMAND ----------

// MAGIC %md
// MAGIC Log KO and throw exception

// COMMAND ----------

// MAGIC %md
// MAGIC Control End

// COMMAND ----------

// // Initialize a result variable
// val result = ""

// // If there is any error captured earlier, handle it
// if (!error.isEmpty) {
//     // Log the error message along with the step or process name
//     LogHelper().logError(s"$name: $error")
    
//     // Mark the control table entry as "KO" (failure) with the captured error message
//     ControlHelper().endControlTable(spark, uniqueKey, steps, "KO", error)
    
//     // Throw an exception to stop execution and propagate the error
//     throw new Exception(s"$name: $error")
// } 

// // If no error occurred, log the result
// LogHelper().logInfo(s"$name: $result")

// // Log the end of the process execution
// LogHelper().logEnd()

// // Mark the control table entry as "OK" (success) with the success message
// val msgStr = msg.mkString(", ")
// ControlHelper().endControlTable(spark, uniqueKey, steps, "OK", msgStr)


// COMMAND ----------

msg

// COMMAND ----------

val salida = savedTables.mkString(", ")
dbutils.notebook.exit(salida)
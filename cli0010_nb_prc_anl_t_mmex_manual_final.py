#!/usr/bin/env python
# coding: utf-8

# ## cli0010_nb_prc_anl_t_mmex_manual_final
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


# In[5]:


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import spark.sqlContext.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column


# In[6]:


val lakehousePath = "CLI0010/trn"
val edwPath = "CLI0010/edw"
val container_output = "lakehouse"
val linked_service_name = "DL_COM"
val my_container = "processed"
val cont_lakehouse = "lakehouse"
val my_account = conexion("Endpoint").toString.substring(8)
val business="AM_COM_Vista_Cliente"


# In[7]:


val ds_output = s"cli0010_tb_aux_m_pivkpi_mmex/"
val ds_output_temp = ds_output.concat("temp")
val pathWriteTemp = s"$edwPath/$business/$ds_output_temp"
val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

var aperiodo = v_year.concat(v_month).concat("01")


# In[8]:


// {ALL / PA-UPA / EST / REAL}

var pivote = spark.emptyDataFrame

if (escenario == "REAL"){
    pivote = spark.read.parquet(parquet_path_temp).where(col("cod_periodo") === aperiodo && col("id_escenario") === 3)
}else if (escenario == "PA-UPA"){
    pivote = spark.read.parquet(parquet_path_temp).where(col("cod_periodo") === aperiodo && col("id_escenario").isin(2,4))
}else if (escenario == "EST"){
    pivote = spark.read.parquet(parquet_path_temp).where(col("cod_periodo") === aperiodo && col("id_escenario") === 1)
}else if (escenario == "PA"){
    pivote = spark.read.parquet(parquet_path_temp).where(col("cod_periodo") === aperiodo && col("id_escenario") === 2)
}else if (escenario == "UPA"){
    pivote = spark.read.parquet(parquet_path_temp).where(col("cod_periodo") === aperiodo && col("id_escenario") === 4)
}else{
    pivote = spark.read.parquet(parquet_path_temp).where(col("cod_periodo") === aperiodo)
}

pivote.cache


# In[9]:


var t_Pool_nnycg_pivot = pivote.dropDuplicates


# In[10]:


var v_periodo = v_year.concat(v_month).concat("01").toInt

val anio = v_periodo.toString().substring(0, 4).toInt


# In[11]:


var df_kpis_temp = t_Pool_nnycg_pivot.where(col("cod_periodo") === v_periodo)


# In[12]:


def look_column_else_zero( tabla : DataFrame , columna : String) : DataFrame =
{
    if ( tabla.columns.map(_.toUpperCase).contains(columna.toUpperCase) ) {
     return tabla }
    else  {
        return tabla.withColumn(columna, lit(0)) }
}


# In[13]:


var df_kpis_1 = look_column_else_zero(df_kpis_temp, "imp_ctral_total_cf_an_upa_est")
var df_kpis_2 = look_column_else_zero(df_kpis_1, "imp_tot_mm_tot_cf_an_upa_est")
var df_kpis = look_column_else_zero(df_kpis_2, "imp_ctral_amort")

// var df_kpis = df_kpis_3.select(col("imp_ctral_total_cf_an_upa_est"), col("imp_ctral_Tcambio_MXN_EUR_ac"), col("id_escenario"), col("cod_periodo"))


# In[21]:


var vc_pa_central = spark.emptyDataFrame

if (escenario == "PA-UPA" || escenario == "ALL" || escenario == "PA"){
    var df_pa_central = df_kpis
        .withColumnRenamed("imp_tot_pa_result_op_AJUSTE","pre_imp_tot_pa_result_op_AJUSTE")
        .withColumn("imp_tot_pa_result_op_AJUSTE", col("pre_imp_tot_pa_result_op_AJUSTE") * (-1))
        .drop("pre_imp_tot_pa_result_op_AJUSTE")
        .select(
        col("cod_periodo").cast(DecimalType(24,10)),
        col("imp_ctral_amort").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_com_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_otros_serv").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_pers").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_primas_seg").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_serv_banc").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_sumin").cast(DecimalType(24,10)),
        col("imp_ctral_mc").cast(DecimalType(24,10)),
        col("imp_ctral_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ctral_otros_result").cast(DecimalType(24,10)),
        col("imp_ctral_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ctral_pers_2_otros_cost").cast(DecimalType(24,10)),
        col("imp_ctral_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ctral_provis_recur").cast(DecimalType(24,10)),
        col("imp_ctral_serv_corp").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ctral_sv_ext_2_otr_sv_ext").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ctral_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_EUR_MXN").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ctral_pers").cast(DecimalType(24,10)),
        col("imp_ctral_ser_ext").cast(DecimalType(24,10)),
        col("imp_ctral_total_cf_concept").cast(DecimalType(24,10)),
        col("imp_ctral_cost_fijos").cast(DecimalType(24,10)),
        col("imp_ctral_resu_ope").cast(DecimalType(24,10)),
        col("imp_ctral_cost_fij_estruct").cast(DecimalType(24,10)),
        col("imp_ctral_total_cf_analitica").cast(DecimalType(24,10)),
        col("imp_ees_cf").cast(DecimalType(24,10)),
        col("imp_ees_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_ees_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ees_amort").cast(DecimalType(24,10)),
        col("imp_ees_corp").cast(DecimalType(24,10)),
        col("imp_ees_cost_log_dist_2_transp").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_aditivos").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_desc_comp").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mat_prima").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_ees_coste_canal").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_act_promo").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_otros_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_sis_ctrl_volu").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_tec_neotech").cast(DecimalType(24,10)),
        // col("imp_ees_cu").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_gna_autom").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_go_a_reg_diesel").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_go_ter").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_gna_auto_go_a_ter").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_prem_92").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_reg_87").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_tot_mm").cast(DecimalType(24,10)),
        col("imp_ees_EBIT_CCS_JV_mar_cortes").cast(DecimalType(24,10)),
        col("imp_ees_extram_monterra").cast(DecimalType(24,10)),
        col("imp_ees_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_extr_ols").cast(DecimalType(24,10)),
        col("imp_ees_IEPS").cast(DecimalType(24,10)),
        col("imp_ees_ing_brut").cast(DecimalType(24,10)),
        col("imp_ees_mb_sin_des_ing_c_prima").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_extram_mont").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_mc_sn_res_est").cast(DecimalType(24,10)),
        //col("imp_ees_num_ees_repsol").cast(DecimalType(24,10)),
        //col("imp_ees_num_ees_mercado").cast(DecimalType(24,10)),
        col("imp_ees_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc_2_gdirec_mc").cast(DecimalType(24,10)),
        col("imp_ees_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_pers_2_otros_cost_pers").cast(DecimalType(24,10)),
        col("imp_ees_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_ees_serv_corp").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_arrend_can").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ees_sv_ext_2_otros_sv_ext").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_tipo_cambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_tipo_cambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ees_ventas_go_a_reg_diesel").cast(DecimalType(24,10)),
        col("imp_ees_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_s_res_et").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_efres_et").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_extr_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_amort").cast(DecimalType(24,10)),
        col("imp_ees_mayor_corp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_act_prm").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_mante").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_otros").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_sc_volu").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_ees_mayor_EBIT_CCS_JV_cort").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_res_est").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_ext_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_ext_ols").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_sin_res").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_mayor_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_corp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_pb_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_mayor_Tcamb_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_mayor_Tcamb_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ees_mayor_vta_go_reg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_unit_ees").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_sres_et").cast(DecimalType(24,10)),
        col("imp_mayo_mc_uni_ees_2_efres_et").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_ext_ols").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_ext_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext").cast(DecimalType(24,10)),
        col("imp_ees_mayor_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_op").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_mayor_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas").cast(DecimalType(24,10)),
        col("imp_ees_minor_vta_go_reg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_mc").cast(DecimalType(24,10)),
        col("imp_ees_minor_otro_mc_2_gdirec").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf").cast(DecimalType(24,10)),
        col("imp_ees_minor_amort").cast(DecimalType(24,10)),
        col("imp_ees_minor_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_minor_result_op").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext").cast(DecimalType(24,10)),
        col("imp_ees_minor_corp").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_corp").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_pb_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_minor_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_sc_volu").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_act_prm").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_otro_cf").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_minor_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_m_prim").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_adt").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_desc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_logist_dist").cast(DecimalType(24,10)),
        col("imp_tot_mm_cost_log_dist_2_tsp").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_canal").cast(DecimalType(24,10)),
        col("imp_tot_mm_extram_olstor").cast(DecimalType(24,10)),
        col("imp_tot_mm_extram_monterra").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb_sin_desc_cv").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_unit").cast(DecimalType(24,10)),
        //col("imp_tot_mm_tipo_cambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_tot_mm_ventas_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_ees_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_vvdd_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_cr").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_ees_cr").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_vvdd_cr").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas").cast(DecimalType(24,10)),
        col("imp_ees_ventas").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_unit_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_s_res").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_resest").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_olstor").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_unit_ees_2_mont").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_s_res_est").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_ef_res_est").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_extram_ols").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_extram_mon").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_g_direc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_vta_dir").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_inter").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_almacen").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf").cast(DecimalType(24,10)),
        col("imp_tot_mm_amort").cast(DecimalType(24,10)),
        col("imp_tot_mm_provis_recur").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_result").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_op").cast(DecimalType(24,10)),
        //col("imp_tot_mm_result_op_2_particip_minor").cast(DecimalType(24,10)),
        //col("imp_tot_mm_result_op_2_imp").cast(DecimalType(24,10)),
        //col("imp_tot_mm_result_net_ajust").cast(DecimalType(24,10)),
        //col("imp_tot_mm_result_net_ajust_2_result_especif_ddi").cast(DecimalType(24,10)),
        //col("imp_tot_mm_result_net_ajust_2_ef_patrim_ddi").cast(DecimalType(24,10)),
        //col("imp_tot_mm_result_net").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_tot_mm_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_ctrl_volu").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_act_promo").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_otro_cf").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_pers").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_viajes").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_com_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_prima_seg").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_sv_banc_s").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_sumin").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_otros_sv").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_gastos").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_corp").cast(DecimalType(24,10)),
        //col("imp_tot_mm_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_tot_ventas_go_a_reg_diesel").cast(DecimalType(24,10)),
        col("imp_tot_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_tot_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_vta_direc").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_interterm").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_almacen").cast(DecimalType(24,10)),
        col("imp_vvdd_cf").cast(DecimalType(24,10)),
        col("imp_vvdd_amort").cast(DecimalType(24,10)),
        col("imp_vvdd_provis_recur").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_result").cast(DecimalType(24,10)),
        col("imp_vvdd_result_op").cast(DecimalType(24,10)),
        col("imp_vvdd_pers").cast(DecimalType(24,10)),
        col("imp_vvdd_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_vvdd_pers_2_otro_cost_pers").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_arrend_can").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_vvdd_sv_ext_2_otros_sv_ext").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_corp").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_vvdd_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_gastos").cast(DecimalType(24,10)),
        col("imp_vvdd_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_vvdd_ing_net").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mat_prim").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_vvdd_mb").cast(DecimalType(24,10)),
        col("imp_vvdd_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_tot").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_unit").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_go_a_reg_diesl").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_bfc_pa_serv_corp").cast(DecimalType(24,10)),
        col("imp_ees_bfc_pa_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_bfc_pa_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_ees_bfc_pa_result_op").cast(DecimalType(24,10)),
        col("imp_ees_bfc_pa_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ees_minor_bfc_pa_tot_cf_c").cast(DecimalType(24,10)),
        col("imp_ees_minor_bfc_pa_result_op").cast(DecimalType(24,10)),
        col("imp_ctral_total_bfc_pa_cf_c").cast(DecimalType(24,10)),
        col("imp_pa_ctral_resu_ope").cast(DecimalType(24,10)),
        col("imp_pa_vvdd_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_pa_vvdd_result_op").cast(DecimalType(24,10)),
        col("imp_tot_bfc_pa_serv_corp").cast(DecimalType(24,10)),
        col("imp_tot_pa_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_pa_mm_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_tot_pa_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_tot_pa_result_op").cast(DecimalType(24,10)),
        col("imp_tot_pa_result_op_AJUSTE").cast(DecimalType(24,10)),
        col("imp_tot_pa_result_net_ajust").cast(DecimalType(24,10)),
        col("imp_tot_pa_result_net").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pa_tot_cf_c").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pa_tot_cf_ana").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pa_result_op_ppl").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pa_result_op").cast(DecimalType(24,10)),
        col("imp_2049_imp_patrimonial_seg").cast(DecimalType(24,10)),
        col("imp_2049_imp_comunicacion").cast(DecimalType(24,10)),
        col("imp_2049_imp_techlab").cast(DecimalType(24,10)),
        col("imp_2049_imp_ingenieria").cast(DecimalType(24,10)),
        col("imp_2049_imp_seguros").cast(DecimalType(24,10)),
        col("imp_2049_imp_digitalizacion").cast(DecimalType(24,10)),
        col("imp_2049_imp_sostenibilidad").cast(DecimalType(24,10)),
        col("imp_2049_imp_auditoria").cast(DecimalType(24,10)),
        col("imp_2049_imp_planif_control").cast(DecimalType(24,10)),
        col("imp_2049_imp_ser_corporativos").cast(DecimalType(24,10)),
        col("imp_2049_imp_juridico").cast(DecimalType(24,10)),
        col("imp_2049_imp_compras").cast(DecimalType(24,10)),
        col("imp_2049_imp_econom_admin").cast(DecimalType(24,10)),
        col("imp_2049_imp_financiero").cast(DecimalType(24,10)),
        col("imp_2049_imp_fiscal").cast(DecimalType(24,10)),
        col("imp_2049_imp_impuestos").cast(DecimalType(24,10)),
        col("imp_2049_imp_minoritarios").cast(DecimalType(24,10)),
        col("imp_2049_imp_otros").cast(DecimalType(24,10)),
        col("imp_2049_imp_pyo").cast(DecimalType(24,10)),
        col("imp_2049_imp_resul_espec_adi").cast(DecimalType(24,10)),
        col("imp_2049_imp_serv_globales").cast(DecimalType(24,10)),
        col("imp_2049_imp_ti").cast(DecimalType(24,10)),
        col("imp_2049_imp_ti_as_a_service").cast(DecimalType(24,10)),
        col("imp_2049_imp_efecto_patri_adi").cast(DecimalType(24,10)),
        col("imp_2049_imp_minoritarios_part").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor").cast(DecimalType(24,10)),
        col("imp_ees_ventas_monterra").cast(DecimalType(24,10)),
        col("imp_total_ventas").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_regular").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_premium").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_rg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_regular").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_premium").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_reg_diesel").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_viaj").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_crc").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_e_commerce").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_fidel_global").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_int_cliente").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_mkt_cloud").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_mkt_fid_evt").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_otros_serv").cast(DecimalType(24,10)),
        col("imp_serv_trans_otros_ser_trans").cast(DecimalType(24,10)),
        col("imp_serv_transv_pyc_cliente").cast(DecimalType(24,10)),
        col("imp_serv_transv_pyo_cliente").cast(DecimalType(24,10)),
        col("imp_serv_transv_sost_cliente").cast(DecimalType(24,10)),
        col("imp_ctral_real_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_c_pa_total_cf_analitica").cast(DecimalType(24,10)),
        col("imp_ctral_real_serv_transv_DG").cast(DecimalType(24,10))).where(col("id_escenario") === "2")


        

    val stack = "stack(326,".concat("'1', '6', imp_ctral_mc, '20492049_03', '', '',"  +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_03', '', ''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_01','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_01_02','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_02','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20502050_01','',''," +
        "'1', '9', imp_c_pa_total_cf_analitica, '20492049_03', '', ''," +
        "'1', '10', imp_ctral_amort,'20492049_03', '', ''," +
        //"'4', '25', imp_ctral_otros_gastos,'20492049_03', '', ''," +  Se ha cambiado esta línea por la de abajo. Sustituimos imp_ctral_otros_gastos por imp_ees_bfc_pa_serv_corp
        "'4', '25', imp_ees_bfc_pa_serv_corp,'20492049_03', '', ''," +
        "'1', '11', imp_ctral_provis_recur, '20492049_03', '', ''," +
        "'1', '12', imp_ctral_otros_result, '20492049_03', '', ''," +
        "'1', '15', imp_pa_ctral_resu_ope, '20492049_03', '', ''," +
        "'1', '13', imp_pa_ctral_resu_ope, '20492049_03', '', ''," +
        "'3', '18', imp_ctral_pers_2_otros_cost, '20492049_03', '60', ''," +
        "'3', '18', imp_ctral_pers_2_retrib, '20492049_03', '59', ''," +
        "'3', '19', imp_ctral_serv_ext_2_arrycan, '20492049_03', '61', ''," +
        "'3', '19', imp_ctral_serv_ext_2_mant_rep, '20492049_03', '64', ''," +
        "'3', '19', imp_ctral_sv_ext_2_otr_sv_ext, '20492049_03', '69', ''," +
        "'3', '19', imp_ctral_serv_ext_2_pub_rrpp, '20492049_03', '62', ''," +
        "'3', '19', imp_ctral_serv_ext_2_seg, '20492049_03', '67', ''," +
        "'3', '19', imp_ctral_serv_ext_2_serv_prof, '20492049_03', '65', ''," +
        "'3', '19', imp_ctral_serv_ext_2_sumin, '20492049_03', '63', ''," +
        "'3', '19', imp_ctral_serv_ext_2_trib, '20492049_03', '68', ''," +
        "'3', '19', imp_ctral_serv_ext_2_viajes, '20492049_03', '66', ''," +
        // "'3', '20', imp_ees_bfc_pa_serv_corp, '20492049_03', '', ''," +
        //"'3', '20', imp_ctral_serv_corp, '20492049_03', '', ''," +
        //"'4', '26', imp_tot_bfc_pa_serv_corp,'20492049_03', '',''," +
        "'3', '20', imp_2049_imp_patrimonial_seg, '20492049_03', '75', ''," +
        "'3', '20', imp_2049_imp_pyo,'20492049_03', '76',''," +
        "'3', '20', imp_2049_imp_comunicacion, '20492049_03', '77', ''," +
        "'3', '20', imp_2049_imp_techlab, '20492049_03', '78', ''," +
        "'3', '20', imp_2049_imp_ti,'20492049_03', '79',''," +
        //"'3', '20', imp_2049_imp_ti_as_a_service, '20492049_03', '80', ''," +
        "'3', '20', imp_2049_imp_compras,'20492049_03', '81',''," +
        "'3', '20', imp_2049_imp_juridico,'20492049_03', '82',''," +
        "'3', '20', imp_2049_imp_financiero,'20492049_03', '83',''," +
        "'3', '20', imp_2049_imp_fiscal, '20492049_03', '84', ''," +
        "'3', '20', imp_2049_imp_econom_admin,'20492049_03', '85',''," +
        "'3', '20', imp_2049_imp_ingenieria, '20492049_03', '86', ''," +
        "'3', '20', imp_2049_imp_ser_corporativos, '20492049_03', '87', ''," +
        "'3', '20', imp_2049_imp_serv_globales, '20492049_03', '88', '103'," +
        "'3', '20', imp_2049_imp_digitalizacion, '20492049_03', '88', '104'," +
        "'3', '20', imp_2049_imp_seguros, '20492049_03', '88', '105'," +
        "'3', '20', imp_2049_imp_sostenibilidad, '20492049_03', '88', '106'," +
        "'3', '20', imp_2049_imp_auditoria, '20492049_03', '88', '107'," +
        "'3', '20', imp_2049_imp_planif_control, '20492049_03', '88', '108'," +
        "'3', '20', imp_2049_imp_otros, '20492049_03', '89', ''," +
        // "'3','21', imp_ctral_real_serv_transv_DG, '20492049_03', '', ''," +
        // "'3','22', imp_ctral_real_otros_serv_DG, '20492049_03', '', ''," +
        "'3','21', imp_serv_trans_otros_ser_trans, '20492049_03', '93', ''," +
        "'3','21', imp_serv_transv_pyc_cliente, '20492049_03', '92', ''," +
        "'3','21', imp_serv_transv_pyo_cliente, '20492049_03', '90', ''," +
        "'3','21', imp_serv_transv_sost_cliente, '20492049_03', '91', ''," +
        "'3','22', imp_otros_serv_dg_fidel_global, '20492049_03', '94', ''," +
        "'3','22', imp_otros_serv_dg_crc, '20492049_03', '95', ''," +
        "'3','22', imp_otros_serv_dg_int_cliente, '20492049_03', '96', ''," +
        "'3','22', imp_otros_serv_dg_mkt_fid_evt, '20492049_03', '97', ''," +
        "'3','22', imp_otros_serv_dg_mkt_cloud, '20492049_03', '98', ''," +
        "'3','22', imp_otros_serv_dg_e_commerce, '20492049_03', '99', ''," +
        "'3','22', imp_otros_serv_dg_otros_serv, '20492049_03', '100', ''," +
        "'3', '23', imp_ctral_total_bfc_pa_cf_c, '20492049_03', '', ''," +
        "'4', '27', imp_c_pa_total_cf_analitica, '20492049_03', '', ''," +
        "'1', '1', imp_ees_mayor_Tcamb_MXN_EUR_m, '20492049_01_01', '', ''," +
        "'1', '2', imp_ees_mayor_vta_go_reg_diesl, '20492049_01_01', '40', '100'," +
        "'1', '2', imp_ees_mayor_ventas_prem_92, '20492049_01_01', '41', '102'," +
        "'1', '2', imp_ees_mayor_ventas_reg_87, '20492049_01_01', '41', '101'," +
        "'1', '5', imp_mayor_mc_uni_ees_2_sres_et, '20492049_01_01', '46', ''," +
        "'1', '5', imp_mayo_mc_uni_ees_2_efres_et, '20492049_01_01', '47', ''," +
        "'1', '5', imp_mayor_mc_uni_ees_2_ext_ols, '20492049_01_01', '48', ''," +
        "'1', '5', imp_mayor_mc_uni_ees_2_ext_mon, '20492049_01_01', '49', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_res_est, '20492049_01_01', '47', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_ext_mon, '20492049_01_01', '49', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_ext_ols, '20492049_01_01', '48', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_sin_res, '20492049_01_01', '46', ''," +
        "'1', '9', imp_ees_mayor_pa_tot_cf_ana, '20492049_01_01', '', ''," +
        "'1', '10', imp_ees_mayor_amort, '20492049_01_01', '', ''," +
        "'1', '11', imp_ees_mayor_provis_recur, '20492049_01_01', '', ''," +
        "'1', '12', imp_ees_mayor_otros_result, '20492049_01_01', '', ''," +
        "'1', '13', imp_ees_mayor_pa_result_op_ppl, '20492049_01_01', '', ''," +
        "'1', '14', imp_ees_mayor_result_otras_soc, '20492049_01_01', '54', ''," +
        "'1', '14', imp_ees_result_otras_soc, '20492049_01', '54', ''," +
        "'1', '15', imp_ees_mayor_pa_result_op, '20492049_01_01', '', ''," +
        "'3', '18', imp_ees_mayor_pers_2_otro_cost, '20492049_01_01', '60', ''," +
        "'3', '18', imp_ees_mayor_pers_2_retrib, '20492049_01_01', '59', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_arrycan, '20492049_01_01', '61', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_man_rep, '20492049_01_01', '64', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_otro_sv, '20492049_01_01', '69', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_pb_rrpp, '20492049_01_01', '62', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_seg, '20492049_01_01', '67', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_sv_prof, '20492049_01_01', '65', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_sumin, '20492049_01_01', '63', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_trib, '20492049_01_01', '68', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_viajes, '20492049_01_01', '66', ''," +
        //"'3', '20', imp_ees_bfc_pa_serv_corp, '20492049_01_01', '', ''," +
        "'3', '20', imp_ees_mayor_serv_corp, '20492049_01_01', '', ''," +
    // "'4', '26', imp_ees_mayor_corp, '20492049_01_01', '', ''," +
        "'3', '21', imp_ees_mayor_serv_transv_DG, '20492049_01_01', '', ''," +
        "'3', '22', imp_ees_mayor_otros_serv_DG, '20492049_01_01', '', ''," +
        "'3', '23', imp_ees_mayor_pa_tot_cf_c, '20492049_01_01', '', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_act_prm, '20492049_01_01', '73', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_mante, '20492049_01_01', '70', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_otros, '20492049_01_01', '74', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_sc_volu, '20492049_01_01', '72', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_neotech, '20492049_01_01', '71', ''," +
        "'4', '25', imp_ees_mayor_otros_gastos, '20492049_01_01', '', ''," +
        "'4', '27', imp_ees_mayor_pa_tot_cf_ana, '20492049_01_01', '', ''," +
        "'1', '2', imp_ees_minor_vta_go_reg_diesl, '20492049_01_02', '40', '100'," +
        "'1', '2', imp_ees_minor_ventas_prem_92, '20492049_01_02', '41', '102'," +
        "'1', '2', imp_ees_minor_ventas_reg_87, '20492049_01_02', '41', '101'," +
        "'1', '8', imp_ees_minor_otro_mc_2_gdirec, '20492049_01_02', '50', ''," +
        "'1', '9', imp_ees_minor_bfc_pa_tot_cf_c, '20492049_01_02', '', ''," +
        "'1', '10', imp_ees_minor_amort, '20492049_01_02', '', ''," +
        "'1', '11', imp_ees_minor_provis_recur, '20492049_01_02', '', ''," +
        "'1', '12', imp_ees_minor_otros_result, '20492049_01_02', '', ''," +
        "'1', '15', imp_ees_minor_bfc_pa_result_op, '20492049_01_02', '', ''," +
        "'1', '13', imp_ees_minor_bfc_pa_result_op, '20492049_01_02', '', ''," +
        "'3', '18', imp_ees_minor_pers_2_retrib, '20492049_01_02', '59', ''," +
        "'3', '18', imp_ees_minor_pers_2_otro_cost, '20492049_01_02', '60', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_arrycan, '20492049_01_02', '61', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_pb_rrpp, '20492049_01_02', '62', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_sumin, '20492049_01_02', '63', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_man_rep, '20492049_01_02', '64', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_sv_prof, '20492049_01_02', '65', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_viajes, '20492049_01_02', '66', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_seg, '20492049_01_02', '67', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_trib, '20492049_01_02', '68', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_otro_sv, '20492049_01_02', '69', ''," +
        // "'3', '20', imp_ees_bfc_pa_serv_corp, '20492049_01_02', '', ''," +
        "'3', '20', imp_ees_minor_serv_corp, '20492049_01_02', '', ''," +
        //"'4', '26', imp_ees_minor_corp, '20492049_01_02', '', ''," +
        "'3', '21', imp_ees_minor_serv_transv_DG, '20492049_01_02', '', ''," +
        "'3', '22', imp_ees_minor_otros_serv_DG, '20492049_01_02', '', ''," +
        "'3', '23', imp_ees_minor_bfc_pa_tot_cf_c, '20492049_01_02', '', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_man_rep, '20492049_01_02', '70', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_neotech, '20492049_01_02', '71', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_sc_volu, '20492049_01_02', '72', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_act_prm, '20492049_01_02', '73', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_otro_cf, '20492049_01_02', '74', ''," +
        "'4', '25', imp_ees_minor_otros_gastos, '20492049_01_02', '', ''," +
        "'4', '27', imp_ees_minor_tot_cf_analit, '20492049_01_02', '', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_efres_et, '20492049_01', '47', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_s_res_et, '20492049_01', '46', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_extr_ols, '20492049_01', '48', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_extr_mon, '20492049_01', '49', ''," +
        "'1', '2', imp_ees_ventas_go_a_reg_diesel, '20492049_01', '40', '100'," +
        "'1', '2', imp_ees_ventas_reg_87, '20492049_01', '41', '101'," +
        "'1', '2', imp_ees_ventas_prem_92, '20492049_01', '41', '102'," +
        "'1', '7', imp_ees_mc_ees_2_mc_sn_res_est, '20492049_01', '46', ''," +
        "'1', '7', imp_ees_mc_ees_2_ef_res_estrat, '20492049_01', '47', ''," +
        "'1', '7', imp_ees_mc_ees_2_extram_olstor, '20492049_01', '48', ''," +
        "'1', '7', imp_ees_mc_ees_2_extram_mont, '20492049_01', '49', ''," +
        "'1', '8', imp_ees_otros_mc_2_gdirec_mc, '20492049_01', '50', ''," +
        "'1', '9', imp_ees_tot_cf_analit, '20492049_01', '', ''," +
        "'1', '10', imp_ees_amort, '20492049_01', '', ''," +
        "'1', '11', imp_ees_provis_recur, '20492049_01', '', ''," +
        "'1', '12', imp_ees_otros_result, '20492049_01', '', ''," +
        "'1', '13', imp_ees_bfc_pa_result_op_ppal, '20492049_01', '', ''," +
        "'1', '15', imp_ees_bfc_pa_result_op, '20492049_01', '', ''," +
        "'3', '18', imp_ees_pers_2_otros_cost_pers, '20492049_01', '60', ''," +
        "'3', '18', imp_ees_pers_2_retrib, '20492049_01', '59', ''," +
        "'3', '19', imp_ees_serv_ext_2_arrend_can, '20492049_01', '61', ''," +
        "'3', '19', imp_ees_serv_ext_2_mant_rep, '20492049_01', '64', ''," +
        "'3', '19', imp_ees_sv_ext_2_otros_sv_ext, '20492049_01', '69', ''," +
        "'3', '19', imp_ees_serv_ext_2_pub_rrpp, '20492049_01', '62', ''," +
        "'3', '19', imp_ees_serv_ext_2_seg, '20492049_01', '67', ''," +
        "'3', '19', imp_ees_serv_ext_2_serv_prof, '20492049_01', '65', ''," +
        "'3', '19', imp_ees_serv_ext_2_sumin, '20492049_01', '63', ''," +
        "'3', '19', imp_ees_serv_ext_2_trib, '20492049_01', '68', ''," +
        "'3', '19', imp_ees_serv_ext_2_viajes, '20492049_01', '66', ''," +
        //"'3', '20', imp_ees_bfc_pa_serv_corp, '20492049_01', '', ''," +
        "'3', '20', imp_ees_serv_corp, '20492049_01', '', ''," +
        //"'4', '26', imp_ees_corp, '20492049_01', '', ''," +
        "'3', '21', imp_ees_serv_transv_DG, '20492049_01', '', ''," +
        "'3', '22', imp_ees_otros_serv_DG, '20492049_01', '', ''," +
        "'3', '23', imp_ees_bfc_pa_tot_cf_concep, '20492049_01', '', ''," +
        "'4', '24', imp_ees_cf_ees_2_act_promo, '20492049_01', '73', ''," +
        "'4', '24', imp_ees_cf_ees_2_mant_rep, '20492049_01', '70', ''," +
        "'4', '24', imp_ees_cf_ees_2_otros_cf_ees, '20492049_01', '74', ''," +
        "'4', '24', imp_ees_cf_ees_2_sis_ctrl_volu, '20492049_01', '72', ''," +
        "'4', '24', imp_ees_cf_ees_2_tec_neotech, '20492049_01', '71', ''," +
        "'4', '25', imp_ees_otros_gastos, '20492049_01', '', ''," +
        "'1', '2', imp_tot_ventas_go_a_reg_diesel, '20502050_01', '40', '100'," +
        "'1', '2', imp_tot_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '2', imp_tot_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '4', imp_vvdd_ventas_go_a_reg_diesl, '20502050_01', '40', '100'," +
        "'1', '4', imp_vvdd_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '4', imp_vvdd_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '3', imp_ees_ventas_go_a_reg_diesel, '20502050_01', '40', '100'," +
        "'1', '3', imp_ees_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '3', imp_ees_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_s_res, '20502050_01', '46', ''," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_resest, '20502050_01', '47', ''," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_olstor, '20502050_01', '48', ''," +
        "'1', '5', imp_tot_mm_mc_unit_ees_2_mont, '20502050_01', '49', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_s_res_est, '20502050_01', '46', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_ef_res_est, '20502050_01', '47', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_extram_ols, '20502050_01', '48', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_extram_mon, '20502050_01', '49', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_g_direc, '20502050_01', '50', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_vta_dir, '20502050_01', '51', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_inter, '20502050_01', '52', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_almacen, '20502050_01', '53', ''," +
        "'1', '14', imp_tot_mm_result_otras_soc, '20502050_01', '54', ''," +
        "'1', '13', imp_tot_pa_result_op_ppal, '20502050_01', '', ''," +
        "'1', '15', imp_tot_pa_result_op, '20502050_01', '', ''," +
        "'1', '999', imp_tot_pa_result_op_AJUSTE, '20502050_01', '', ''," +
        "'1', '10', imp_tot_mm_amort, '20502050_01', '', ''," +
        "'1', '11', imp_tot_mm_provis_recur, '20502050_01', '', ''," +
        "'1', '12', imp_tot_mm_otros_result, '20502050_01', '', ''," +
        "'3', '22', imp_tot_mm_otros_serv_DG, '20502050_01', '', ''," +
        "'1', '29', imp_2049_imp_minoritarios_part, '20502050_01', '', ''," +
        "'1', '30', imp_2049_imp_impuestos, '20502050_01', '', ''," +
        "'1', '31', imp_2049_imp_resul_espec_adi, '20502050_01', '', ''," +
        "'1', '32', imp_2049_imp_efecto_patri_adi, '20502050_01', '', ''," +
        "'1', '17', imp_tot_pa_result_net, '20502050_01', '', ''," +
        "'1', '16', imp_tot_pa_result_net_ajust, '20502050_01', '', ''," +
        "'3', '18', imp_tot_mm_pers_2_retrib, '20502050_01', '59', ''," +
        "'3', '18', imp_tot_mm_pers_2_otro_cost, '20502050_01', '60', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_arrycan, '20502050_01', '61', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_pub_rrpp, '20502050_01', '62', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_sumin, '20502050_01', '63', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_mant_rep, '20502050_01', '64', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_sv_prof, '20502050_01', '65', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_viajes, '20502050_01', '66', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_seg, '20502050_01', '67', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_trib, '20502050_01', '68', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_otro_sv, '20502050_01', '69', ''," +
        //"'3', '21', imp_tot_mm_serv_transv_DG, '20502050_01', '', ''," +
        "'3', '23', imp_tot_pa_tot_cf_concep, '20502050_01', '', ''," +
        "'1', '9', imp_pa_mm_tot_cf_analit, '20502050_01', '', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_mant_rep, '20502050_01', '70', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_neotech, '20502050_01', '71', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_ctrl_volu, '20502050_01', '72', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_act_promo, '20502050_01', '73', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_otro_cf, '20502050_01', '74', ''," +
    // "'4', '25', imp_tot_mm_otros_gastos, '20502050_01', '', ''," Se ha cambiado esta línea por lo de abajo
        "'4', '25', imp_ees_bfc_pa_serv_corp, '20502050_01', '', ''," +
        "'4', '27', imp_pa_mm_tot_cf_analit, '20502050_01', '', ''," + //Revisar los costes fijos analitica  imp_real_mm_tot_cf_analit
    // "'4', '26', imp_tot_bfc_pa_serv_corp,'20502050_01', '',''," +
        "'3', '20', imp_2049_imp_patrimonial_seg, '20502050_01', '75', ''," +
        "'3', '20', imp_2049_imp_pyo,'20502050_01', '76',''," +
        "'3', '20', imp_2049_imp_comunicacion, '20502050_01', '77', ''," +
        "'3', '20', imp_2049_imp_techlab, '20502050_01', '78', ''," +
        "'3', '20', imp_2049_imp_ti,'20502050_01', '79',''," +
        //"'3', '20', imp_2049_imp_ti_as_a_service, '20502050_01', '80', ''," +
        "'3', '20', imp_2049_imp_compras,'20502050_01', '81',''," +
        "'3', '20', imp_2049_imp_juridico,'20502050_01', '82',''," +
        "'3', '20', imp_2049_imp_financiero,'20502050_01', '83',''," +
        "'3', '20', imp_2049_imp_fiscal, '20502050_01', '84', ''," +
        "'3', '20', imp_2049_imp_econom_admin,'20502050_01', '85',''," +
        "'3', '20', imp_2049_imp_ingenieria, '20502050_01', '86', ''," +
        "'3', '20', imp_2049_imp_ser_corporativos, '20502050_01', '87', ''," +
        "'3', '20', imp_2049_imp_serv_globales, '20502050_01', '88', '103'," +
        "'3', '20', imp_2049_imp_digitalizacion, '20502050_01', '88', '104'," +
        "'3', '20', imp_2049_imp_seguros, '20502050_01', '88', '105'," +
        "'3', '20', imp_2049_imp_sostenibilidad, '20502050_01', '88', '106'," +
        "'3', '20', imp_2049_imp_auditoria, '20502050_01', '88', '107'," +
        "'3', '20', imp_2049_imp_planif_control, '20502050_01', '88', '108'," +
        "'3', '20', imp_2049_imp_otros, '20502050_01', '89', ''," +
        "'3','21', imp_serv_trans_otros_ser_trans, '20502050_01', '93', ''," +
        "'3','21', imp_serv_transv_pyc_cliente, '20502050_01', '92', ''," +
        "'3','21', imp_serv_transv_pyo_cliente, '20502050_01', '90', ''," +
        "'3','21', imp_serv_transv_sost_cliente, '20502050_01', '91', ''," +
        "'3','22', imp_otros_serv_dg_fidel_global, '20502050_01', '94', ''," +
        "'3','22', imp_otros_serv_dg_crc, '20502050_01', '95', ''," +
        "'3','22', imp_otros_serv_dg_int_cliente, '20502050_01', '96', ''," +
        "'3','22', imp_otros_serv_dg_mkt_fid_evt, '20502050_01', '97', ''," +
        "'3','22', imp_otros_serv_dg_mkt_cloud, '20502050_01', '98', ''," +
        "'3','22', imp_otros_serv_dg_e_commerce, '20502050_01', '99', ''," +
        "'3','22', imp_otros_serv_dg_otros_serv, '20502050_01', '100', ''," +
        "'1', '2', imp_vvdd_ventas_go_a_reg_diesl, '20492049_02', '40', '100'," +
        "'1', '2', imp_vvdd_ventas_reg_87, '20492049_02', '41', '101'," +
        "'1', '2', imp_vvdd_ventas_prem_92, '20492049_02', '41', '102'," +
        "'1', '8', imp_vvdd_otros_mc_2_vta_direc, '20492049_02', '51', ''," +
        "'1', '8', imp_vvdd_otros_mc_2_interterm, '20492049_02', '52', ''," +
        "'1', '8', imp_vvdd_otros_mc_2_almacen, '20492049_02', '53', ''," +
        "'1', '9', imp_vvdd_tot_cf_analit, '20492049_02', '', ''," +
        "'1', '10',  imp_vvdd_amort, '20492049_02', '', ''," +
        "'1', '11', imp_vvdd_provis_recur, '20492049_02', '', ''," +
        "'1', '12', imp_vvdd_otros_result, '20492049_02', '', ''," +
        "'1', '15', imp_pa_vvdd_result_op, '20492049_02', '', ''," +
        "'1', '13', imp_pa_vvdd_result_op, '20492049_02', '', ''," +
        "'3', '18', imp_vvdd_pers_2_retrib, '20492049_02', '59', ''," +
        "'3', '18', imp_vvdd_pers_2_otro_cost_pers, '20492049_02', '60', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_arrend_can, '20492049_02', '61', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_pub_rrpp, '20492049_02', '62', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_sumin, '20492049_02', '63', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_mant_rep, '20492049_02', '64', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_serv_prof, '20492049_02', '65', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_viajes, '20492049_02', '66', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_seg, '20492049_02', '67', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_trib, '20492049_02', '68', ''," +
        "'3', '19', imp_vvdd_sv_ext_2_otros_sv_ext, '20492049_02', '69', ''," +
        // "'3', '20', imp_ees_bfc_pa_serv_corp, '20492049_02', '', ''," +
        "'3', '20', imp_vvdd_serv_corp, '20492049_02', '', ''," +
        "'3', '21', imp_vvdd_serv_transv_DG, '20492049_02', '', ''," +
        "'3', '22', imp_vvdd_otros_serv_DG, '20492049_02', '', ''," +
        "'3', '23', imp_pa_vvdd_tot_cf_concep, '20492049_02', '', ''," +
        "'4', '25', imp_vvdd_otros_gastos, '20492049_02', '', ''," +
        "'4', '27', imp_vvdd_tot_cf_analit, '20492049_02', '', ''," +
        "'1', '28', imp_ees_ventas, '20492049_01', '46',''," +
        "'1', '28', imp_ees_ventas, '20492049_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20492049_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20492049_01','49',''," +
        "'1', '28', imp_ees_mayor_ventas, '20492049_01_01', '46',''," +
        "'1', '28', imp_ees_mayor_ventas, '20492049_01_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20492049_01_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20492049_01_01', '49',''," +
        "'1', '28', imp_total_ventas, '20502050_01', '46',''," +
        "'1', '28', imp_total_ventas, '20502050_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20502050_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20502050_01', '49',''," +
        "'4', '33', imp_ctral_cf_est_2_pers, '20492049_03', '101', ''," +
        "'4', '33', imp_ctral_cf_est_2_viaj, '20492049_03', '102', ''," +
        "'4', '33', imp_ctral_cf_est_2_com_rrpp, '20492049_03', '103', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_prof, '20492049_03', '104', ''," +
        "'4', '33', imp_ctral_cf_est_2_primas_seg, '20492049_03', '105', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_banc, '20492049_03', '106', ''," +
        "'4', '33', imp_ctral_cf_est_2_pub_rrpp, '20492049_03', '107', ''," +
        "'4', '33', imp_ctral_cf_est_2_sumin, '20492049_03', '108', ''," +
        "'4', '33', imp_ctral_cf_est_2_otros_serv, '20492049_03', '109', ''," +
        "'4', '33', imp_ctral_cf_est_2_pers, '20502050_01', '101', ''," +
        "'4', '33', imp_ctral_cf_est_2_viaj, '20502050_01', '102', ''," +
        "'4', '33', imp_ctral_cf_est_2_com_rrpp, '20502050_01', '103', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_prof, '20502050_01', '104', ''," +
        "'4', '33', imp_ctral_cf_est_2_primas_seg, '20502050_01', '105', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_banc, '20502050_01', '106', ''," +
        "'4', '33', imp_ctral_cf_est_2_pub_rrpp, '20502050_01', '107', ''," +
        "'4', '33', imp_ctral_cf_est_2_sumin, '20502050_01', '108', ''," +
        "'4', '33', imp_ctral_cf_est_2_otros_serv, '20502050_01', '109', ''," +
        "'4', '27', imp_ees_tot_cf_analit, '20492049_01', '', '')" +
        " as (ID_Informe, ID_concepto, importe, id_negocio, id_dim1, id_dim2)")

    vc_pa_central = df_pa_central
        .select(
            expr(stack)
            ,lit("").as("id_sociedad") //vacia
            ,col("cod_periodo").as("Fecha")
            ,lit("2").as("ID_Agregacion") // todos 2
            ,col("imp_ctral_Tcambio_MXN_EUR_ac").as("TasaAC")
            ,col("imp_ctral_Tcambio_MXN_EUR_m").as("TasaM")
            ,lit("2").as("ID_Escenario")
        ).select("ID_Informe", "ID_concepto", "ID_Negocio", "ID_Sociedad","ID_Agregacion","ID_Dim1" , "ID_Dim2","ID_Escenario","Importe", "Fecha", "TasaAC", "TasaM").filter($"Importe".isNotNull).cache
}


# In[22]:


var vc_pa_central1 = spark.emptyDataFrame

if (escenario == "PA-UPA" || escenario == "ALL" || escenario == "PA"){
    vc_pa_central1 = vc_pa_central.select(
        col("ID_Informe").cast(IntegerType),
        col("ID_concepto").cast(IntegerType),
        col("ID_Negocio").cast(VarcharType(30)),
        col("ID_Sociedad").cast(IntegerType),
        col("ID_Agregacion").cast(IntegerType),
        col("ID_Dim1").cast(IntegerType),
        col("ID_Dim2").cast(IntegerType),
        col("Importe").cast(DecimalType(24,10)),
        col("TasaAC").cast(DecimalType(24,10)),
        col("TasaM").cast(DecimalType(24,10)),
        col("ID_Escenario").cast(IntegerType),
        lit("Pesos").as("val_divisa"),
        col("Fecha").cast(IntegerType))
}


# In[23]:


var vc_est_central = spark.emptyDataFrame

if (escenario == "EST" || escenario == "ALL"){
    var df_est_central = df_kpis.select(
        col("cod_periodo").cast(DecimalType(24,10)),
        col("imp_ctral_amort").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_com_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_otros_serv").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_pers").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_primas_seg").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_serv_banc").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_sumin").cast(DecimalType(24,10)),
        col("imp_ctral_mc").cast(DecimalType(24,10)),
        col("imp_ctral_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ctral_otros_result").cast(DecimalType(24,10)),
        col("imp_ctral_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ctral_pers_2_otros_cost").cast(DecimalType(24,10)),
        col("imp_ctral_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ctral_provis_recur").cast(DecimalType(24,10)),
        col("imp_ctral_serv_corp").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ctral_sv_ext_2_otr_sv_ext").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ctral_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_EUR_MXN").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ctral_pers").cast(DecimalType(24,10)),
        col("imp_ctral_ser_ext").cast(DecimalType(24,10)),
        col("imp_ctral_total_cf_concept").cast(DecimalType(24,10)),
        col("imp_ctral_cost_fijos").cast(DecimalType(24,10)),
        col("imp_ctral_resu_ope").cast(DecimalType(24,10)),
        col("imp_ctral_cost_fij_estruct").cast(DecimalType(24,10)),
        col("imp_ctral_total_cf_analitica").cast(DecimalType(24,10)),
        col("imp_ees_cf").cast(DecimalType(24,10)),
        col("imp_ees_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_ees_result_op").cast(DecimalType(24,10)),
        col("imp_ees_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ees_amort").cast(DecimalType(24,10)),
        col("imp_ees_corp").cast(DecimalType(24,10)),
        col("imp_ees_cost_log_dist_2_transp").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_aditivos").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_desc_comp").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mat_prima").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_ees_coste_canal").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_act_promo").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_otros_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_sis_ctrl_volu").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_tec_neotech").cast(DecimalType(24,10)),
        // col("imp_ees_cu").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_gna_autom").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_go_a_reg_diesel").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_go_ter").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_gna_auto_go_a_ter").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_prem_92").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_reg_87").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_tot_mm").cast(DecimalType(24,10)),
        col("imp_ees_EBIT_CCS_JV_mar_cortes").cast(DecimalType(24,10)),
        col("imp_ees_extram_monterra").cast(DecimalType(24,10)),
        col("imp_ees_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_IEPS").cast(DecimalType(24,10)),
        col("imp_ees_ing_brut").cast(DecimalType(24,10)),
        col("imp_ees_mb_sin_des_ing_c_prima").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_extram_mont").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_mc_sn_res_est").cast(DecimalType(24,10)),
        // col("imp_ees_num_ees_repsol").cast(DecimalType(24,10)),
        // col("imp_ees_num_ees_mercado").cast(DecimalType(24,10)),
        col("imp_ees_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc_2_gdirec_mc").cast(DecimalType(24,10)),
        col("imp_ees_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_pers_2_otros_cost_pers").cast(DecimalType(24,10)),
        col("imp_ees_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_ees_serv_corp").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_arrend_can").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ees_sv_ext_2_otros_sv_ext").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_tipo_cambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_tipo_cambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ees_ventas_go_a_reg_diesel").cast(DecimalType(24,10)),
        col("imp_ees_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_s_res_et").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_efres_et").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_extr_ols").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_extr_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_amort").cast(DecimalType(24,10)),
        col("imp_ees_mayor_corp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_act_prm").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_mante").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_otros").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_sc_volu").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_ees_mayor_EBIT_CCS_JV_cort").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_res_est").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_ext_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_ext_ols").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_sin_res").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_mayor_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_corp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_pb_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_mayor_Tcamb_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_mayor_Tcamb_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ees_mayor_vta_go_reg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_unit_ees").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_sres_et").cast(DecimalType(24,10)),
        col("imp_mayo_mc_uni_ees_2_efres_et").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_ext_ols").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_ext_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext").cast(DecimalType(24,10)),
        col("imp_ees_mayor_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_op").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_mayor_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas").cast(DecimalType(24,10)),
        col("imp_ees_minor_vta_go_reg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_mc").cast(DecimalType(24,10)),
        col("imp_ees_minor_otro_mc_2_gdirec").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf").cast(DecimalType(24,10)),
        col("imp_ees_minor_amort").cast(DecimalType(24,10)),
        col("imp_ees_minor_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_minor_result_op").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext").cast(DecimalType(24,10)),
        col("imp_ees_minor_corp").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_pb_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_minor_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_sc_volu").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_act_prm").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_otro_cf").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_minor_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_tot_mm_ing_net").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_m_prim").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_adt").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_desc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_logist_dist").cast(DecimalType(24,10)),
        col("imp_tot_mm_cost_log_dist_2_tsp").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_canal").cast(DecimalType(24,10)),
        col("imp_tot_mm_extram_olstor").cast(DecimalType(24,10)),
        col("imp_tot_mm_extram_monterra").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb_sin_desc_cv").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_unit").cast(DecimalType(24,10)),
        //col("imp_tot_mm_tipo_cambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_tot_mm_ventas_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_ees_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_vvdd_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_cr").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_ees_cr").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_vvdd_cr").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas").cast(DecimalType(24,10)),
        col("imp_ees_ventas").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_unit_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_s_res").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_resest").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_olstor").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_unit_ees_2_mont").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_s_res_est").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_ef_res_est").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_extram_ols").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_extram_mon").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_g_direc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_vta_dir").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_inter").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_almacen").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf").cast(DecimalType(24,10)),
        col("imp_tot_mm_amort").cast(DecimalType(24,10)),
        col("imp_tot_mm_provis_recur").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_result").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_op").cast(DecimalType(24,10)),
        //col("imp_tot_mm_result_op_2_particip_minor").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_op_2_imp").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net_ajust").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net_ajust_2_result_especif_ddi").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net_ajust_2_ef_patrim_ddi").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_tot_mm_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_ctrl_volu").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_act_promo").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_otro_cf").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_pers").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_viajes").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_com_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_prima_seg").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_sv_banc_s").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_sumin").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_otros_sv").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_gastos").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_corp").cast(DecimalType(24,10)),
        col("imp_tot_mm_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ctral_total_cf_an_upa_est").cast(DecimalType(24,10)),
        col("imp_tot_mm_tot_cf_an_upa_est").cast(DecimalType(24,10)),
        col("imp_tot_ventas_go_a_reg_diesel").cast(DecimalType(24,10)),
        col("imp_tot_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_tot_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_vta_direc").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_interterm").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_almacen").cast(DecimalType(24,10)),
        col("imp_vvdd_cf").cast(DecimalType(24,10)),
        col("imp_vvdd_amort").cast(DecimalType(24,10)),
        col("imp_vvdd_provis_recur").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_result").cast(DecimalType(24,10)),
        col("imp_vvdd_result_op").cast(DecimalType(24,10)),
        col("imp_vvdd_pers").cast(DecimalType(24,10)),
        col("imp_vvdd_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_vvdd_pers_2_otro_cost_pers").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_arrend_can").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_vvdd_sv_ext_2_otros_sv_ext").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_corp").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_vvdd_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_gastos").cast(DecimalType(24,10)),
        col("imp_vvdd_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_vvdd_ing_net").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mat_prim").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_vvdd_mb").cast(DecimalType(24,10)),
        col("imp_vvdd_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_tot").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_unit").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_go_a_reg_diesl").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor").cast(DecimalType(24,10)),
        col("imp_ees_ventas_monterra").cast(DecimalType(24,10)),
        col("imp_total_ventas").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_regular").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_premium").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_rg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_regular").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_premium").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_reg_diesel").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_viaj").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_corp").cast(DecimalType(24,10))).where(col("id_escenario") === "1")



        
    val stack = "stack(254,".concat("'1', '6', imp_ctral_mc, '20492049_03', '', '',"  +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_03', '', ''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_01','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_01_01','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_01_02','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_02','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20502050_01','',''," +
        "'1', '9', imp_ctral_total_cf_analitica, '20492049_03', '', ''," +
        "'4', '25', imp_ctral_otros_gastos, '20492049_03', '', ''," +
        "'1', '10', imp_ctral_amort,'20492049_03', '', ''," +
        "'1', '11', imp_ctral_provis_recur, '20492049_03', '', ''," +
        "'1', '12', imp_ctral_otros_result, '20492049_03', '', ''," +
        "'1', '15', imp_ctral_resu_ope, '20492049_03', '', ''," +
        "'1', '13', imp_ctral_resu_ope, '20492049_03', '', ''," +
        "'3', '18', imp_ctral_pers_2_otros_cost, '20492049_03', '60', ''," +
        "'3', '18', imp_ctral_pers_2_retrib, '20492049_03', '59', ''," +
        "'3', '19', imp_ctral_serv_ext_2_arrycan, '20492049_03', '61', ''," +
        "'3', '19', imp_ctral_serv_ext_2_mant_rep, '20492049_03', '64', ''," +
        "'3', '19', imp_ctral_sv_ext_2_otr_sv_ext, '20492049_03', '69', ''," +
        "'3', '19', imp_ctral_serv_ext_2_pub_rrpp, '20492049_03', '62', ''," +
        "'3', '19', imp_ctral_serv_ext_2_seg, '20492049_03', '67', ''," +
        "'3', '19', imp_ctral_serv_ext_2_serv_prof, '20492049_03', '65', ''," +
        "'3', '19', imp_ctral_serv_ext_2_sumin, '20492049_03', '63', ''," +
        "'3', '19', imp_ctral_serv_ext_2_trib, '20492049_03', '68', ''," +
        "'3', '19', imp_ctral_serv_ext_2_viajes, '20492049_03', '66', ''," +
        "'3', '20', imp_ctral_serv_corp, '20492049_03', '', ''," +
        "'3', '21', imp_ctral_serv_transv_DG, '20492049_03', '', ''," +
        "'3', '22', imp_ctral_otros_serv_DG, '20492049_03', '', ''," +
        "'3', '23', imp_ctral_total_cf_concept, '20492049_03', '', ''," +
        "'4', '27', imp_ctral_total_cf_analitica, '20492049_03', '', ''," +
        "'1', '1', imp_ees_mayor_Tcamb_MXN_EUR_m, '20492049_01_01', '', ''," +
        "'1', '2', imp_ees_mayor_vta_go_reg_diesl, '20492049_01_01', '40', '100'," +
        "'1', '2', imp_ees_mayor_ventas_prem_92, '20492049_01_01', '41', '102'," +
        "'1', '2', imp_ees_mayor_ventas_reg_87, '20492049_01_01', '41', '101'," +
        "'1', '14', imp_ees_mayor_result_otras_soc, '20492049_01_01', '54', ''," +
        "'1', '2', imp_tot_ventas_go_a_reg_diesel, '20502050_01', '40', '100'," +
        "'1', '2', imp_tot_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '2', imp_tot_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '5', imp_mayor_mc_uni_ees_2_sres_et, '20492049_01_01', '46', ''," +
        "'1', '5', imp_mayo_mc_uni_ees_2_efres_et, '20492049_01_01', '47', ''," +
        "'1', '5', imp_mayor_mc_uni_ees_2_ext_ols, '20492049_01_01', '48', ''," +
        "'1', '5', imp_mayor_mc_uni_ees_2_ext_mon, '20492049_01_01', '49', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_res_est, '20492049_01_01', '47', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_ext_mon, '20492049_01_01', '49', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_ext_ols, '20492049_01_01', '48', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_sin_res, '20492049_01_01', '46', ''," +
        "'1', '9', imp_ees_mayor_tot_cf_analit, '20492049_01_01', '', ''," +
        "'1', '10', imp_ees_mayor_amort, '20492049_01_01', '', ''," +
        "'1', '11', imp_ees_mayor_provis_recur, '20492049_01_01', '', ''," +
        "'1', '12', imp_ees_mayor_otros_result, '20492049_01_01', '', ''," +
        "'1', '13', imp_ees_mayor_result_op_ppal, '20492049_01_01', '', ''," +
        "'1', '15', imp_ees_mayor_result_op, '20492049_01_01', '', ''," +
        "'3', '18', imp_ees_mayor_pers_2_otro_cost, '20492049_01_01', '60', ''," +
        "'3', '18', imp_ees_mayor_pers_2_retrib, '20492049_01_01', '59', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_arrycan, '20492049_01_01', '61', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_man_rep, '20492049_01_01', '64', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_otro_sv, '20492049_01_01', '69', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_pb_rrpp, '20492049_01_01', '62', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_seg, '20492049_01_01', '67', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_sv_prof, '20492049_01_01', '65', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_sumin, '20492049_01_01', '63', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_trib, '20492049_01_01', '68', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_viajes, '20492049_01_01', '66', ''," +
        "'3', '20', imp_ees_mayor_serv_corp, '20492049_01_01', '', ''," +
        //"'4', '26', imp_ees_mayor_corp, '20492049_01_01', '', ''," +
        "'3', '21', imp_ees_mayor_serv_transv_DG, '20492049_01_01', '', ''," +
        "'3', '22', imp_ees_mayor_otros_serv_DG, '20492049_01_01', '', ''," +
        "'3', '23', imp_ees_mayor_tot_cf_concep, '20492049_01_01', '', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_act_prm, '20492049_01_01', '73', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_mante, '20492049_01_01', '70', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_otros, '20492049_01_01', '74', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_sc_volu, '20492049_01_01', '72', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_neotech, '20492049_01_01', '71', ''," +
        "'4', '25', imp_ees_mayor_otros_gastos, '20492049_01_01', '', ''," +
        "'4', '27', imp_ees_mayor_tot_cf_analit, '20492049_01_01', '', ''," +
        "'1', '2', imp_ees_minor_vta_go_reg_diesl, '20492049_01_02', '40', '100'," +
        "'1', '2', imp_ees_minor_ventas_prem_92, '20492049_01_02', '41', '102'," +
        "'1', '2', imp_ees_minor_ventas_reg_87, '20492049_01_02', '41', '101'," +
        "'1', '8', imp_ees_minor_otro_mc_2_gdirec, '20492049_01_02', '50', ''," +
        "'1', '9', imp_ees_minor_tot_cf_analit, '20492049_01_02', '', ''," +
        "'1', '10', imp_ees_minor_amort, '20492049_01_02', '', ''," +
        "'1', '11', imp_ees_minor_provis_recur, '20492049_01_02', '', ''," +
        "'1', '12', imp_ees_minor_otros_result, '20492049_01_02', '', ''," +
        "'1', '15', imp_ees_minor_result_op, '20492049_01_02', '', ''," +
        "'1', '13', imp_ees_minor_result_op, '20492049_01_02', '', ''," +
        "'3', '18', imp_ees_minor_pers_2_retrib, '20492049_01_02', '59', ''," +
        "'3', '18', imp_ees_minor_pers_2_otro_cost, '20492049_01_02', '60', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_arrycan, '20492049_01_02', '61', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_pb_rrpp, '20492049_01_02', '62', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_sumin, '20492049_01_02', '63', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_man_rep, '20492049_01_02', '64', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_sv_prof, '20492049_01_02', '65', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_viajes, '20492049_01_02', '66', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_seg, '20492049_01_02', '67', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_trib, '20492049_01_02', '68', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_otro_sv, '20492049_01_02', '69', ''," +
        "'3', '20', imp_ees_minor_serv_corp, '20492049_01_02', '', ''," +
        //"'4', '26', imp_ees_minor_corp, '20492049_01_02', '', ''," +
        "'3', '21', imp_ees_minor_serv_transv_DG, '20492049_01_02', '', ''," +
        "'3', '22', imp_ees_minor_otros_serv_DG, '20492049_01_02', '', ''," +
        "'3', '23', imp_ees_minor_tot_cf_concep, '20492049_01_02', '', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_man_rep, '20492049_01_02', '70', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_neotech, '20492049_01_02', '71', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_sc_volu, '20492049_01_02', '72', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_act_prm, '20492049_01_02', '73', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_otro_cf, '20492049_01_02', '74', ''," +
        "'4', '25', imp_ees_minor_otros_gastos, '20492049_01_02', '', ''," +
        "'4', '27', imp_ees_minor_tot_cf_analit, '20492049_01_02', '', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_efres_et, '20492049_01', '47', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_s_res_et, '20492049_01', '46', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_extr_ols, '20492049_01', '48', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_extr_mon, '20492049_01', '49', ''," +
        "'1', '2', imp_ees_ventas_go_a_reg_diesel, '20492049_01', '40', '100'," +
        "'1', '2', imp_ees_ventas_reg_87, '20492049_01', '41', '101'," +
        "'1', '2', imp_ees_ventas_prem_92, '20492049_01', '41', '102'," +
        "'1', '7', imp_ees_mc_ees_2_mc_sn_res_est, '20492049_01', '46', ''," +
        "'1', '7', imp_ees_mc_ees_2_ef_res_estrat, '20492049_01', '47', ''," +
        "'1', '7', imp_ees_mc_ees_2_extram_olstor, '20492049_01', '48', ''," +
        "'1', '7', imp_ees_mc_ees_2_extram_mont, '20492049_01', '49', ''," +
        "'1', '8', imp_ees_otros_mc_2_gdirec_mc, '20492049_01', '50', ''," +
        "'1', '9', imp_ees_tot_cf_analit, '20492049_01', '', ''," +
        "'1', '10', imp_ees_amort, '20492049_01', '', ''," +
        "'1', '11', imp_ees_provis_recur, '20492049_01', '', ''," +
        "'1', '12', imp_ees_otros_result, '20492049_01', '', ''," +
        "'1', '14', imp_ees_result_otras_soc, '20492049_01', '54', ''," +
        "'4', '27', imp_ees_tot_cf_analit, '20492049_01', '', ''," +
        "'1', '13', imp_ees_result_op_ppal, '20492049_01', '', ''," +
        "'1', '15', imp_ees_result_op, '20492049_01', '', ''," +
        "'3', '18', imp_ees_pers_2_otros_cost_pers, '20492049_01', '60', ''," +
        "'3', '18', imp_ees_pers_2_retrib, '20492049_01', '59', ''," +
        "'3', '19', imp_ees_serv_ext_2_arrend_can, '20492049_01', '61', ''," +
        "'3', '19', imp_ees_serv_ext_2_mant_rep, '20492049_01', '64', ''," +
        "'3', '19', imp_ees_sv_ext_2_otros_sv_ext, '20492049_01', '69', ''," +
        "'3', '19', imp_ees_serv_ext_2_pub_rrpp, '20492049_01', '62', ''," +
        "'3', '19', imp_ees_serv_ext_2_seg, '20492049_01', '67', ''," +
        "'3', '19', imp_ees_serv_ext_2_serv_prof, '20492049_01', '65', ''," +
        "'3', '19', imp_ees_serv_ext_2_sumin, '20492049_01', '63', ''," +
        "'3', '19', imp_ees_serv_ext_2_trib, '20492049_01', '68', ''," +
        "'3', '19', imp_ees_serv_ext_2_viajes, '20492049_01', '66', ''," +
        "'3', '20', imp_ees_serv_corp, '20492049_01', '', ''," +
        //"'4', '26', imp_ees_corp, '20492049_01', '', ''," +
        "'3', '21', imp_ees_serv_transv_DG, '20492049_01', '', ''," +
        "'3', '22', imp_ees_otros_serv_DG, '20492049_01', '', ''," +
        "'3', '23', imp_ees_tot_cf_concep, '20492049_01', '', ''," +
        "'4', '24', imp_ees_cf_ees_2_act_promo, '20492049_01', '73', ''," +
        "'4', '24', imp_ees_cf_ees_2_mant_rep, '20492049_01', '70', ''," +
        "'4', '24', imp_ees_cf_ees_2_otros_cf_ees, '20492049_01', '74', ''," +
        "'4', '24', imp_ees_cf_ees_2_sis_ctrl_volu, '20492049_01', '72', ''," +
        "'4', '24', imp_ees_cf_ees_2_tec_neotech, '20492049_01', '71', ''," +
        "'4', '25', imp_ees_otros_gastos, '20492049_01', '', ''," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_s_res, '20502050_01', '46', ''," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_resest, '20502050_01', '47', ''," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_olstor, '20502050_01', '48', ''," +
        "'1', '5', imp_tot_mm_mc_unit_ees_2_mont, '20502050_01', '49', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_s_res_est, '20502050_01', '46', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_ef_res_est, '20502050_01', '47', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_extram_ols, '20502050_01', '48', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_extram_mon, '20502050_01', '49', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_g_direc, '20502050_01', '50', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_vta_dir, '20502050_01', '51', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_inter, '20502050_01', '52', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_almacen, '20502050_01', '53', ''," +
        "'1', '13', imp_tot_mm_result_op_ppal, '20502050_01', '', ''," +
        "'1', '15', imp_tot_mm_result_op, '20502050_01', '', ''," +
        "'1', '4', imp_vvdd_ventas_go_a_reg_diesl, '20502050_01', '40', '100'," +
        "'1', '4', imp_vvdd_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '4', imp_vvdd_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '3', imp_ees_ventas_go_a_reg_diesel, '20502050_01', '40', '100'," +
        "'1', '3', imp_ees_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '3', imp_ees_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '14', imp_tot_mm_result_otras_soc, '20502050_01', '54', ''," +
        //"'1', '29', imp_tot_mm_result_op_2_particip_minor, '20502050_01', '', ''," +
        // "'1', '30', imp_tot_mm_result_op_2_imp, '20502050_01', '', ''," +
        // "'1', '31', imp_tot_mm_result_net_ajust_2_result_especif_ddi, '20502050_01', '', ''," +
        // "'1', '32', imp_tot_mm_result_net_ajust_2_ef_patrim_ddi, '20502050_01', '', ''," +
        // "'1', '17', imp_tot_mm_result_net, '20502050_01', '', ''," +
        "'3', '18', imp_tot_mm_pers_2_retrib, '20502050_01', '59', ''," +
        "'3', '18', imp_tot_mm_pers_2_otro_cost, '20502050_01', '60', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_arrycan, '20502050_01', '61', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_pub_rrpp, '20502050_01', '62', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_sumin, '20502050_01', '63', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_mant_rep, '20502050_01', '64', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_sv_prof, '20502050_01', '65', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_viajes, '20502050_01', '66', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_seg, '20502050_01', '67', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_trib, '20502050_01', '68', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_otro_sv, '20502050_01', '69', ''," +
        "'3', '21', imp_tot_mm_serv_transv_DG, '20502050_01', '', ''," +
        "'3', '23', imp_tot_mm_tot_cf_concep, '20502050_01', '', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_mant_rep, '20502050_01', '70', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_neotech, '20502050_01', '71', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_ctrl_volu, '20502050_01', '72', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_act_promo, '20502050_01', '73', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_otro_cf, '20502050_01', '74', ''," +
        "'4', '25', imp_tot_mm_otros_gastos, '20502050_01', '', ''," +
        //"'4', '26', imp_tot_mm_corp, '20502050_01', '', ''," +
        "'3', '20', imp_ctral_serv_corp, '20502050_01', '', ''," +
        "'4', '27', imp_tot_mm_tot_cf_an_upa_est, '20502050_01', '', ''," +
        "'1', '9', imp_tot_mm_tot_cf_an_upa_est, '20502050_01', '', ''," +
        "'1', '10', imp_tot_mm_amort, '20502050_01', '', ''," +
        "'1', '11', imp_tot_mm_provis_recur, '20502050_01', '', ''," +
        "'1', '12', imp_tot_mm_otros_result, '20502050_01', '', ''," +
        "'3', '22', imp_tot_mm_otros_serv_DG, '20502050_01', '', ''," +
        "'1', '2', imp_vvdd_ventas_go_a_reg_diesl, '20492049_02', '40', '100'," +
        "'1', '2', imp_vvdd_ventas_reg_87, '20492049_02', '41', '101'," +
        "'1', '2', imp_vvdd_ventas_prem_92, '20492049_02', '41', '102'," +
        "'1', '8', imp_vvdd_otros_mc_2_vta_direc, '20492049_02', '51', ''," +
        "'1', '8', imp_vvdd_otros_mc_2_interterm, '20492049_02', '52', ''," +
        "'1', '8', imp_vvdd_otros_mc_2_almacen, '20492049_02', '53', ''," +
        "'1', '9', imp_vvdd_tot_cf_analit, '20492049_02', '', ''," +
        "'1', '10',  imp_vvdd_amort, '20492049_02', '', ''," +
        "'1', '11', imp_vvdd_provis_recur, '20492049_02', '', ''," +
        "'1', '12', imp_vvdd_otros_result, '20492049_02', '', ''," +
        "'1', '15', imp_vvdd_result_op, '20492049_02', '', ''," +
        "'1', '13', imp_vvdd_result_op, '20492049_02', '', ''," +
        "'3', '18', imp_vvdd_pers_2_retrib, '20492049_02', '59', ''," +
        "'3', '18', imp_vvdd_pers_2_otro_cost_pers, '20492049_02', '60', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_arrend_can, '20492049_02', '61', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_pub_rrpp, '20492049_02', '62', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_sumin, '20492049_02', '63', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_mant_rep, '20492049_02', '64', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_serv_prof, '20492049_02', '65', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_viajes, '20492049_02', '66', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_seg, '20492049_02', '67', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_trib, '20492049_02', '68', ''," +
        "'3', '19', imp_vvdd_sv_ext_2_otros_sv_ext, '20492049_02', '69', ''," +
        "'3', '20', imp_vvdd_serv_corp, '20492049_02', '', ''," +
        "'3', '21', imp_vvdd_serv_transv_DG, '20492049_02', '', ''," +
        "'3', '22', imp_vvdd_otros_serv_DG, '20492049_02', '', ''," +
        "'3', '23', imp_vvdd_tot_cf_concep, '20492049_02', '', ''," +
        "'4', '25', imp_vvdd_otros_gastos, '20492049_02', '', ''," +
        "'4', '27', imp_vvdd_tot_cf_analit, '20492049_02', '', ''," +
        "'1', '28', imp_ees_ventas, '20492049_01', '46',''," +
        "'1', '28', imp_ees_ventas, '20492049_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20492049_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20492049_01','49',''," +
        "'1', '28', imp_ees_mayor_ventas, '20492049_01_01', '46',''," +
        "'1', '28', imp_ees_mayor_ventas, '20492049_01_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20492049_01_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20492049_01_01', '49',''," +
        "'1', '28', imp_total_ventas, '20502050_01', '46',''," +
        "'1', '28', imp_total_ventas, '20502050_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20502050_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20502050_01', '49',''," +
        "'4', '33', imp_ctral_cf_est_2_pers, '20492049_03', '101', ''," +
        "'4', '33', imp_ctral_cf_est_2_viaj, '20492049_03', '102', ''," +
        "'4', '33', imp_ctral_cf_est_2_com_rrpp, '20492049_03', '103', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_prof, '20492049_03', '104', ''," +
        "'4', '33', imp_ctral_cf_est_2_primas_seg, '20492049_03', '105', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_banc, '20492049_03', '106', ''," +
        "'4', '33', imp_ctral_cf_est_2_pub_rrpp, '20492049_03', '107', ''," +
        "'4', '33', imp_ctral_cf_est_2_sumin, '20492049_03', '108', ''," +
        "'4', '33', imp_ctral_cf_est_2_otros_serv, '20492049_03', '109', ''," +
        "'4', '33', imp_ctral_cf_est_2_pers, '20502050_01', '101', ''," +
        "'4', '33', imp_ctral_cf_est_2_viaj, '20502050_01', '102', ''," +
        "'4', '33', imp_ctral_cf_est_2_com_rrpp, '20502050_01', '103', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_prof, '20502050_01', '104', ''," +
        "'4', '33', imp_ctral_cf_est_2_primas_seg, '20502050_01', '105', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_banc, '20502050_01', '106', ''," +
        "'4', '33', imp_ctral_cf_est_2_pub_rrpp, '20502050_01', '107', ''," +
        "'4', '33', imp_ctral_cf_est_2_sumin, '20502050_01', '108', ''," +
        "'4', '33', imp_ctral_cf_est_2_otros_serv, '20502050_01', '109', '')" +
        " as (ID_Informe, ID_concepto, importe, id_negocio, id_dim1, id_dim2)")

    vc_est_central = df_est_central
        .select(
            expr(stack)
            ,lit("").as("id_sociedad") //vacia
            ,col("cod_periodo").as("Fecha")
            ,lit("2").as("ID_Agregacion") // todos 2
            ,col("imp_ctral_Tcambio_MXN_EUR_ac").as("TasaAC")
            ,col("imp_ctral_Tcambio_MXN_EUR_m").as("TasaM") 
            ,lit("4").as("ID_Escenario")
        ).select("ID_Informe", "ID_concepto", "ID_Negocio", "ID_Sociedad","ID_Agregacion","ID_Dim1" , "ID_Dim2","ID_Escenario","Importe", "Fecha", "TasaAC", "TasaM").filter($"Importe".isNotNull).cache
} 


# In[24]:


var vc_est_central1 = spark.emptyDataFrame

if (escenario == "EST" || escenario == "ALL"){

    vc_est_central1 = vc_est_central.select(
        col("ID_Informe").cast(IntegerType),
        col("ID_concepto").cast(IntegerType),
        col("ID_Negocio").cast(VarcharType(30)),
        col("ID_Sociedad").cast(IntegerType),
        col("ID_Agregacion").cast(IntegerType),
        col("ID_Dim1").cast(IntegerType),
        col("ID_Dim2").cast(IntegerType),
        col("Importe").cast(DecimalType(24,10)),
        col("TasaAC").cast(DecimalType(24,10)),
        col("TasaM").cast(DecimalType(24,10)),
        col("ID_Escenario").cast(IntegerType),
        lit("Pesos").as("val_divisa"),
        col("Fecha").cast(IntegerType))
}


# In[25]:


var vc_real_central = spark.emptyDataFrame

if (escenario == "REAL" || escenario == "ALL"){
    var df_real_central = df_kpis
        .withColumnRenamed("imp_tot_real_result_op_AJUSTE","pre_imp_tot_real_result_op_AJUSTE")
        .withColumn("imp_tot_real_result_op_AJUSTE", col("pre_imp_tot_real_result_op_AJUSTE") * (-1))
        .drop("pre_imp_tot_real_result_op_AJUSTE")
        .select(
        col("cod_periodo").cast(DecimalType(24,10)),
        col("imp_ctral_amort").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_com_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_otros_serv").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_pers").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_primas_seg").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_serv_banc").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_sumin").cast(DecimalType(24,10)),
        col("imp_ctral_mc").cast(DecimalType(24,10)),
        col("imp_ctral_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ctral_otros_result").cast(DecimalType(24,10)),
        col("imp_ctral_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ctral_pers_2_otros_cost").cast(DecimalType(24,10)),
        col("imp_ctral_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ctral_provis_recur").cast(DecimalType(24,10)),
        col("imp_ctral_serv_corp").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ctral_sv_ext_2_otr_sv_ext").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ctral_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_EUR_MXN").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ctral_pers").cast(DecimalType(24,10)),
        col("imp_ctral_ser_ext").cast(DecimalType(24,10)),
        col("imp_ctral_total_cf_concept").cast(DecimalType(24,10)),
        col("imp_ctral_cost_fijos").cast(DecimalType(24,10)),
        col("imp_ctral_resu_ope").cast(DecimalType(24,10)),
        col("imp_ctral_cost_fij_estruct").cast(DecimalType(24,10)),
        col("imp_ctral_total_cf_analitica").cast(DecimalType(24,10)),
        col("imp_ees_cf").cast(DecimalType(24,10)),
        col("imp_ees_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_ees_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ees_amort").cast(DecimalType(24,10)),
        col("imp_ees_corp").cast(DecimalType(24,10)),
        col("imp_ees_cost_log_dist_2_transp").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_aditivos").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_desc_comp").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mat_prima").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_ees_coste_canal").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_act_promo").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_otros_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_sis_ctrl_volu").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_tec_neotech").cast(DecimalType(24,10)),
        // col("imp_ees_cu").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_gna_autom").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_go_a_reg_diesel").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_go_ter").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_gna_auto_go_a_ter").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_prem_92").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_reg_87").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_tot_mm").cast(DecimalType(24,10)),
        col("imp_ees_EBIT_CCS_JV_mar_cortes").cast(DecimalType(24,10)),
        col("imp_ees_extram_monterra").cast(DecimalType(24,10)),
        col("imp_ees_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_IEPS").cast(DecimalType(24,10)),
        col("imp_ees_ing_brut").cast(DecimalType(24,10)),
        col("imp_ees_mb_sin_des_ing_c_prima").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_extram_mont").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_mc_sn_res_est").cast(DecimalType(24,10)),
        // col("imp_ees_num_ees_repsol").cast(DecimalType(24,10)),
        // col("imp_ees_num_ees_mercado").cast(DecimalType(24,10)),
        col("imp_ees_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc_2_gdirec_mc").cast(DecimalType(24,10)),
        col("imp_ees_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_ees_pers_2_otros_cost_pers").cast(DecimalType(24,10)),
        col("imp_ees_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_serv_corp").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_arrend_can").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ees_sv_ext_2_otros_sv_ext").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_tipo_cambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_tipo_cambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ees_ventas_go_a_reg_diesel").cast(DecimalType(24,10)),
        col("imp_ees_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_s_res_et").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_efres_et").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_extr_ols").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_extr_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_amort").cast(DecimalType(24,10)),
        col("imp_ees_mayor_corp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_act_prm").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_mante").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_otros").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_sc_volu").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_ees_mayor_EBIT_CCS_JV_cort").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_res_est").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_ext_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_ext_ols").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_sin_res").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_mayor_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_corp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_pb_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_mayor_Tcamb_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_mayor_Tcamb_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ees_mayor_vta_go_reg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_unit_ees").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_sres_et").cast(DecimalType(24,10)),
        col("imp_mayo_mc_uni_ees_2_efres_et").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_ext_ols").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_ext_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext").cast(DecimalType(24,10)),
        col("imp_ees_mayor_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_op").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_mayor_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas").cast(DecimalType(24,10)),
        col("imp_ees_minor_vta_go_reg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_mc").cast(DecimalType(24,10)),
        col("imp_ees_minor_otro_mc_2_gdirec").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf").cast(DecimalType(24,10)),
        col("imp_ees_minor_amort").cast(DecimalType(24,10)),
        col("imp_ees_minor_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_minor_result_op").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_pb_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_minor_corp").cast(DecimalType(24,10)),
        col("imp_ees_minor_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_sc_volu").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_act_prm").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_otro_cf").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_minor_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_m_prim").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_adt").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_desc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_logist_dist").cast(DecimalType(24,10)),
        col("imp_tot_mm_cost_log_dist_2_tsp").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_canal").cast(DecimalType(24,10)),
        col("imp_tot_mm_extram_olstor").cast(DecimalType(24,10)),
        col("imp_tot_mm_extram_monterra").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb_sin_desc_cv").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_unit").cast(DecimalType(24,10)),
        //col("imp_tot_mm_tipo_cambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_tot_mm_ventas_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_ees_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_vvdd_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_cr").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_ees_cr").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_vvdd_cr").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas").cast(DecimalType(24,10)),
        col("imp_ees_ventas").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_unit_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_s_res").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_resest").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_olstor").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_unit_ees_2_mont").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_s_res_est").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_ef_res_est").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_extram_ols").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_extram_mon").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_g_direc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_vta_dir").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_inter").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_almacen").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf").cast(DecimalType(24,10)),
        col("imp_tot_mm_amort").cast(DecimalType(24,10)),
        col("imp_tot_mm_provis_recur").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_result").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_op").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_op_2_particip_minor").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_op_2_imp").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net_ajust").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net_ajust_2_result_especif_ddi").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net_ajust_2_ef_patrim_ddi").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_tot_mm_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_ctrl_volu").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_act_promo").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_otro_cf").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_pers").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_viajes").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_com_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_prima_seg").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_sv_banc_s").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_sumin").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_otros_sv").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_gastos").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_corp").cast(DecimalType(24,10)),
        col("imp_tot_mm_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_tot_ventas_go_a_reg_diesel").cast(DecimalType(24,10)),
        col("imp_tot_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_tot_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_vta_direc").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_interterm").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_almacen").cast(DecimalType(24,10)),
        col("imp_vvdd_cf").cast(DecimalType(24,10)),
        col("imp_vvdd_amort").cast(DecimalType(24,10)),
        col("imp_vvdd_provis_recur").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_result").cast(DecimalType(24,10)),
        col("imp_vvdd_result_op").cast(DecimalType(24,10)),
        col("imp_vvdd_pers").cast(DecimalType(24,10)),
        col("imp_vvdd_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_vvdd_pers_2_otro_cost_pers").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_arrend_can").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_vvdd_sv_ext_2_otros_sv_ext").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_corp").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_vvdd_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_gastos").cast(DecimalType(24,10)),
        col("imp_vvdd_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_vvdd_ing_net").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mat_prim").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_vvdd_mb").cast(DecimalType(24,10)),
        col("imp_vvdd_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_tot").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_unit").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_go_a_reg_diesl").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_crc").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_e_commerce").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_fidel_global").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_int_cliente").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_mkt_cloud").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_mkt_fid_evt").cast(DecimalType(24,10)),
        col("imp_otros_serv_dg_otros_serv").cast(DecimalType(24,10)),
        col("imp_serv_trans_otros_ser_trans").cast(DecimalType(24,10)),
        col("imp_serv_transv_pyc_cliente").cast(DecimalType(24,10)),
        col("imp_serv_transv_pyo_cliente").cast(DecimalType(24,10)),
        col("imp_serv_transv_sost_cliente").cast(DecimalType(24,10)),
        col("imp_ctral_real_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ctral_real_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_bfc_real_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_bfc_real_result_op_ppl").cast(DecimalType(24,10)),
        col("imp_ees_bfc_real_result_op").cast(DecimalType(24,10)),
        col("imp_ees_bfc_real_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ees_mino_bfc_real_tot_cf_c").cast(DecimalType(24,10)),
        col("imp_ees_minor_bfc_real_resu_op").cast(DecimalType(24,10)),
        col("imp_bfc_impuestos").cast(DecimalType(24,10)),
        col("imp_bfc_resultado_esp_adi").cast(DecimalType(24,10)),
        col("imp_bfc_minoritarios").cast(DecimalType(24,10)),
        col("imp_bfc_otros").cast(DecimalType(24,10)),
        col("imp_bfc_real_serv_corp").cast(DecimalType(24,10)),
        col("imp_ctral_total_bfc_real_cf_c").cast(DecimalType(24,10)),
        col("imp_real_ctral_resu_ope").cast(DecimalType(24,10)),
        col("imp_tot_real_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_real_mm_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_tot_real_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_tot_real_result_op").cast(DecimalType(24,10)),
        col("imp_tot_real_result_op_AJUSTE").cast(DecimalType(24,10)),
        col("imp_tot_real_result_net_ajust").cast(DecimalType(24,10)),
        col("imp_tot_real_result_net").cast(DecimalType(24,10)),
        col("imp_tot_bfc_real_serv_corp").cast(DecimalType(24,10)),
        col("imp_real_vvdd_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_real_vvdd_result_op").cast(DecimalType(24,10)),
        col("imp_ees_mayor_real_tot_cf_c").cast(DecimalType(24,10)),
        col("imp_ees_mayor_real_tot_cf_ana").cast(DecimalType(24,10)),
        col("imp_ees_mayor_real_resu_op_ppl").cast(DecimalType(24,10)),
        col("imp_ees_mayor_real_result_op").cast(DecimalType(24,10)),
        col("imp_ees_bfc_real_serv_corp").cast(DecimalType(24,10)),
        col("imp_bfc_pyo").cast(DecimalType(24,10)),
        col("imp_bfc_ti").cast(DecimalType(24,10)),
        col("imp_2049_imp_patrimonial_seg").cast(DecimalType(24,10)),
        col("imp_2049_imp_comunicacion").cast(DecimalType(24,10)),
        col("imp_2049_imp_techlab").cast(DecimalType(24,10)),
        col("imp_2049_imp_ingenieria").cast(DecimalType(24,10)),
        col("imp_2049_imp_seguros").cast(DecimalType(24,10)),
        col("imp_2049_imp_digitalizacion").cast(DecimalType(24,10)),
        col("imp_2049_imp_sostenibilidad").cast(DecimalType(24,10)),
        col("imp_2049_imp_auditoria").cast(DecimalType(24,10)),
        col("imp_2049_imp_planif_control").cast(DecimalType(24,10)),
        col("imp_2049_imp_ser_corporativos").cast(DecimalType(24,10)),
        col("imp_2049_imp_juridico").cast(DecimalType(24,10)),
        col("imp_2049_imp_compras").cast(DecimalType(24,10)),
        col("imp_2049_imp_econom_admin").cast(DecimalType(24,10)),
        col("imp_2049_imp_financiero").cast(DecimalType(24,10)),
        col("imp_2049_imp_fiscal").cast(DecimalType(24,10)),
        col("imp_2049_imp_impuestos").cast(DecimalType(24,10)),
        col("imp_2049_imp_minoritarios").cast(DecimalType(24,10)),
        col("imp_2049_imp_otros").cast(DecimalType(24,10)),
        col("imp_2049_imp_pyo").cast(DecimalType(24,10)),
        col("imp_2049_imp_resul_espec_adi").cast(DecimalType(24,10)),
        col("imp_2049_imp_serv_globales").cast(DecimalType(24,10)),
        col("imp_2049_imp_ti").cast(DecimalType(24,10)),
        col("imp_2049_imp_ti_as_a_service").cast(DecimalType(24,10)),
        col("imp_2049_imp_efecto_patri_adi").cast(DecimalType(24,10)),
        col("imp_2049_imp_minoritarios_part").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor").cast(DecimalType(24,10)),
        col("imp_ees_ventas_monterra").cast(DecimalType(24,10)),
        col("imp_total_ventas").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_regular").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_premium").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_rg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_regular").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_premium").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_reg_diesel").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_viaj").cast(DecimalType(24,10)),
        col("imp_c_real_total_cf_analitica").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_corp").cast(DecimalType(24,10))).where(col("id_escenario") === "3")

// display(df_real_central.select(col("imp_tot_real_result_op_AJUSTE")))

    val stack = "stack(311,".concat("'1', '6', imp_ctral_mc, '20492049_03', '', '',"  +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_03', '', ''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_01','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_01_02','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_02','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20502050_01','',''," +
        "'1', '9', imp_c_real_total_cf_analitica, '20492049_03', '', ''," +
        "'1', '10', imp_ctral_amort,'20492049_03', '', ''," +
        "'1', '11', imp_ctral_provis_recur, '20492049_03', '', ''," +
        "'1', '12', imp_ctral_otros_result, '20492049_03', '', ''," +
        "'1', '15', imp_real_ctral_resu_ope, '20492049_03', '', '',"+
        "'1', '13', imp_real_ctral_resu_ope, '20492049_03', '', '',"+
        "'3', '18', imp_ctral_pers_2_otros_cost, '20492049_03', '60', ''," +
        "'3', '18', imp_ctral_pers_2_retrib, '20492049_03', '59', ''," +
        "'3', '19', imp_ctral_serv_ext_2_arrycan, '20492049_03', '61', ''," +
        "'3', '19', imp_ctral_serv_ext_2_mant_rep, '20492049_03', '64', ''," +
        "'3', '19', imp_ctral_sv_ext_2_otr_sv_ext, '20492049_03', '69', ''," +
        "'3', '19', imp_ctral_serv_ext_2_pub_rrpp, '20492049_03', '62', ''," +
        "'3', '19', imp_ctral_serv_ext_2_seg, '20492049_03', '67', ''," +
        "'3', '19', imp_ctral_serv_ext_2_serv_prof, '20492049_03', '65', ''," +
        "'3', '19', imp_ctral_serv_ext_2_sumin, '20492049_03', '63', ''," +
        "'3', '19', imp_ctral_serv_ext_2_trib, '20492049_03', '68', ''," +
        "'3', '19', imp_ctral_serv_ext_2_viajes, '20492049_03', '66', ''," +
        //"'4', '25', imp_ctral_otros_gastos, '20492049_03', '', ''," + Se ha cambiado esta linea por la de abajo
        "'4', '25', imp_ees_bfc_real_serv_corp, '20492049_03', '', ''," +
        // "'3', '20', imp_ctral_serv_corp, '20492049_03', '', ''," +
        //"'4','26', imp_tot_bfc_real_serv_corp,'20492049_03', '', ''," +
        "'3', '20', imp_2049_imp_patrimonial_seg, '20492049_03', '75', ''," +
        "'3', '20', imp_2049_imp_pyo,'20492049_03', '76',''," +
        "'3', '20', imp_2049_imp_comunicacion, '20492049_03', '77', ''," +
        "'3', '20', imp_2049_imp_techlab, '20492049_03', '78', ''," +
        "'3', '20', imp_2049_imp_ti,'20492049_03', '79',''," +
        //"'3', '20', imp_2049_imp_ti_as_a_service, '20492049_03', '80', ''," +
        "'3', '20', imp_2049_imp_compras,'20492049_03', '81',''," +
        "'3', '20', imp_2049_imp_juridico,'20492049_03', '82',''," +
        "'3', '20', imp_2049_imp_financiero,'20492049_03', '83',''," +
        "'3', '20', imp_2049_imp_fiscal, '20492049_03', '84', ''," +
        "'3', '20', imp_2049_imp_econom_admin,'20492049_03', '85',''," +
        "'3', '20', imp_2049_imp_ingenieria, '20492049_03', '86', ''," +
        "'3', '20', imp_2049_imp_ser_corporativos, '20492049_03', '87', ''," +
        "'3', '20', imp_2049_imp_serv_globales, '20492049_03', '88', '103'," +
        "'3', '20', imp_2049_imp_digitalizacion, '20492049_03', '88', '104'," +
        "'3', '20', imp_2049_imp_seguros, '20492049_03', '88', '105'," +
        "'3', '20', imp_2049_imp_sostenibilidad, '20492049_03', '88', '106'," +
        "'3', '20', imp_2049_imp_auditoria, '20492049_03', '88', '107'," +
        "'3', '20', imp_2049_imp_planif_control, '20492049_03', '88', '108'," +
        "'3', '20', imp_2049_imp_otros, '20492049_03', '89', ''," +
        //"'3', '21', imp_ctral_serv_transv_DG, '20492049_03', '', ''," +
        //"'3', '22', imp_ctral_otros_serv_DG, '20492049_03', '', ''," +
        "'3','23', imp_ctral_total_bfc_real_cf_c, '20492049_03', '', ''," +
        "'4', '27', imp_c_real_total_cf_analitica, '20492049_03', '', ''," +
        "'1', '1', imp_ees_mayor_Tcamb_MXN_EUR_m, '20492049_01_01', '', ''," +
        "'1', '2', imp_ees_mayor_vta_go_reg_diesl, '20492049_01_01', '40', '100'," +
        "'1', '2', imp_ees_mayor_ventas_prem_92, '20492049_01_01', '41', '102'," +
        "'1', '2', imp_ees_mayor_ventas_reg_87, '20492049_01_01', '41', '101'," +
        "'1', '5', imp_mayor_mc_uni_ees_2_sres_et, '20492049_01_01', '46', ''," +
        "'1', '5', imp_mayo_mc_uni_ees_2_efres_et, '20492049_01_01', '47', ''," +
        "'1', '5', imp_mayor_mc_uni_ees_2_ext_ols, '20492049_01_01', '48', ''," +
        "'1', '5', imp_mayor_mc_uni_ees_2_ext_mon, '20492049_01_01', '49', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_res_est, '20492049_01_01', '47', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_ext_mon, '20492049_01_01', '49', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_ext_ols, '20492049_01_01', '48', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_sin_res, '20492049_01_01', '46', ''," +
        "'1', '9', imp_ees_mayor_real_tot_cf_ana, '20492049_01_01', '', ''," +
        "'1', '10', imp_ees_mayor_amort, '20492049_01_01', '', ''," +
        "'1', '11', imp_ees_mayor_provis_recur, '20492049_01_01', '', ''," +
        "'1', '12', imp_ees_mayor_otros_result, '20492049_01_01', '', ''," +
        "'1', '14', imp_ees_mayor_result_otras_soc, '20492049_01_01', '54', ''," +
        "'1', '14', imp_ees_result_otras_soc, '20492049_01', '54', ''," +
        //"'1', '15', imp_ees_mayor_result_op, '20492049_01_01', '', ''," +
        "'3', '18', imp_ees_mayor_pers_2_otro_cost, '20492049_01_01', '60', ''," +
        "'3', '18', imp_ees_mayor_pers_2_retrib, '20492049_01_01', '59', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_arrycan, '20492049_01_01', '61', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_man_rep, '20492049_01_01', '64', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_otro_sv, '20492049_01_01', '69', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_pb_rrpp, '20492049_01_01', '62', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_seg, '20492049_01_01', '67', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_sv_prof, '20492049_01_01', '65', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_sumin, '20492049_01_01', '63', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_trib, '20492049_01_01', '68', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_viajes, '20492049_01_01', '66', ''," +
        "'3', '20', imp_ees_mayor_serv_corp, '20492049_01_01', '', ''," +
        "'3', '21', imp_ees_mayor_serv_transv_DG, '20492049_01_01', '', ''," +
        "'3', '22', imp_ees_mayor_otros_serv_DG, '20492049_01_01', '', ''," +
        //"'3', '23', imp_ees_mayor_tot_cf_concep, '20492049_01_01', '', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_act_prm, '20492049_01_01', '73', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_mante, '20492049_01_01', '70', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_otros, '20492049_01_01', '74', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_sc_volu, '20492049_01_01', '72', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_neotech, '20492049_01_01', '71', ''," +
        "'4', '25', imp_ees_mayor_otros_gastos, '20492049_01_01', '', ''," +
        //"'4', '26', imp_ees_mayor_corp, '20492049_01_01', '', ''," +
        //"'4', '27', imp_ees_mayor_tot_cf_analit, '20492049_01_01', '', ''," +
        "'3', '23', imp_ees_mayor_real_tot_cf_c, '20492049_01_01', '', ''," +
        "'4', '27', imp_ees_mayor_real_tot_cf_ana, '20492049_01_01', '', ''," +
        "'1', '13', imp_ees_mayor_real_resu_op_ppl, '20492049_01_01', '', ''," +
        "'1', '15', imp_ees_mayor_real_result_op, '20492049_01_01', '', ''," +
        "'1', '2', imp_ees_minor_vta_go_reg_diesl, '20492049_01_02', '40', '100'," +
        "'1', '2', imp_ees_minor_ventas_prem_92, '20492049_01_02', '41', '102'," +
        "'1', '2', imp_ees_minor_ventas_reg_87, '20492049_01_02', '41', '101'," +
        "'1', '8', imp_ees_minor_otro_mc_2_gdirec, '20492049_01_02', '50', ''," +
        "'1', '9', imp_ees_minor_tot_cf_analit, '20492049_01_02', '', ''," +
        "'1', '10', imp_ees_minor_amort, '20492049_01_02', '', ''," +
        "'1', '11', imp_ees_minor_provis_recur, '20492049_01_02', '', ''," +
        "'1', '12', imp_ees_minor_otros_result, '20492049_01_02', '', ''," +
        "'1', '15', imp_ees_minor_bfc_real_resu_op, '20492049_01_02', '', ''," +
        "'1', '13', imp_ees_minor_bfc_real_resu_op, '20492049_01_02', '', ''," +
        "'3', '18', imp_ees_minor_pers_2_retrib, '20492049_01_02', '59', ''," +
        "'3', '18', imp_ees_minor_pers_2_otro_cost, '20492049_01_02', '60', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_arrycan, '20492049_01_02', '61', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_pb_rrpp, '20492049_01_02', '62', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_sumin, '20492049_01_02', '63', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_man_rep, '20492049_01_02', '64', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_sv_prof, '20492049_01_02', '65', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_viajes, '20492049_01_02', '66', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_seg, '20492049_01_02', '67', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_trib, '20492049_01_02', '68', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_otro_sv, '20492049_01_02', '69', ''," +
    // "'4', '26', imp_ees_minor_corp, '20492049_01_02', '', ''," +
        "'3', '21', imp_ees_minor_serv_transv_DG, '20492049_01_02', '', ''," +
        "'3', '22', imp_ees_minor_otros_serv_DG, '20492049_01_02', '', ''," +
        "'3', '23', imp_ees_mino_bfc_real_tot_cf_c, '20492049_01_02', '', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_man_rep, '20492049_01_02', '70', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_neotech, '20492049_01_02', '71', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_sc_volu, '20492049_01_02', '72', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_act_prm, '20492049_01_02', '73', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_otro_cf, '20492049_01_02', '74', ''," +
        "'4', '25', imp_ees_minor_otros_gastos, '20492049_01_02', '', ''," +
        "'4', '27', imp_ees_minor_tot_cf_analit, '20492049_01_02', '', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_efres_et, '20492049_01', '47', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_s_res_et, '20492049_01', '46', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_extr_ols, '20492049_01', '48', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_extr_mon, '20492049_01', '49', ''," +
        "'1', '2', imp_ees_ventas_go_a_reg_diesel, '20492049_01', '40', '100'," +
        "'1', '2', imp_ees_ventas_reg_87, '20492049_01', '41', '101'," +
        "'1', '2', imp_ees_ventas_prem_92, '20492049_01', '41', '102'," +
        "'1', '7', imp_ees_mc_ees_2_mc_sn_res_est, '20492049_01', '46', ''," +
        "'1', '7', imp_ees_mc_ees_2_ef_res_estrat, '20492049_01', '47', ''," +
        "'1', '7', imp_ees_mc_ees_2_extram_olstor, '20492049_01', '48', ''," +
        "'1', '7', imp_ees_mc_ees_2_extram_mont, '20492049_01', '49', ''," +
        "'1', '8', imp_ees_otros_mc_2_gdirec_mc, '20492049_01', '50', ''," +
        "'1', '9', imp_ees_bfc_real_tot_cf_analit, '20492049_01', '', ''," +
        "'1', '10', imp_ees_amort, '20492049_01', '', ''," +
        "'1', '11', imp_ees_provis_recur, '20492049_01', '', ''," +
        "'1', '12', imp_ees_otros_result, '20492049_01', '', ''," +
        "'1', '13', imp_ees_bfc_real_result_op_ppl, '20492049_01', '', ''," +
        "'1', '15', imp_ees_bfc_real_result_op, '20492049_01', '', ''," +
        "'3', '18', imp_ees_pers_2_otros_cost_pers, '20492049_01', '60', ''," +
        "'3', '18', imp_ees_pers_2_retrib, '20492049_01', '59', ''," +
        "'3', '19', imp_ees_serv_ext_2_arrend_can, '20492049_01', '61', ''," +
        "'3', '19', imp_ees_serv_ext_2_mant_rep, '20492049_01', '64', ''," +
        "'3', '19', imp_ees_sv_ext_2_otros_sv_ext, '20492049_01', '69', ''," +
        "'3', '19', imp_ees_serv_ext_2_pub_rrpp, '20492049_01', '62', ''," +
        "'3', '19', imp_ees_serv_ext_2_seg, '20492049_01', '67', ''," +
        "'3', '19', imp_ees_serv_ext_2_serv_prof, '20492049_01', '65', ''," +
        "'3', '19', imp_ees_serv_ext_2_sumin, '20492049_01', '63', ''," +
        "'3', '19', imp_ees_serv_ext_2_trib, '20492049_01', '68', ''," +
        "'3', '19', imp_ees_serv_ext_2_viajes, '20492049_01', '66', ''," +
    // "'4', '26', imp_ees_corp, '20492049_01', '', ''," +
        "'3', '21', imp_ees_serv_transv_DG, '20492049_01', '', ''," +
        "'3', '22', imp_ees_otros_serv_DG, '20492049_01', '', ''," +
        "'3', '23', imp_ees_bfc_real_tot_cf_concep, '20492049_01', '', ''," +
        "'4', '24', imp_ees_cf_ees_2_act_promo, '20492049_01', '73', ''," +
        "'4', '24', imp_ees_cf_ees_2_mant_rep, '20492049_01', '70', ''," +
        "'4', '24', imp_ees_cf_ees_2_otros_cf_ees, '20492049_01', '74', ''," +
        "'4', '24', imp_ees_cf_ees_2_sis_ctrl_volu, '20492049_01', '72', ''," +
        "'4', '24', imp_ees_cf_ees_2_tec_neotech, '20492049_01', '71', ''," +
        "'4', '25', imp_ees_otros_gastos, '20492049_01', '', ''," +
        "'1', '2', imp_tot_ventas_go_a_reg_diesel, '20502050_01', '40', '100'," +
        "'1', '2', imp_tot_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '2', imp_tot_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_s_res, '20502050_01', '46', ''," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_resest, '20502050_01', '47', ''," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_olstor, '20502050_01', '48', ''," +
        "'1', '5', imp_tot_mm_mc_unit_ees_2_mont, '20502050_01', '49', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_s_res_est, '20502050_01', '46', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_ef_res_est, '20502050_01', '47', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_extram_ols, '20502050_01', '48', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_extram_mon, '20502050_01', '49', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_g_direc, '20502050_01', '50', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_vta_dir, '20502050_01', '51', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_inter, '20502050_01', '52', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_almacen, '20502050_01', '53', ''," +
        "'1', '13', imp_tot_real_result_op_ppal, '20502050_01', '', ''," +
        "'1', '15', imp_tot_real_result_op, '20502050_01', '', ''," +
        "'1', '999', imp_tot_real_result_op_AJUSTE, '20502050_01', '', ''," +
        "'1', '10', imp_tot_mm_amort, '20502050_01', '', ''," +
        "'1', '29', imp_bfc_minoritarios, '20502050_01', '', ''," +
        "'1', '30', imp_bfc_impuestos, '20502050_01', '', ''," +
        "'1', '31', imp_bfc_resultado_esp_adi, '20502050_01', '', ''," +
        "'1', '16', imp_tot_real_result_net_ajust, '20502050_01', '', ''," +
        "'1', '14', imp_tot_mm_result_otras_soc, '20502050_01', '54', ''," +
        "'1', '17', imp_tot_real_result_net, '20502050_01', '', ''," +
        "'3', '18', imp_tot_mm_pers_2_retrib, '20502050_01', '59', ''," +
        "'3', '18', imp_tot_mm_pers_2_otro_cost, '20502050_01', '60', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_arrycan, '20502050_01', '61', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_pub_rrpp, '20502050_01', '62', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_sumin, '20502050_01', '63', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_mant_rep, '20502050_01', '64', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_sv_prof, '20502050_01', '65', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_viajes, '20502050_01', '66', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_seg, '20502050_01', '67', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_trib, '20502050_01', '68', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_otro_sv, '20502050_01', '69', ''," +
        //"'3', '21', imp_tot_mm_serv_transv_DG, '20502050_01', '', ''," +
        "'3', '23', imp_tot_real_tot_cf_concep, '20502050_01', '', ''," +
        "'1', '9', imp_real_mm_tot_cf_analit, '20502050_01', '', ''," +
        "'1', '11', imp_tot_mm_provis_recur, '20502050_01', '', ''," +
        "'1', '12', imp_tot_mm_otros_result, '20502050_01', '', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_mant_rep, '20502050_01', '70', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_neotech, '20502050_01', '71', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_ctrl_volu, '20502050_01', '72', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_act_promo, '20502050_01', '73', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_otro_cf, '20502050_01', '74', ''," +
    // "'4', '25', imp_tot_mm_otros_gastos, '20502050_01', '', ''," + Se ha cambiado esta línea por la de
        "'4', '25', imp_ees_bfc_real_serv_corp, '20502050_01', '', ''," +
        "'4', '27', imp_real_mm_tot_cf_analit, '20502050_01', '', ''," +
        "'1', '3', imp_ees_ventas_go_a_reg_diesel, '20502050_01', '40', '100'," +
        "'1', '3', imp_ees_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '3', imp_ees_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '4', imp_vvdd_ventas_go_a_reg_diesl, '20502050_01', '40', '100'," +
        "'1', '4', imp_vvdd_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '4', imp_vvdd_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '2', imp_vvdd_ventas_go_a_reg_diesl, '20492049_02', '40', '100'," +
        "'1', '2', imp_vvdd_ventas_reg_87, '20492049_02', '41', '101'," +
        "'1', '2', imp_vvdd_ventas_prem_92, '20492049_02', '41', '102'," +
        "'1', '8', imp_vvdd_otros_mc_2_vta_direc, '20492049_02', '51', ''," +
        "'1', '8', imp_vvdd_otros_mc_2_interterm, '20492049_02', '52', ''," +
        "'1', '8', imp_vvdd_otros_mc_2_almacen, '20492049_02', '53', ''," +
        "'1', '9', imp_vvdd_tot_cf_analit, '20492049_02', '', ''," +
        "'1', '10',  imp_vvdd_amort, '20492049_02', '', ''," +
        "'1', '11', imp_vvdd_provis_recur, '20492049_02', '', ''," +
        "'1', '12', imp_vvdd_otros_result, '20492049_02', '', ''," +
        //"'1', '15', imp_vvdd_result_op, '20492049_02', '', ''," +
        "'3', '18', imp_vvdd_pers_2_retrib, '20492049_02', '59', ''," +
        "'3', '18', imp_vvdd_pers_2_otro_cost_pers, '20492049_02', '60', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_arrend_can, '20492049_02', '61', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_pub_rrpp, '20492049_02', '62', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_sumin, '20492049_02', '63', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_mant_rep, '20492049_02', '64', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_serv_prof, '20492049_02', '65', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_viajes, '20492049_02', '66', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_seg, '20492049_02', '67', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_trib, '20492049_02', '68', ''," +
        "'3', '19', imp_vvdd_sv_ext_2_otros_sv_ext, '20492049_02', '69', ''," +
        "'3', '20', imp_vvdd_serv_corp, '20492049_02', '', ''," +
        "'3', '21', imp_vvdd_serv_transv_DG, '20492049_02', '', ''," +
        "'3', '22', imp_vvdd_otros_serv_DG, '20492049_02', '', ''," +
        //"'3', '23', imp_vvdd_tot_cf_concep, '20492049_02', '', ''," +
        "'4', '25', imp_vvdd_otros_gastos, '20492049_02', '', ''," +
        "'4', '27', imp_vvdd_tot_cf_analit, '20492049_02', '', ''," +
        "'3', '23', imp_real_vvdd_tot_cf_concep, '20492049_02', '', ''," +
        "'1', '15', imp_real_vvdd_result_op, '20492049_02', '', ''," +
        "'1', '13', imp_real_vvdd_result_op, '20492049_02', '', ''," +
        // "'3','21', imp_ctral_real_serv_transv_DG, '20492049_03', '', ''," +
        // "'3','22', imp_ctral_real_otros_serv_DG, '20492049_03', '', ''," +
        "'3','21', imp_serv_trans_otros_ser_trans, '20502050_01', '93', ''," +
        "'3','21', imp_serv_transv_pyc_cliente, '20502050_01', '92', ''," +
        "'3','21', imp_serv_transv_pyo_cliente, '20502050_01', '90', ''," +
        "'3','21', imp_serv_transv_sost_cliente, '20502050_01', '91', ''," +
        "'3','22', imp_otros_serv_dg_fidel_global, '20502050_01', '94', ''," +
        "'3','22', imp_otros_serv_dg_crc, '20502050_01', '95', ''," +
        "'3','22', imp_otros_serv_dg_int_cliente, '20502050_01', '96', ''," +
        "'3','22', imp_otros_serv_dg_mkt_fid_evt, '20502050_01', '97', ''," +
        "'3','22', imp_otros_serv_dg_mkt_cloud, '20502050_01', '98', ''," +
        "'3','22', imp_otros_serv_dg_e_commerce, '20502050_01', '99', ''," +
        "'3','22', imp_otros_serv_dg_otros_serv, '20502050_01', '100', ''," +
        "'3','20', imp_ees_minor_serv_corp,'20492049_01_02', '', ''," +
        // "'3','20', imp_ees_bfc_real_serv_corp,'20492049_01', '', ''," +
        // "'3','20', imp_ees_bfc_real_serv_corp,'20492049_01_01', '', ''," +
        // "'3','20', imp_ees_bfc_real_serv_corp,'20492049_01_02', '', ''," +
        // "'3','20', imp_bfc_real_serv_corp,'20492049_03', '', ''," +
        "'3', '20', imp_2049_imp_patrimonial_seg, '20502050_01', '75', ''," +
        "'3', '20', imp_2049_imp_pyo,'20502050_01', '76',''," +
        "'3', '20', imp_2049_imp_comunicacion, '20502050_01', '77', ''," +
        "'3', '20', imp_2049_imp_techlab, '20502050_01', '78', ''," +
        "'3', '20', imp_2049_imp_ti,'20502050_01', '79',''," +
        //"'3', '20', imp_2049_imp_ti_as_a_service, '20502050_01', '80', ''," +
        "'3', '20', imp_2049_imp_compras,'20502050_01', '81',''," +
        "'3', '20', imp_2049_imp_juridico,'20502050_01', '82',''," +
        "'3', '20', imp_2049_imp_financiero,'20502050_01', '83',''," +
        "'3', '20', imp_2049_imp_fiscal, '20502050_01', '84', ''," +
        "'3', '20', imp_2049_imp_econom_admin,'20502050_01', '85',''," +
        "'3', '20', imp_2049_imp_ingenieria, '20502050_01', '86', ''," +
        "'3', '20', imp_2049_imp_ser_corporativos, '20502050_01', '87', ''," +
        "'3', '20', imp_2049_imp_serv_globales, '20502050_01', '88', '103'," +
        "'3', '20', imp_2049_imp_digitalizacion, '20502050_01', '88', '104'," +
        "'3', '20', imp_2049_imp_seguros, '20502050_01', '88', '105'," +
        "'3', '20', imp_2049_imp_sostenibilidad, '20502050_01', '88', '106'," +
        "'3', '20', imp_2049_imp_auditoria, '20502050_01', '88', '107'," +
        "'3', '20', imp_2049_imp_planif_control, '20502050_01', '88', '108'," +
        "'3', '20', imp_2049_imp_otros, '20502050_01', '89', ''," +
        //"'4','26', imp_tot_bfc_real_serv_corp,'20502050_01', '', ''," +
        "'1', '28', imp_ees_ventas, '20492049_01', '46',''," +
        "'1', '28', imp_ees_ventas, '20492049_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20492049_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20492049_01','49',''," +
        "'1', '28', imp_ees_mayor_ventas, '20492049_01_01', '46',''," +
        "'1', '28', imp_ees_mayor_ventas, '20492049_01_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20492049_01_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20492049_01_01', '49',''," +
        "'1', '28', imp_total_ventas, '20502050_01', '46',''," +
        "'1', '28', imp_total_ventas, '20502050_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20502050_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20502050_01', '49',''," +
        "'3','21', imp_serv_trans_otros_ser_trans, '20492049_03', '93', ''," +
        "'3','21', imp_serv_transv_pyc_cliente, '20492049_03', '92', ''," +
        "'3','21', imp_serv_transv_pyo_cliente, '20492049_03', '90', ''," +
        "'3','21', imp_serv_transv_sost_cliente, '20492049_03', '91', ''," +
        "'3','22', imp_otros_serv_dg_fidel_global, '20492049_03', '94', ''," +
        "'3','22', imp_otros_serv_dg_crc, '20492049_03', '95', ''," +
        "'3','22', imp_otros_serv_dg_int_cliente, '20492049_03', '96', ''," +
        "'3','22', imp_otros_serv_dg_mkt_fid_evt, '20492049_03', '97', ''," +
        "'3','22', imp_otros_serv_dg_mkt_cloud, '20492049_03', '98', ''," +
        "'3','22', imp_otros_serv_dg_e_commerce, '20492049_03', '99', ''," +
        "'3','22', imp_otros_serv_dg_otros_serv, '20492049_03', '100', ''," +
        "'4', '33', imp_ctral_cf_est_2_pers, '20492049_03', '101', ''," +
        "'4', '33', imp_ctral_cf_est_2_viaj, '20492049_03', '102', ''," +
        "'4', '33', imp_ctral_cf_est_2_com_rrpp, '20492049_03', '103', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_prof, '20492049_03', '104', ''," +
        "'4', '33', imp_ctral_cf_est_2_primas_seg, '20492049_03', '105', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_banc, '20492049_03', '106', ''," +
        "'4', '33', imp_ctral_cf_est_2_pub_rrpp, '20492049_03', '107', ''," +
        "'4', '33', imp_ctral_cf_est_2_sumin, '20492049_03', '108', ''," +
        "'4', '33', imp_ctral_cf_est_2_otros_serv, '20492049_03', '109', ''," +
        "'4', '33', imp_ctral_cf_est_2_pers, '20502050_01', '101', ''," +
        "'4', '33', imp_ctral_cf_est_2_viaj, '20502050_01', '102', ''," +
        "'4', '33', imp_ctral_cf_est_2_com_rrpp, '20502050_01', '103', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_prof, '20502050_01', '104', ''," +
        "'4', '33', imp_ctral_cf_est_2_primas_seg, '20502050_01', '105', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_banc, '20502050_01', '106', ''," +
        "'4', '33', imp_ctral_cf_est_2_pub_rrpp, '20502050_01', '107', ''," +
        "'4', '33', imp_ctral_cf_est_2_sumin, '20502050_01', '108', ''," +
        "'4', '33', imp_ctral_cf_est_2_otros_serv, '20502050_01', '109', ''," +
        "'4', '27', imp_ees_bfc_real_tot_cf_analit, '20492049_01', '', '')" +
        " as (ID_Informe, ID_concepto, importe, id_negocio, id_dim1, id_dim2)")


    vc_real_central = df_real_central
        .select(
            expr(stack)
            ,lit("").as("id_sociedad") //vacia
            ,col("cod_periodo").as("Fecha")
            ,lit("2").as("ID_Agregacion") // todos 2
            ,col("imp_ctral_Tcambio_MXN_EUR_ac").as("TasaAC")
            ,col("imp_ctral_Tcambio_MXN_EUR_m").as("TasaM")
            ,lit("1").as("ID_Escenario")
        ).select("ID_Informe", "ID_concepto", "ID_Negocio", "ID_Sociedad","ID_Agregacion","ID_Dim1" , "ID_Dim2","ID_Escenario","Importe", "Fecha", "TasaAC", "TasaM").filter($"Importe".isNotNull).cache
 }       
    


# In[26]:


var vc_real_central1 = spark.emptyDataFrame

if (escenario == "REAL" || escenario == "ALL"){

    vc_real_central1 = vc_real_central.select(
        col("ID_Informe").cast(IntegerType),
        col("ID_concepto").cast(IntegerType),
        col("ID_Negocio").cast(VarcharType(30)),
        col("ID_Sociedad").cast(IntegerType),
        col("ID_Agregacion").cast(IntegerType),
        col("ID_Dim1").cast(IntegerType),
        col("ID_Dim2").cast(IntegerType),
        col("Importe").cast(DecimalType(24,10)),
        col("TasaAC").cast(DecimalType(24,10)),
        col("TasaM").cast(DecimalType(24,10)),
        col("ID_Escenario").cast(IntegerType),
        lit("Pesos").as("val_divisa"),
        col("Fecha").cast(IntegerType))
}


# In[27]:


var vc_upa_central = spark.emptyDataFrame

if (escenario == "PA-UPA" || escenario == "ALL" || escenario == "UPA"){

    var df_upa_central = df_kpis.select(
        col("cod_periodo").cast(DecimalType(24,10)),
        col("imp_ctral_amort").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_com_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_otros_serv").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_pers").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_primas_seg").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_serv_banc").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_sumin").cast(DecimalType(24,10)),
        col("imp_ctral_mc").cast(DecimalType(24,10)),
        col("imp_ctral_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ctral_otros_result").cast(DecimalType(24,10)),
        col("imp_ctral_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ctral_pers_2_otros_cost").cast(DecimalType(24,10)),
        col("imp_ctral_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ctral_provis_recur").cast(DecimalType(24,10)),
        col("imp_ctral_serv_corp").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ctral_sv_ext_2_otr_sv_ext").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ctral_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ctral_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_EUR_MXN").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ctral_Tcambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ctral_pers").cast(DecimalType(24,10)),
        col("imp_ctral_ser_ext").cast(DecimalType(24,10)),
        col("imp_ctral_total_cf_concept").cast(DecimalType(24,10)),
        col("imp_ctral_cost_fijos").cast(DecimalType(24,10)),
        col("imp_ctral_resu_ope").cast(DecimalType(24,10)),
        col("imp_ctral_cost_fij_estruct").cast(DecimalType(24,10)),
        col("imp_ctral_total_cf_analitica").cast(DecimalType(24,10)),
        col("imp_ctral_total_cf_an_upa_est").cast(DecimalType(24,10)),
        col("imp_tot_mm_tot_cf_an_upa_est").cast(DecimalType(24,10)),
        col("imp_ees_cf").cast(DecimalType(24,10)),
        col("imp_ees_result_op").cast(DecimalType(24,10)),
        col("imp_ees_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_ees_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ees_amort").cast(DecimalType(24,10)),
        col("imp_ees_corp").cast(DecimalType(24,10)),
        col("imp_ees_cost_log_dist_2_transp").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_aditivos").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_desc_comp").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mat_prima").cast(DecimalType(24,10)),
        col("imp_ees_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_ees_coste_canal").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_act_promo").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_otros_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_sis_ctrl_volu").cast(DecimalType(24,10)),
        col("imp_ees_cf_ees_2_tec_neotech").cast(DecimalType(24,10)),
        // col("imp_ees_cu").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_gna_autom").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_go_a_reg_diesel").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_go_ter").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_gna_auto_go_a_ter").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_prem_92").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_reg_87").cast(DecimalType(24,10)),
        // col("imp_ees_cu_2_tot_mm").cast(DecimalType(24,10)),
        col("imp_ees_EBIT_CCS_JV_mar_cortes").cast(DecimalType(24,10)),
        col("imp_ees_extram_monterra").cast(DecimalType(24,10)),
        col("imp_ees_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_IEPS").cast(DecimalType(24,10)),
        col("imp_ees_ing_brut").cast(DecimalType(24,10)),
        col("imp_ees_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_ees_mb_sin_des_ing_c_prima").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_ef_res_estrat").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_extram_mont").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_extram_olstor").cast(DecimalType(24,10)),
        col("imp_ees_mc_ees_2_mc_sn_res_est").cast(DecimalType(24,10)),
        // col("imp_ees_num_ees_repsol").cast(DecimalType(24,10)),
        // col("imp_ees_num_ees_mercado").cast(DecimalType(24,10)),
        col("imp_ees_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_otros_mc_2_gdirec_mc").cast(DecimalType(24,10)),
        col("imp_ees_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_pers_2_otros_cost_pers").cast(DecimalType(24,10)),
        col("imp_ees_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_serv_corp").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_arrend_can").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_ees_sv_ext_2_otros_sv_ext").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_tipo_cambio_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_tipo_cambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ees_ventas_go_a_reg_diesel").cast(DecimalType(24,10)),
        col("imp_ees_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_s_res_et").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_efres_et").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_extr_ols").cast(DecimalType(24,10)),
        col("imp_ees_mc_unit_ees_2_extr_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_amort").cast(DecimalType(24,10)),
        col("imp_ees_mayor_corp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_act_prm").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_mante").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_otros").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_sc_volu").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_ees_mayor_EBIT_CCS_JV_cort").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_res_est").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_ext_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_ext_ols").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees_2_sin_res").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_mayor_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_mayor_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_corp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_pb_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_mayor_sv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_mayor_Tcamb_MXN_EUR_ac").cast(DecimalType(24,10)),
        col("imp_ees_mayor_Tcamb_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_ees_mayor_vta_go_reg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_mayor_ventas").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_ees").cast(DecimalType(24,10)),
        col("imp_ees_mayor_mc_unit_ees").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_sres_et").cast(DecimalType(24,10)),
        col("imp_mayo_mc_uni_ees_2_efres_et").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_ext_ols").cast(DecimalType(24,10)),
        col("imp_mayor_mc_uni_ees_2_ext_mon").cast(DecimalType(24,10)),
        col("imp_ees_mayor_pers").cast(DecimalType(24,10)),
        col("imp_ees_mayor_serv_ext").cast(DecimalType(24,10)),
        col("imp_ees_mayor_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_ees_mayor_result_op").cast(DecimalType(24,10)),
        col("imp_ees_mayor_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_mayor_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas").cast(DecimalType(24,10)),
        col("imp_ees_minor_vta_go_reg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_ees_minor_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_mc").cast(DecimalType(24,10)),
        col("imp_ees_minor_otro_mc_2_gdirec").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf").cast(DecimalType(24,10)),
        col("imp_ees_minor_amort").cast(DecimalType(24,10)),
        col("imp_ees_minor_provis_recur").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_result").cast(DecimalType(24,10)),
        col("imp_ees_minor_result_op").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers").cast(DecimalType(24,10)),
        col("imp_ees_minor_corp").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_ees_minor_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_pb_rrpp").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_ees_minor_sv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_ees_minor_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_man_rep").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_sc_volu").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_act_prm").cast(DecimalType(24,10)),
        col("imp_ees_minor_cf_ees_2_otro_cf").cast(DecimalType(24,10)),
        col("imp_ees_minor_otros_gastos").cast(DecimalType(24,10)),
        col("imp_ees_minor_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_m_prim").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_adt").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_prod_2_desc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_logist_dist").cast(DecimalType(24,10)),
        col("imp_tot_mm_cost_log_dist_2_tsp").cast(DecimalType(24,10)),
        col("imp_tot_mm_coste_canal").cast(DecimalType(24,10)),
        col("imp_tot_mm_extram_olstor").cast(DecimalType(24,10)),
        col("imp_tot_mm_extram_monterra").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_mb_sin_desc_cv").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_tot").cast(DecimalType(24,10)),
        col("imp_tot_mm_cv_unit").cast(DecimalType(24,10)),
        //col("imp_tot_mm_tipo_cambio_MXN_EUR_m").cast(DecimalType(24,10)),
        col("imp_tot_mm_ventas_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_ees_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_vvdd_mvcv").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_cr").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_ees_cr").cast(DecimalType(24,10)),
        //col("imp_tot_mm_ventas_vvdd_cr").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas").cast(DecimalType(24,10)),
        col("imp_ees_ventas").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_unit_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_s_res").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_resest").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_uni_ees_2_olstor").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_unit_ees_2_mont").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_s_res_est").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_ef_res_est").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_extram_ols").cast(DecimalType(24,10)),
        col("imp_tot_mm_mc_ees_2_extram_mon").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_mc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_g_direc").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_vta_dir").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_inter").cast(DecimalType(24,10)),
        col("imp_tot_mm_otro_mc_2_t_almacen").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf").cast(DecimalType(24,10)),
        col("imp_tot_mm_amort").cast(DecimalType(24,10)),
        col("imp_tot_mm_provis_recur").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_result").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_op_ppal").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_otras_soc").cast(DecimalType(24,10)),
        col("imp_ees_EBIT_CCS_JV_mar_cortes").cast(DecimalType(24,10)),
        col("imp_tot_mm_result_op").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_op_2_particip_minor").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_op_2_imp").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net_ajust").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net_ajust_2_result_especif_ddi").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net_ajust_2_ef_patrim_ddi").cast(DecimalType(24,10)),
        // col("imp_tot_mm_result_net").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_tot_mm_pers_2_otro_cost").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_arrycan").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_sv_prof").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_ext_2_otro_sv").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_tot_mm_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_neotech").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_ctrl_volu").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_act_promo").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_ees_2_otro_cf").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_pers").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_viajes").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_com_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_prima_seg").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_sv_banc_s").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_sumin").cast(DecimalType(24,10)),
        col("imp_tot_mm_cf_estr_2_otros_sv").cast(DecimalType(24,10)),
        col("imp_tot_mm_otros_gastos").cast(DecimalType(24,10)),
        col("imp_tot_mm_serv_corp").cast(DecimalType(24,10)),
        col("imp_tot_mm_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_tot_ventas_go_a_reg_diesel").cast(DecimalType(24,10)),
        col("imp_tot_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_tot_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_vta_direc").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_interterm").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_mc_2_almacen").cast(DecimalType(24,10)),
        col("imp_vvdd_cf").cast(DecimalType(24,10)),
        col("imp_vvdd_amort").cast(DecimalType(24,10)),
        col("imp_vvdd_provis_recur").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_result").cast(DecimalType(24,10)),
        col("imp_vvdd_result_op").cast(DecimalType(24,10)),
        col("imp_vvdd_pers").cast(DecimalType(24,10)),
        col("imp_vvdd_pers_2_retrib").cast(DecimalType(24,10)),
        col("imp_vvdd_pers_2_otro_cost_pers").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_arrend_can").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_pub_rrpp").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_sumin").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_mant_rep").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_serv_prof").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_viajes").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_seg").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_ext_2_trib").cast(DecimalType(24,10)),
        col("imp_vvdd_sv_ext_2_otros_sv_ext").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_corp").cast(DecimalType(24,10)),
        col("imp_vvdd_serv_transv_DG").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_serv_DG").cast(DecimalType(24,10)),
        col("imp_vvdd_tot_cf_concep").cast(DecimalType(24,10)),
        col("imp_vvdd_otros_gastos").cast(DecimalType(24,10)),
        col("imp_vvdd_tot_cf_analit").cast(DecimalType(24,10)),
        col("imp_vvdd_ing_net").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mat_prim").cast(DecimalType(24,10)),
        col("imp_vvdd_coste_prod_2_mermas").cast(DecimalType(24,10)),
        col("imp_vvdd_mb").cast(DecimalType(24,10)),
        col("imp_vvdd_mc").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_tot").cast(DecimalType(24,10)),
        col("imp_vvdd_cv_unit").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_go_a_reg_diesl").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_prem_92").cast(DecimalType(24,10)),
        col("imp_vvdd_ventas_reg_87").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor").cast(DecimalType(24,10)),
        col("imp_ees_ventas_monterra").cast(DecimalType(24,10)),
        col("imp_total_ventas").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_regular").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_premium").cast(DecimalType(24,10)),
        col("imp_ees_ventas_olstor_rg_diesl").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_regular").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_premium").cast(DecimalType(24,10)),
        col("imp_ees_ventas_mont_reg_diesel").cast(DecimalType(24,10)),
        col("imp_ctral_cf_est_2_viaj").cast(DecimalType(24,10)),
        col("imp_ees_minor_serv_corp").cast(DecimalType(24,10))).where(col("id_escenario") === "4")



        
    val stack = "stack(254,".concat("'1', '6', imp_ctral_mc, '20492049_03', '', '',"  +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_03', '', ''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_01','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_01_01','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_01_02','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20492049_02','',''," +
        "'1', '1', imp_ctral_Tcambio_MXN_EUR_m, '20502050_01','',''," +
        "'1', '9', imp_ctral_total_cf_analitica, '20492049_03', '', ''," +
        "'1', '10', imp_ctral_amort,'20492049_03', '', ''," +
        "'1', '11', imp_ctral_provis_recur, '20492049_03', '', ''," +
        "'1', '12', imp_ctral_otros_result, '20492049_03', '', ''," +
        "'1', '15', imp_ctral_resu_ope, '20492049_03', '', '',"+
        "'1', '13', imp_ctral_resu_ope, '20492049_03', '', '',"+
        "'3', '18', imp_ctral_pers_2_otros_cost, '20492049_03', '60', ''," +
        "'3', '18', imp_ctral_pers_2_retrib, '20492049_03', '59', ''," +
        "'3', '19', imp_ctral_serv_ext_2_arrycan, '20492049_03', '61', ''," +
        "'3', '19', imp_ctral_serv_ext_2_mant_rep, '20492049_03', '64', ''," +
        "'3', '19', imp_ctral_sv_ext_2_otr_sv_ext, '20492049_03', '69', ''," +
        "'3', '19', imp_ctral_serv_ext_2_pub_rrpp, '20492049_03', '62', ''," +
        "'3', '19', imp_ctral_serv_ext_2_seg, '20492049_03', '67', ''," +
        "'3', '19', imp_ctral_serv_ext_2_serv_prof, '20492049_03', '65', ''," +
        "'3', '19', imp_ctral_serv_ext_2_sumin, '20492049_03', '63', ''," +
        "'3', '19', imp_ctral_serv_ext_2_trib, '20492049_03', '68', ''," +
        "'3', '19', imp_ctral_serv_ext_2_viajes, '20492049_03', '66', ''," +
        "'3', '20', imp_ctral_serv_corp, '20492049_03', '', ''," +
        "'3', '21', imp_ctral_serv_transv_DG, '20492049_03', '', ''," +
        "'3', '22', imp_ctral_otros_serv_DG, '20492049_03', '', ''," +
        "'3','23', imp_ctral_total_cf_concept, '20492049_03', '', ''," +
        "'4','25', imp_ctral_otros_gastos, '20492049_03', '', ''," +
        "'4', '27', imp_ctral_total_cf_analitica, '20492049_03', '', ''," +
        "'1', '1', imp_ees_mayor_Tcamb_MXN_EUR_m, '20492049_01_01', '', ''," +
        "'1', '2', imp_ees_mayor_vta_go_reg_diesl, '20492049_01_01', '40', '100'," +
        "'1', '2', imp_ees_mayor_ventas_prem_92, '20492049_01_01', '41', '102'," +
        "'1', '2', imp_ees_mayor_ventas_reg_87, '20492049_01_01', '41', '101'," +
        "'1', '5', imp_mayor_mc_uni_ees_2_sres_et, '20492049_01_01', '46', ''," +
        "'1', '5', imp_mayo_mc_uni_ees_2_efres_et, '20492049_01_01', '47', ''," +
        "'1', '5', imp_mayor_mc_uni_ees_2_ext_ols, '20492049_01_01', '48', ''," +
        "'1', '5', imp_mayor_mc_uni_ees_2_ext_mon, '20492049_01_01', '49', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_res_est, '20492049_01_01', '47', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_ext_mon, '20492049_01_01', '49', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_ext_ols, '20492049_01_01', '48', ''," +
        "'1', '7', imp_ees_mayor_mc_ees_2_sin_res, '20492049_01_01', '46', ''," +
        "'1', '9', imp_ees_mayor_tot_cf_analit, '20492049_01_01', '', ''," +
        "'1', '10', imp_ees_mayor_amort, '20492049_01_01', '', ''," +
        "'1', '11', imp_ees_mayor_provis_recur, '20492049_01_01', '', ''," +
        "'1', '12', imp_ees_mayor_otros_result, '20492049_01_01', '', ''," +
        "'1', '13', imp_ees_mayor_result_op_ppal, '20492049_01_01', '', ''," +
        "'1', '14', imp_ees_mayor_result_otras_soc, '20492049_01_01', '54', ''," +
        "'1', '15', imp_ees_mayor_result_op, '20492049_01_01', '', ''," +
        "'3', '18', imp_ees_mayor_pers_2_otro_cost, '20492049_01_01', '60', ''," +
        "'3', '18', imp_ees_mayor_pers_2_retrib, '20492049_01_01', '59', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_arrycan, '20492049_01_01', '61', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_man_rep, '20492049_01_01', '64', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_otro_sv, '20492049_01_01', '69', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_pb_rrpp, '20492049_01_01', '62', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_seg, '20492049_01_01', '67', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_sv_prof, '20492049_01_01', '65', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_sumin, '20492049_01_01', '63', ''," +
        "'3', '19', imp_ees_mayor_serv_ext_2_trib, '20492049_01_01', '68', ''," +
        "'3', '19', imp_ees_mayor_sv_ext_2_viajes, '20492049_01_01', '66', ''," +
        "'3', '20', imp_ees_mayor_serv_corp, '20492049_01_01', '', ''," +
    // "'4', '26', imp_ees_mayor_corp, '20492049_01_01', '', ''," +
        "'3', '21', imp_ees_mayor_serv_transv_DG, '20492049_01_01', '', ''," +
        "'3', '22', imp_ees_mayor_otros_serv_DG, '20492049_01_01', '', ''," +
        "'3', '23', imp_ees_mayor_tot_cf_concep, '20492049_01_01', '', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_act_prm, '20492049_01_01', '73', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_mante, '20492049_01_01', '70', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_otros, '20492049_01_01', '74', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_sc_volu, '20492049_01_01', '72', ''," +
        "'4', '24', imp_ees_mayor_cf_ees_2_neotech, '20492049_01_01', '71', ''," +
        "'4', '25', imp_ees_mayor_otros_gastos, '20492049_01_01', '', ''," +
        "'4', '27', imp_ees_mayor_tot_cf_analit, '20492049_01_01', '', ''," +
        "'1', '2', imp_ees_minor_vta_go_reg_diesl, '20492049_01_02', '40', '100'," +
        "'1', '2', imp_ees_minor_ventas_prem_92, '20492049_01_02', '41', '102'," +
        "'1', '2', imp_ees_minor_ventas_reg_87, '20492049_01_02', '41', '101'," +
        "'1', '8', imp_ees_minor_otro_mc_2_gdirec, '20492049_01_02', '50', ''," +
        "'1', '9', imp_ees_minor_tot_cf_analit, '20492049_01_02', '', ''," +
        "'1', '10', imp_ees_minor_amort, '20492049_01_02', '', ''," +
        "'1', '11', imp_ees_minor_provis_recur, '20492049_01_02', '', ''," +
        "'1', '12', imp_ees_minor_otros_result, '20492049_01_02', '', ''," +
        "'1', '15', imp_ees_minor_result_op, '20492049_01_02', '', ''," +
        "'1', '13', imp_ees_minor_result_op, '20492049_01_02', '', ''," +
        "'3', '18', imp_ees_minor_pers_2_retrib, '20492049_01_02', '59', ''," +
        "'3', '18', imp_ees_minor_pers_2_otro_cost, '20492049_01_02', '60', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_arrycan, '20492049_01_02', '61', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_pb_rrpp, '20492049_01_02', '62', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_sumin, '20492049_01_02', '63', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_man_rep, '20492049_01_02', '64', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_sv_prof, '20492049_01_02', '65', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_viajes, '20492049_01_02', '66', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_seg, '20492049_01_02', '67', ''," +
        "'3', '19', imp_ees_minor_serv_ext_2_trib, '20492049_01_02', '68', ''," +
        "'3', '19', imp_ees_minor_sv_ext_2_otro_sv, '20492049_01_02', '69', ''," +
        "'3', '20', imp_ees_minor_serv_corp, '20492049_01_02', '', ''," +
        //"'4', '26', imp_ees_minor_corp, '20492049_01_02', '', ''," +
        "'3', '21', imp_ees_minor_serv_transv_DG, '20492049_01_02', '', ''," +
        "'3', '22', imp_ees_minor_otros_serv_DG, '20492049_01_02', '', ''," +
        "'3', '23', imp_ees_minor_tot_cf_concep, '20492049_01_02', '', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_man_rep, '20492049_01_02', '70', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_neotech, '20492049_01_02', '71', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_sc_volu, '20492049_01_02', '72', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_act_prm, '20492049_01_02', '73', ''," +
        "'4', '24', imp_ees_minor_cf_ees_2_otro_cf, '20492049_01_02', '74', ''," +
        "'4', '25', imp_ees_minor_otros_gastos, '20492049_01_02', '', ''," +
        "'4', '27', imp_ees_minor_tot_cf_analit, '20492049_01_02', '', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_efres_et, '20492049_01', '47', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_s_res_et, '20492049_01', '46', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_extr_ols, '20492049_01', '48', ''," +
        "'1', '5', imp_ees_mc_unit_ees_2_extr_mon, '20492049_01', '49', ''," +
        "'1', '2', imp_ees_ventas_go_a_reg_diesel, '20492049_01', '40', '100'," +
        "'1', '2', imp_ees_ventas_reg_87, '20492049_01', '41', '101'," +
        "'1', '2', imp_ees_ventas_prem_92, '20492049_01', '41', '102'," +
        "'1', '7', imp_ees_mc_ees_2_mc_sn_res_est, '20492049_01', '46', ''," +
        "'1', '7', imp_ees_mc_ees_2_ef_res_estrat, '20492049_01', '47', ''," +
        "'1', '7', imp_ees_mc_ees_2_extram_olstor, '20492049_01', '48', ''," +
        "'1', '7', imp_ees_mc_ees_2_extram_mont, '20492049_01', '49', ''," +
        "'1', '8', imp_ees_otros_mc_2_gdirec_mc, '20492049_01', '50', ''," +
        "'1', '9', imp_ees_tot_cf_analit, '20492049_01', '', ''," +
        "'1', '10', imp_ees_amort, '20492049_01', '', ''," +
        "'1', '11', imp_ees_provis_recur, '20492049_01', '', ''," +
        "'1', '12', imp_ees_otros_result, '20492049_01', '', ''," +
    // "'4', '26', imp_ees_corp, '20492049_01', '', ''," +
        "'1', '15', imp_ees_result_op, '20492049_01', '', ''," +
        "'1', '13', imp_ees_result_op_ppal, '20492049_01', '', ''," +
        "'1', '14', imp_ees_result_otras_soc, '20492049_01', '54', ''," +
        "'3', '18', imp_ees_pers_2_otros_cost_pers, '20492049_01', '60', ''," +
        "'3', '18', imp_ees_pers_2_retrib, '20492049_01', '59', ''," +
        "'3', '19', imp_ees_serv_ext_2_arrend_can, '20492049_01', '61', ''," +
        "'3', '19', imp_ees_serv_ext_2_mant_rep, '20492049_01', '64', ''," +
        "'3', '19', imp_ees_sv_ext_2_otros_sv_ext, '20492049_01', '69', ''," +
        "'3', '19', imp_ees_serv_ext_2_pub_rrpp, '20492049_01', '62', ''," +
        "'3', '19', imp_ees_serv_ext_2_seg, '20492049_01', '67', ''," +
        "'3', '19', imp_ees_serv_ext_2_serv_prof, '20492049_01', '65', ''," +
        "'3', '19', imp_ees_serv_ext_2_sumin, '20492049_01', '63', ''," +
        "'3', '19', imp_ees_serv_ext_2_trib, '20492049_01', '68', ''," +
        "'3', '19', imp_ees_serv_ext_2_viajes, '20492049_01', '66', ''," +
        "'3', '20', imp_ees_serv_corp, '20492049_01', '', ''," +
        "'3', '21', imp_ees_serv_transv_DG, '20492049_01', '', ''," +
        "'3', '22', imp_ees_otros_serv_DG, '20492049_01', '', ''," +
        "'3', '23', imp_ees_tot_cf_concep, '20492049_01', '', ''," +
        "'4', '24', imp_ees_cf_ees_2_act_promo, '20492049_01', '73', ''," +
        "'4', '24', imp_ees_cf_ees_2_mant_rep, '20492049_01', '70', ''," +
        "'4', '24', imp_ees_cf_ees_2_otros_cf_ees, '20492049_01', '74', ''," +
        "'4', '24', imp_ees_cf_ees_2_sis_ctrl_volu, '20492049_01', '72', ''," +
        "'4', '24', imp_ees_cf_ees_2_tec_neotech, '20492049_01', '71', ''," +
        "'4', '25', imp_ees_otros_gastos, '20492049_01', '', ''," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_s_res, '20502050_01', '46', ''," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_resest, '20502050_01', '47', ''," +
        "'1', '5', imp_tot_mm_mc_uni_ees_2_olstor, '20502050_01', '48', ''," +
        "'1', '5', imp_tot_mm_mc_unit_ees_2_mont, '20502050_01', '49', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_s_res_est, '20502050_01', '46', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_ef_res_est, '20502050_01', '47', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_extram_ols, '20502050_01', '48', ''," +
        "'1', '7', imp_tot_mm_mc_ees_2_extram_mon, '20502050_01', '49', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_g_direc, '20502050_01', '50', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_vta_dir, '20502050_01', '51', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_inter, '20502050_01', '52', ''," +
        "'1', '8', imp_tot_mm_otro_mc_2_t_almacen, '20502050_01', '53', ''," +
        "'1', '13', imp_tot_mm_result_op_ppal, '20502050_01', '', ''," +
        "'1', '15', imp_tot_mm_result_op, '20502050_01', '', ''," +
        "'1', '9', imp_tot_mm_tot_cf_an_upa_est, '20502050_01', '', ''," +
        "'1', '10', imp_tot_mm_amort, '20502050_01', '', ''," +
        "'1', '11', imp_tot_mm_provis_recur, '20502050_01', '', ''," +
        "'1', '12', imp_tot_mm_otros_result, '20502050_01', '', ''," +
        "'1', '14', imp_tot_mm_result_otras_soc, '20502050_01', '54', ''," +
        "'3', '22', imp_tot_mm_otros_serv_DG, '20502050_01', '', ''," +
        // "'1', '30', imp_tot_mm_result_op_2_imp, '20502050_01', '', ''," +
        // "'1', '31', imp_tot_mm_result_net_ajust_2_result_especif_ddi, '20502050_01', '', ''," +
        // "'1', '32', imp_tot_mm_result_net_ajust_2_ef_patrim_ddi, '20502050_01', '', ''," +
        // "'1', '17', imp_tot_mm_result_net, '20502050_01', '', ''," +
        "'3', '18', imp_tot_mm_pers_2_retrib, '20502050_01', '59', ''," +
        "'3', '18', imp_tot_mm_pers_2_otro_cost, '20502050_01', '60', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_arrycan, '20502050_01', '61', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_pub_rrpp, '20502050_01', '62', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_sumin, '20502050_01', '63', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_mant_rep, '20502050_01', '64', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_sv_prof, '20502050_01', '65', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_viajes, '20502050_01', '66', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_seg, '20502050_01', '67', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_trib, '20502050_01', '68', ''," +
        "'3', '19', imp_tot_mm_serv_ext_2_otro_sv, '20502050_01', '69', ''," +
        "'3', '21', imp_tot_mm_serv_transv_DG, '20502050_01', '', ''," +
        "'3', '23', imp_tot_mm_tot_cf_concep, '20502050_01', '', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_mant_rep, '20502050_01', '70', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_neotech, '20502050_01', '71', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_ctrl_volu, '20502050_01', '72', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_act_promo, '20502050_01', '73', ''," +
        "'4', '24', imp_tot_mm_cf_ees_2_otro_cf, '20502050_01', '74', ''," +
        "'4', '25', imp_tot_mm_otros_gastos, '20502050_01', '', ''," +
        //"'4', '26', imp_tot_mm_corp, '20502050_01', '', ''," +
        "'3', '20', imp_ctral_serv_corp, '20502050_01', '', ''," +
        "'4', '27', imp_tot_mm_tot_cf_an_upa_est, '20502050_01', '', ''," +
        "'1', '3', imp_ees_ventas_go_a_reg_diesel, '20502050_01', '40', '100'," +
        "'1', '3', imp_ees_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '3', imp_ees_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '2', imp_tot_ventas_go_a_reg_diesel, '20502050_01', '40', '100'," +
        "'1', '2', imp_tot_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '2', imp_tot_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '4', imp_vvdd_ventas_go_a_reg_diesl, '20502050_01', '40', '100'," +
        "'1', '4', imp_vvdd_ventas_reg_87, '20502050_01', '41', '101'," +
        "'1', '4', imp_vvdd_ventas_prem_92, '20502050_01', '41', '102'," +
        "'1', '2', imp_vvdd_ventas_go_a_reg_diesl, '20492049_02', '40', '100'," +
        "'1', '2', imp_vvdd_ventas_reg_87, '20492049_02', '41', '101'," +
        "'1', '2', imp_vvdd_ventas_prem_92, '20492049_02', '41', '102'," +
        "'1', '8', imp_vvdd_otros_mc_2_vta_direc, '20492049_02', '51', ''," +
        "'1', '8', imp_vvdd_otros_mc_2_interterm, '20492049_02', '52', ''," +
        "'1', '8', imp_vvdd_otros_mc_2_almacen, '20492049_02', '53', ''," +
        "'1', '9', imp_vvdd_tot_cf_analit, '20492049_02', '', ''," +
        "'1', '10',  imp_vvdd_amort, '20492049_02', '', ''," +
        "'1', '11', imp_vvdd_provis_recur, '20492049_02', '', ''," +
        "'1', '12', imp_vvdd_otros_result, '20492049_02', '', ''," +
        "'1', '15', imp_vvdd_result_op, '20492049_02', '', ''," +
        "'1', '13', imp_vvdd_result_op, '20492049_02', '', ''," +
        "'3', '18', imp_vvdd_pers_2_retrib, '20492049_02', '59', ''," +
        "'3', '18', imp_vvdd_pers_2_otro_cost_pers, '20492049_02', '60', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_arrend_can, '20492049_02', '61', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_pub_rrpp, '20492049_02', '62', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_sumin, '20492049_02', '63', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_mant_rep, '20492049_02', '64', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_serv_prof, '20492049_02', '65', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_viajes, '20492049_02', '66', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_seg, '20492049_02', '67', ''," +
        "'3', '19', imp_vvdd_serv_ext_2_trib, '20492049_02', '68', ''," +
        "'3', '19', imp_vvdd_sv_ext_2_otros_sv_ext, '20492049_02', '69', ''," +
        "'3', '20', imp_vvdd_serv_corp, '20492049_02', '', ''," +
        "'3', '21', imp_vvdd_serv_transv_DG, '20492049_02', '', ''," +
        "'3', '22', imp_vvdd_otros_serv_DG, '20492049_02', '', ''," +
        "'3', '23', imp_vvdd_tot_cf_concep, '20492049_02', '', ''," +
        "'4', '25', imp_vvdd_otros_gastos, '20492049_02', '', ''," +
        "'4', '27', imp_vvdd_tot_cf_analit, '20492049_02', '', ''," +
        "'1', '28', imp_ees_ventas, '20492049_01', '46',''," +
        "'1', '28', imp_ees_ventas, '20492049_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20492049_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20492049_01','49',''," +
        "'1', '28', imp_ees_mayor_ventas, '20492049_01_01', '46',''," +
        "'1', '28', imp_ees_mayor_ventas, '20492049_01_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20492049_01_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20492049_01_01', '49',''," +
        "'1', '28', imp_total_ventas, '20502050_01', '46',''," +
        "'1', '28', imp_total_ventas, '20502050_01', '47',''," +
        "'1', '28', imp_ees_ventas_olstor, '20502050_01', '48',''," +
        "'1', '28', imp_ees_ventas_monterra, '20502050_01', '49',''," +
        "'4', '33', imp_ctral_cf_est_2_pers, '20492049_03', '101', ''," +
        "'4', '33', imp_ctral_cf_est_2_viaj, '20492049_03', '102', ''," +
        "'4', '33', imp_ctral_cf_est_2_com_rrpp, '20492049_03', '103', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_prof, '20492049_03', '104', ''," +
        "'4', '33', imp_ctral_cf_est_2_primas_seg, '20492049_03', '105', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_banc, '20492049_03', '106', ''," +
        "'4', '33', imp_ctral_cf_est_2_pub_rrpp, '20492049_03', '107', ''," +
        "'4', '33', imp_ctral_cf_est_2_sumin, '20492049_03', '108', ''," +
        "'4', '33', imp_ctral_cf_est_2_otros_serv, '20492049_03', '109', ''," +
        "'4', '33', imp_ctral_cf_est_2_pers, '20502050_01', '101', ''," +
        "'4', '33', imp_ctral_cf_est_2_viaj, '20502050_01', '102', ''," +
        "'4', '33', imp_ctral_cf_est_2_com_rrpp, '20502050_01', '103', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_prof, '20502050_01', '104', ''," +
        "'4', '33', imp_ctral_cf_est_2_primas_seg, '20502050_01', '105', ''," +
        "'4', '33', imp_ctral_cf_est_2_serv_banc, '20502050_01', '106', ''," +
        "'4', '33', imp_ctral_cf_est_2_pub_rrpp, '20502050_01', '107', ''," +
        "'4', '33', imp_ctral_cf_est_2_sumin, '20502050_01', '108', ''," +
        "'4', '33', imp_ctral_cf_est_2_otros_serv, '20502050_01', '109', ''," +
        "'4', '27', imp_ees_tot_cf_analit, '20492049_01', '', '')" +
        " as (ID_Informe, ID_concepto, importe, id_negocio, id_dim1, id_dim2)")



    vc_upa_central = df_upa_central
        .select(
            expr(stack)
            ,lit("").as("id_sociedad") //vacia
            ,col("cod_periodo").as("Fecha")
            ,lit("2").as("ID_Agregacion") // todos 2
            ,col("imp_ctral_Tcambio_MXN_EUR_m").as("TasaAC")
            ,col("imp_ctral_Tcambio_MXN_EUR_m").as("TasaM")
            ,lit("3").as("ID_Escenario")
        ).select("ID_Informe", "ID_concepto", "ID_Negocio", "ID_Sociedad","ID_Agregacion","ID_Dim1" , "ID_Dim2","ID_Escenario","Importe", "Fecha", "TasaAC", "TasaM").filter($"Importe".isNotNull).cache
 }       



# In[28]:


var vc_upa_central1 = spark.emptyDataFrame

if (escenario == "PA-UPA" || escenario == "ALL" || escenario == "UPA"){

    vc_upa_central1 = vc_upa_central.select(
        col("ID_Informe").cast(IntegerType),
        col("ID_concepto").cast(IntegerType),
        col("ID_Negocio").cast(VarcharType(30)),
        col("ID_Sociedad").cast(IntegerType),
        col("ID_Agregacion").cast(IntegerType),
        col("ID_Dim1").cast(IntegerType),
        col("ID_Dim2").cast(IntegerType),
        col("Importe").cast(DecimalType(24,10)),
        col("TasaAC").cast(DecimalType(24,10)),
        col("TasaM").cast(DecimalType(24,10)),
        col("ID_Escenario").cast(IntegerType),
        lit("Pesos").as("val_divisa"),
        col("Fecha").cast(IntegerType))
}


# In[29]:


var vc_final_central = vc_upa_central1.unionByName(vc_pa_central1, true).unionByName(vc_real_central1, true).unionByName(vc_est_central1, true).cache


# In[30]:


var b = vc_final_central.select(
    col("ID_Informe").cast(IntegerType).as("id_informe"),
    col("ID_concepto").cast(IntegerType).as("id_concepto"),
    col("ID_Negocio").cast(VarcharType(30)).as("id_negocio"),
    col("ID_Sociedad").cast(IntegerType).as("id_sociedad"),
    col("ID_Agregacion").cast(IntegerType).as("id_tipodato"),
    col("ID_Dim1").cast(IntegerType).as("id_dim1"),
    col("ID_Dim2").cast(IntegerType).as("id_dim2"),
    col("ID_Escenario").cast(IntegerType).as("id_escenario"),
    col("Importe").cast(DecimalType(24,3)).as("imp_importe"),
    col("TasaAC").cast(DecimalType(24,10)),
    col("TasaM").cast(DecimalType(24,10)),
    col("val_divisa").cast(VarcharType(30)),
    col("Fecha").cast(IntegerType).as("num_fecha")).filter($"Importe".isNotNull).cache

b.limit(1).count()


# In[31]:


//Filtramos NUEVODF (TODO MENOS ACUM DE EST): Me quedo con todo - (el acumulado de estimado de la fecha actual)
val DFSinAcumuladoEST = b.filter(!($"id_escenario" === 4 and $"id_tipodato" === 1 and $"num_fecha" === v_periodo))
val DFRealLastMonth = b.filter($"id_escenario" === 1 and $"id_tipodato" === 2 and $"num_fecha" === v_periodo)
val DFEstLastMonth = b.filter($"id_escenario" === 4 and $"id_tipodato" === 2 and $"num_fecha" === v_periodo)


# In[32]:


var v_periodo_ytd = anio.toString().concat("00").concat("00").toInt


# In[33]:


import org.apache.spark.sql.types._
import org.apache.spark.sql.Column


//YTD DE ESTIMADO
//cargamos la tabla de la vista en un df
var df_vista = readFromSQLPool("sch_anl","cli0010_tb_fac_m_pbi_mmex", token).where(col("num_fecha") > v_periodo_ytd).cache

var DF_EST_YTD = DFEstLastMonth

// SI HAY DATO DE ESTIMADO HACEMOS ACUMULADO
if(!(DFEstLastMonth.rdd.isEmpty() || DFEstLastMonth.agg(sum("imp_importe")).collectAsList().get(0).get(0).equals(0) || DFEstLastMonth.agg(sum("imp_importe")).collectAsList().get(0).isNullAt(0))){

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
        col("num_fecha").contains(string_fecha_estimados.substring(0,4)) && //Del mismo año
        col("num_fecha")<string_fecha_estimados) //Hasta la fecha que estoy ejecutando (sin incluir)
        .drop(col("id_escenario")).withColumn("id_escenario",lit("4").cast(IntegerType))
    }else{df_vista.limit(0)}


    //recumeramos la fecha max del df de real por si no tuvieramos todas la fechas, recuperar las que faltan de estimado
    val maxFechaReal = df_pbi_real_to_estimados.select(max("num_fecha")).collectAsList()

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
        col("num_fecha").contains(string_fecha_estimados.substring(0,4)) &&
        col("num_fecha")> string_max_fecha_estimados && col("num_fecha") < string_fecha_estimados ) //.between(string_max_fecha_estimados.toInt+100,string_fecha_estimados.toInt-100))
    }else{df_vista.limit(0)}

    DF_EST_YTD = 
    if(!(DFRealLastMonth.rdd.isEmpty() || DFRealLastMonth.agg(sum("imp_importe")).collectAsList().get(0).get(0).equals(0) || DFRealLastMonth.agg(sum("imp_importe")).collectAsList().get(0).isNullAt(0))){
        df_pbi_real_to_estimados //Parte de estimado
        .unionByName(df_pbi_estimados_auxiliar,true) //Parte de real
        .unionByName(DFRealLastMonth.withColumn("id_escenario",lit("4").cast(IntegerType)),true) //último mes real
    }else{
        df_pbi_real_to_estimados //Parte de estimado
        .unionByName(df_pbi_estimados_auxiliar,true) //Parte de real
        .unionByName(DFEstLastMonth,true) //último mes estimado
    }
}
DF_EST_YTD.cache


# In[34]:


var T_Resto = df_vista
    .filter( $"num_fecha" >= v_year.toInt*10000+100+1 && $"num_fecha" < v_periodo )
    .filter( $"id_escenario" =!= 4 )
    .filter( $"id_tipodato" === 2 )
    .filter( $"val_divisa"=== "Pesos" )
    .select(
        col("ID_Escenario"),
        col("ID_Informe"),
        col("ID_concepto"),
        col("ID_Dim1"),
        col("ID_Dim2"),
        col("val_divisa"),
        col("ID_Negocio"),
        col("num_fecha"),
        col("imp_importe"),
        col("id_tipodato"),
        col("ID_Sociedad"),
        lit(null).as("TasaAC"),
        lit(null).as("TasaM")
    )

var T_Actual_Sin_Estimado = b.filter($"id_escenario" =!= 4 )
    .select(
        col("ID_Escenario"),
        col("ID_Informe"),
        col("ID_concepto"), 
        col("ID_Dim1"),
        col("ID_Dim2"),
        col("val_divisa"),
        col("ID_Negocio"),
        col("num_fecha"),
        col("imp_importe"),
        col("id_tipodato"),
        col("ID_Sociedad"),
        col("TasaAC"),
        col("TasaM")
    )

var T_Total_Part_1 = DF_EST_YTD.select(col("ID_Escenario"),col("ID_Informe"),col("ID_concepto"),col("ID_Dim1"),col("ID_Dim2"),col("val_divisa"),col("ID_Negocio"),col("num_fecha"),col("imp_importe"),col("id_tipodato"),col("ID_Sociedad"),col("TasaAC"), col("TasaM"))
    .union(T_Resto.select(col("ID_Escenario"),col("ID_Informe"),col("ID_concepto"),col("ID_Dim1"),col("ID_Dim2"),col("val_divisa"),col("ID_Negocio"),col("num_fecha"),col("imp_importe"),col("id_tipodato"),col("ID_Sociedad"),col("TasaAC"), col("TasaM")))
    .union(T_Actual_Sin_Estimado.select(col("ID_Escenario"),col("ID_Informe"),col("ID_concepto"),col("ID_Dim1"),col("ID_Dim2"),col("val_divisa"),col("ID_Negocio"),col("num_fecha"),col("imp_importe"),col("id_tipodato"),col("ID_Sociedad"),col("TasaAC"), col("TasaM"))).cache
    // .union(T_Estimados_Sin_Real.select(col("ID_Escenario"),col("ID_Informe"),col("ID_concepto"),col("ID_Dim1"),col("ID_Dim2"),col("ID_Dim3"),col("ID_Negocio"),col("num_fecha"),col("imp_importe"),col("id_tipodato"),col("ID_Sociedad")))
    
	
var T_Total = T_Total_Part_1
    .select(
        col("ID_Escenario")
        ,col("ID_Informe"),
        col("ID_concepto"),
        when( col("ID_Dim1") === "NULL", null ).otherwise(col("ID_Dim1")).as("ID_Dim1"),
        when( col("ID_Dim2") === "NULL", null ).otherwise(col("ID_Dim2")).as("ID_Dim2"),
        col("val_divisa"),
        col("ID_Negocio"),
        col("num_fecha"),
        col("imp_importe"),
        col("id_tipodato"),
        col("ID_Sociedad"),
        col("TasaAC"), 
        col("TasaM")
    ).cache

T_Total.limit(1).count()


# In[35]:


import org.apache.spark.sql.expressions.Window

val DFwithYear = T_Total.withColumn("anio",col("num_fecha").substr(0, 4))

val windowSpecfAgg = Window.partitionBy("anio", "id_escenario"
    ,"id_informe","id_concepto", "id_negocio", "id_dim1", "id_dim2","val_divisa","id_sociedad")
    .orderBy("num_fecha")
val windowSpecfAnioAgg = Window.partitionBy("anio").orderBy("num_fecha")

val aggDF_L = DFwithYear.withColumn("row_number",row_number.over(windowSpecfAnioAgg)).
    withColumn("imp_ytd", sum(col("imp_importe")).over(windowSpecfAgg)).cache

val t_DFYTD = aggDF_L.dropDuplicates


# In[36]:


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
    col("TasaAC").cast(DecimalType(24,10)),
    col("TasaM").cast(DecimalType(24,10)),
    col("val_divisa").cast(VarcharType(30)),
    col("imp_ytd").cast(DecimalType(24,3)),
    col("anio").cast(IntegerType),
    col("row_number").cast(IntegerType),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull)

val unPivotDF = t_DFYTD2.selectExpr("id_sociedad","id_escenario", "num_fecha", "id_tipodato", "id_negocio", "id_dim1", "id_dim2","val_divisa", "id_informe", "id_concepto", "TasaAC", "TasaM", "anio ", "row_number",
        "stack(2, 'imp_importe', imp_importe,'imp_ytd', imp_ytd) as (id_tipo_dato, imp_importe)")

val idagreColDF = unPivotDF.withColumn("id_tipodato",when(col("id_tipo_dato") === "imp_ytd", 1).otherwise(2))

val DFYTDPBI_pre = idagreColDF.drop("row_number", "anio", "id_tipo_dato")
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
                col("TasaAC").cast(DecimalType(24,10)),
                col("TasaM").cast(DecimalType(24,10)),
                col("val_divisa").cast(VarcharType(30)),
                col("num_fecha").cast(IntegerType)
        ).where(col("num_fecha") === v_periodo ).cache


# In[37]:


spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

val ds_output = s"cli0010_tb_aux_m_pivkpi_mmex/"
val ds_output_temp = ds_output.concat("pre_pbi")
val pathWriteTemp = s"$edwPath/$business/$ds_output_temp"
val parquet_path_temp = f"abfss://$container_output@$my_account/$pathWriteTemp"

DFYTDPBI_pre.coalesce(1).write.partitionBy("num_fecha").mode("overwrite").format("parquet").save(parquet_path_temp)


# In[38]:


for ((k,v) <- sc.getPersistentRDDs) {
   v.unpersist()
}

var DFYTDPBI = spark.read.parquet(parquet_path_temp).where(col("num_fecha") === v_periodo)

if (escenario == "REAL"){
    DFYTDPBI = DFYTDPBI.where(col("id_escenario") === 1).cache
}else if (escenario == "PA-UPA"){
    DFYTDPBI = DFYTDPBI.where(col("id_escenario").isin(2,3)).cache
}else if (escenario == "EST"){
    DFYTDPBI = DFYTDPBI.where(col("id_escenario") === 4).cache
}else if (escenario == "PA"){
    DFYTDPBI = DFYTDPBI.where(col("id_escenario") === 2).cache
}else if (escenario == "UPA"){
    DFYTDPBI = DFYTDPBI.where(col("id_escenario") === 3).cache
}else{
    DFYTDPBI.cache
}


# In[39]:


var acumuladoEuros = DFYTDPBI.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    (col("imp_importe").cast(DecimalType(24,3)) / col("TasaAC")).as("imp_importe"),
    col("TasaAC").cast(DecimalType(24,10)),
    col("TasaM").cast(DecimalType(24,10)),
    lit("Euros").as("val_divisa"),
    col("num_fecha").cast(IntegerType)).where(col("id_tipodato") === 1).filter($"imp_importe".isNotNull)

var facPesos = DFYTDPBI.select(
    col("id_informe").cast(IntegerType),
    col("id_concepto").cast(IntegerType),
    col("id_negocio").cast(VarcharType(30)),
    col("id_sociedad").cast(IntegerType),
    col("id_tipodato").cast(IntegerType),
    col("id_dim1").cast(IntegerType),
    col("id_dim2").cast(IntegerType),
    col("id_escenario").cast(IntegerType),
    col("imp_importe").cast(DecimalType(24,3)),
    col("TasaAC").cast(DecimalType(24,10)),
    col("TasaM").cast(DecimalType(24,10)),
    col("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("val_divisa") === "Pesos").cache

var pesosEurosAcum = facPesos.union(acumuladoEuros).dropDuplicates.cache
pesosEurosAcum.limit(1).cache


# **Euros a kiloeuros** ------------------------------------------ se queda post acumulados y periodos -----------------------------------------------------

# In[40]:


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
    col("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("val_divisa") === "Pesos").cache

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
    col("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("ID_concepto") <= 8 && col("val_divisa") === "Euros").cache

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
    col("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("ID_concepto") > 8 && col("val_divisa") === "Euros").cache


# In[41]:


var h = a.unionByName(c,true).unionByName(d,true).cache
h.limit(1).cache


# In[42]:


var t_Pool_glp2 = readFromSQLPool("sch_anl","cli0010_tb_fac_m_pbi_mmex", token).where(col("num_fecha") > v_periodo_ytd).cache

var anios = anio * 10000
var FacEurosAcum = t_Pool_glp2.where(col("id_tipodato") === 1 && col("val_divisa") === "Euros" && col("num_fecha").lt(v_periodo) && col("num_fecha") > anios)


# In[43]:


var FacPesosEurosAcum = FacEurosAcum.union(h).where(col("num_fecha") === v_periodo).dropDuplicates.cache


# In[44]:


var  FacEurosAcum = t_Pool_glp2.filter( $"num_fecha" >= v_year.toInt*10000+100+1 && $"num_fecha" < v_periodo )
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
        col("num_fecha"),
        col("imp_importe"),
        col("id_tipodato"),
        col("ID_Sociedad")
    )

var FacEurosResto = t_Pool_glp2.filter( $"num_fecha" >= v_year.toInt*10000+100+1 && $"num_fecha" < v_periodo )
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
        col("num_fecha"),
        col("imp_importe"),
        col("id_tipodato"),
        col("ID_Sociedad")
    )

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
        col("num_fecha"),
        col("imp_importe"),
        col("id_tipodato"),
        col("ID_Sociedad")
    )

var FacFinalEuros = FacEurosAcum.union(FacEurosResto).union(PeriodoActual).cache


# In[45]:


val windowSpecfAgg = Window.partitionBy("ID_Escenario","ID_Informe","ID_concepto", "ID_Negocio", "ID_Dim1", "Id_Dim2", "val_divisa").orderBy("num_fecha")//.rowsBetween(Window.unboundedPreceding, Window.currentRow)
val resultDF = FacFinalEuros.withColumn("prevValue", lag("imp_importe", 1, 0).over(windowSpecfAgg)).cache


# In[46]:


var resultDFF = resultDF.where(col("val_divisa") === "Euros" && col("num_fecha") === v_periodo)

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
    col("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull)

var DFfinal = FacPesosEurosAcum.union(EurosPorDiferencia).cache


# In[47]:


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
    col("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("id_concepto") =!= 1 && col("id_tipodato") === 1)

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
    col("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("id_tipodato") === 2).cache

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
    col("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("id_concepto") === 1)


# In[48]:


var factConCambios = factTipoDeCambio.unionByName(limpiezaTipoDeCambio,true).unionByName(limpiezaTipoDeCambio2,true).cache

factConCambios.limit(1).count()


# **m3 sin conversion a euros** -------------------------se queda post acumulados y periodos -------------------------------------------

# In[49]:


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
    col("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("val_divisa") === "Pesos").cache

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
    col("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("val_divisa") === "Euros" && col("id_concepto") =!= 2 && col("id_concepto") =!= 3 && col("id_concepto") =!= 4 && col("id_concepto") =!= 28 && col("id_concepto") =!= 1).cache

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
    lit("Euros").as("val_divisa").cast(VarcharType(30)),
    col("num_fecha").cast(IntegerType)).filter($"imp_importe".isNotNull).where(col("id_concepto") === 2 || col("id_concepto") === 3 || col("id_concepto") === 4 || col("id_concepto") === 28 || col("id_concepto") === 1).cache


# In[50]:


var factConCambios2 = m3Euros.unionByName(m3Euros2, true).unionByName(m3Pesos, true).cache


# In[51]:


var finaliza = factConCambios2.where(col("num_fecha") === v_periodo)

if (escenario == "REAL"){
    finaliza = finaliza.where(col("id_escenario") === 1).cache
}else if (escenario == "PA-UPA"){
    finaliza = finaliza.where(col("id_escenario").isin(2,3)).cache
}else if (escenario == "EST"){
    finaliza = finaliza.where(col("id_escenario") === 4).cache
}else if (escenario == "PA"){
    finaliza = finaliza.where(col("id_escenario") === 2).cache
}else if (escenario == "UPA"){
    finaliza = finaliza.where(col("id_escenario") === 3).cache
}else{
    finaliza.cache
}

// finaliza.limit(1).count()


# In[52]:


spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

val ds_output = s"cli0010_tb_fac_m_pbi_mmex/"
val pathWriteFinal = s"$edwPath/$business/$ds_output"
val parquet_path_final = f"abfss://$container_output@$my_account/$pathWriteFinal"

finaliza.coalesce(1).write.partitionBy("num_fecha").mode("overwrite").format("parquet").save(parquet_path_final)


# In[53]:


for ((k,v) <- sc.getPersistentRDDs) {
   v.unpersist()
}

var finaliza_pbi = spark.read.parquet(parquet_path_final).where(col("num_fecha") === v_periodo)

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


# In[54]:


import com.microsoft.azure.synapse.tokenlibrary.TokenLibrary

import java.sql.Connection 
import java.sql.ResultSet 
import java.sql.Statement 
import com.microsoft.sqlserver.jdbc.SQLServerDataSource

var schemaName = "sch_anl"

var sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmex where num_fecha in ($v_periodo)"

if (escenario == "REAL"){
    sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmex where num_fecha in ($v_periodo) and id_escenario = 1"
}else if (escenario == "PA-UPA"){
    sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmex where num_fecha in ($v_periodo) and id_escenario in (2,3)"
}else if (escenario == "EST"){
    sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmex where num_fecha in ($v_periodo) and id_escenario = 4"
}else if (escenario == "PA"){
    sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmex where num_fecha in ($v_periodo) and id_escenario = 2"
}else if (escenario == "UPA"){
    sqlQuery = s"delete from sch_anl.cli0010_tb_fac_m_pbi_mmex where num_fecha in ($v_periodo) and id_escenario = 3"
}
print(sqlQuery+"\n")
executeSQLPool (token, url, sqlQuery , schemaName, "sch_anl.cli0010_tb_fac_m_pbi_mmex");


# In[55]:


finaliza_pbi.
write.
    format("com.microsoft.sqlserver.jdbc.spark").
    mode("append"). //overwrite
    option("url", url).
    option("dbtable", s"sch_anl.cli0010_tb_fac_m_pbi_mmex").
    option("mssqlIsolationLevel", "READ_UNCOMMITTED").
    option("truncate", "false").
    option("tableLock","true").
    option("reliabilityLevel","BEST_EFFORT").
    option("numPartitions","1").
    option("batchsize","1000000").
    option("accessToken", token).
    save()


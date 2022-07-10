from AGF.bureau_report_data.proj.one_click.src import one_click_tradeline_attr_generation_20210311 as calc
import mc_response_model.A3_project as mc_ver
import take_rate_model.A3_project as take_rate
import Constant as mc
import common.util.Util as utl
import common.db.edh_connector as hv
import datetime
import dateutil
import re
import warnings
import modeling as gb
import pandas as pd
import common.util.iv as iv
import os
from random import sample
from sklearn.tree import DecisionTreeClassifier
from sklearn_pandas import DataFrameMapper
from sklearn2pmml.pipeline import PMMLPipeline
from update_partner_come_in_flag import update_table

# "AppstatsAttrGen.202110.2021-10-22" ,
# "LastissuedAttrGen.202110.2021-10-22",
# "NonpartnerAttrGen.202110.2021-10-22",
# 'PartnerAttrGen.202110.2021-10-22'
# case = ['appstatsGenerateAttrAll']

description = dict(
    lst='large scale testing, func: ?',
    mc_run='model data generation: for tu tradeline attribute, it needs double check as it may break.',
    modeling='modeling: build model assume data is there, download if not , func: modeling',
    modelrescore='model Rescore with existing model pickle and cols def, func: modelrescore',
    pmml='convert model pickle into pmml, func: model2pmml'
)


class mcResponse:
    # base parameter setting
    ssn_field = None
    snap_d_field = None
    snap_d = None
    runType = None
    base_date = None
    col_version = None
    ext = None
    segment = None
    period = None
    period_in_table = None
    runVerison = None

    # All table in Use: for different input table and final model input table
    base_candidate_table = None
    app_attr_table = None
    partner_come_table = None
    issue_attr_table = None
    nonpartner_table = None
    partner_table = None
    dm_table = None
    mc_table = None
    response_table = None
    ita_table = None
    ita_trade_table = None
    mkt_table = None
    model_table_stella = None
    model_table = None

    # model input data and iv related setting (intermediate table name is set inside model class)
    model_data_file = None
    model_iv_file = None

    # upload data to EDH related setting
    model_rescore_result_file = None
    upload_table_input_file = None
    upload_table_result_file = None

    replace_dict = {}

    def __init__(self, runVerison, snap_d='NANNAN', ssn_field='ssn', snap_d_field='snap_d', **kw):
        self.setValues(runVerison, ssn_field=ssn_field, snap_d_field=snap_d_field, snap_d=snap_d, **kw)
        self.determineMarketView()
        # ver.execute_cases(case=['lastissuedGenerateAttrAll',
        #                         'appstatsGenerateAttrAll',
        #                         'nonpartnerGenerateAttrAll',
        #                         'partnerGenerateAttrAll',
        #                         'dmGenerateAttrAll'],
        #
        #           replace_dict=self.replace_dict,
        #           runVerison=self.runVerison,
        #           para_dict=self.__dict__)

    def createBaseView(self,
                       sql_query=mc.SQL_PATH + 'presto_offer_view.sql',
                       escape_statement=0,
                       period_based_on_snap_d=True):
        """
        Create Base Population Table for the given date

        :param sql_query:
        :param escape_statement:
        :param period_based_on_snap_d:         bool, if yes, it will be end of date last month;
                                               otherwise, it will be based on self.period
        :return:
        """
        replace_dict = {}
        para_dict = {}

        case = ['creationDefault']
        if period_based_on_snap_d:
            period = (datetime.datetime.strptime(self.snap_d, '%Y-%m-%d').replace(day=1) - datetime.timedelta(days=1)).strftime(
                '%Y-%m-%d')
        else:
            period = self.period

        replace_dict.update(
            app_view=mc.APP_VIEW,
            base_table=self.base_candidate_table,
            snap_d=self.snap_d,
            period=period
        )

        para_dict.update(
            sql_query=sql_query,
            escape_statement=escape_statement
        )

        mc_ver.execute_cases(case=case,
                             replace_dict=replace_dict,
                             runVerison=self.runVerison,
                             para_dict=para_dict)

    def determineMarketView(self):
        if self.snap_d == '2021-06-01':
            self.mkt_table = 'Marketing_temp.May_Uniq_User_Imps_Summary_RECO'
        elif self.snap_d == '2021-07-01':
            self.mkt_table = 'Marketing_temp.June_Uniq_User_Imps_Summary_RECO'
        elif self.snap_d == '2021-11-01':
            self.mkt_table = 'Marketing_temp.OCT_Uniq_User_Imps_Summary_RECO'
        else:
            self.mkt_table = 'Marketing_temp.OCT_Uniq_User_Imps_Summary_RECO'
            warnings.warn('mkt_table={mkt_table} will be in Use by default'.format(mkt_table='Marketing_temp.OCT_Uniq_User_Imps_Summary_RECO'))

    def createTakeRateView(self
                           , end_d=None
                           , start_d='2017-01-01'
                           , sql_query=take_rate.SQL_PATH + 'presto_offer_view_v4.sql'
                           , connect_type='presto'
                           ):
        case = ['creationDefault']
        replace_dict = {}
        para_dict = {}
        # = '2021-10-22'
        # base_date = '202110'
        # four month data
        if not end_d:
            end_d = (datetime.datetime.strptime(self.snap_d, '%Y-%m-%d') + dateutil.relativedelta.relativedelta(
                months=1)).strftime('%Y-%m-%d')

        replace_dict.update(
            app_view=mc.APP_VIEW,
            base_table=self.partner_come_table,
            start_d=start_d,
            end_d=end_d
        )
        para_dict.update(sql_query=sql_query,
                         connect_type=connect_type)

        take_rate.execute_cases(case=case,
                                replace_dict=replace_dict,
                                runVerison=self.runVerison,
                                para_dict=para_dict)

    def createResponseView(self
                           , sql_query=mc.SQL_PATH + 'presto_response_flag.sql'
                           , escape_statement=0
                           , take_rate_view_creation=True
                           ):
        """
        create response view

        :param sql_query:
        :param escape_statement:
        :param take_rate_view_creation: bool, if no, do not recreate the view.
        :return:
        """
        case = ['creationDefault']
        replace_dict = {}
        para_dict = {}
        end_d = (datetime.datetime.strptime(self.snap_d, '%Y-%m-%d') + dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m-%d')

        # start_d = (datetime.datetime.strptime(end_d, '%Y-%m-%d') - dateutil.relativedelta.relativedelta(years=1)).strftime('%Y-%m-%d')

        replace_dict.update(
            app_view=mc.APP_VIEW,
            base_candidate_table=self.base_candidate_table,
            start_d=self.snap_d,
            response_table=self.response_table,
            base_partner_view=self.partner_come_table,
            end_d=end_d
        )

        para_dict.update(sql_query=sql_query
                         , escape_statement=escape_statement)

        if take_rate_view_creation:
            update_table()
            self.createTakeRateView(end_d=end_d)

        mc_ver.execute_cases(case=case,
                             replace_dict=replace_dict,
                             runVerison=self.runVerison,
                             para_dict=para_dict)

    def createAppstatsView(self):
        replace_dict = {}
        para_dict = {}

        case = ['appstatsGenerateAttrAll']
        replace_dict.update(partner_come_table=self.partner_come_table)

        para_dict.update(dict(
            ssn_field=self.ssn_field
            , snap_d_field=self.snap_d_field
            , base_candidate_table=self.base_candidate_table
            , base_attr_table=self.app_attr_table
        ))
        mc_ver.execute_cases(case=case,
                             replace_dict=replace_dict,
                             runVerison=self.runVerison,
                             para_dict=para_dict)

    def createLastissuedView(self):
        replace_dict = {}
        para_dict = {}

        case = ['lastissuedGenerateAttrAll']
        replace_dict.update(partner_come_table=self.partner_come_table)

        para_dict.update(dict(
            ssn_field=self.ssn_field
            , snap_d_field=self.snap_d_field
            , base_candidate_table=self.base_candidate_table
            , base_attr_table=self.issue_attr_table
        ))
        mc_ver.execute_cases(case=case,
                             replace_dict=replace_dict,
                             runVerison=self.runVerison,
                             para_dict=para_dict)

    def createMcView(self, case_in_use=None):
        para_dict = {}
        replace_dict = {}
        case = ['generateAttrAll']

        # mc_table = 'dept_risk.fyang_mc_response_model_202109_20210601_base_attr_final'
        para_dict.update(dict(
            ssn_field=self.ssn_field
            , snap_d_field=self.snap_d_field
            , base_candidate_table=self.base_candidate_table
            , final_attr_table=self.mc_table
        ))

        if case_in_use:
            para_dict.update(case_in_use=case_in_use)

        mc_ver.execute_cases(case=case,
                             replace_dict=replace_dict,
                             runVerison=self.runVerison,
                             para_dict=para_dict)

    def createPartnerView(self):
        case = ['partnerGenerateAttrAll']
        replace_dict = {}
        para_dict = {}
        replace_dict.update(partner_come_table=self.partner_come_table)

        para_dict.update(dict(
            ssn_field=self.ssn_field
            , snap_d_field=self.snap_d_field
            , base_candidate_table=self.base_candidate_table
            , base_attr_table=self.partner_table
        ))

        mc_ver.execute_cases(case=case,
                             replace_dict=replace_dict,
                             runVerison=self.runVerison,
                             para_dict=para_dict)

    def createNonpartnerView(self):
        case = ['nonpartnerGenerateAttrAll']
        replace_dict = {}
        para_dict = {}
        replace_dict.update(partner_come_table=self.partner_come_table)

        para_dict.update(dict(
            ssn_field=self.ssn_field
            , snap_d_field=self.snap_d_field
            , base_candidate_table=self.base_candidate_table
            , base_attr_table=self.nonpartner_table
        ))

        mc_ver.execute_cases(case=case,
                             replace_dict=replace_dict,
                             runVerison=self.runVerison,
                             para_dict=para_dict)

    def createITAView(self, escape_statement=0, sql_query=mc.SQL_PATH+'presto_ita_attribute.sql', period=None):
        case = ['creationDefault']
        para_dict = {}
        replace_dict = {}

        if not period:
            period = self.period

        replace_dict.update(ita_table=self.ita_table,
                            period=period)

        para_dict.update(dict(escape_statement=escape_statement,
                              sql_query=sql_query))

        mc_ver.execute_cases(case=case,
                             replace_dict=replace_dict,
                             runVerison=self.runVerison,
                             para_dict=para_dict)

    def createITAtradeView(self, step=0, substep=0, date=None,
                           view_creation=True,
                           pp_view_creation=None,
                           escape_statement_final=0, period=None, no_lr_attr=False
                           ):
        if not date:
            date = self.period_in_table

        project_name = None

        if period:
            period_setting = period
        else:
            period_setting = self.period

        base_table = 'dept_risk_data_science.for_jack_navneet_fyang'
        final_table_name_with_dti_other = base_table + '_tu_all_{base_date}'.format(base_date=self.base_date)

        payment_pattern_base_table = base_table + '_ep_fe_candidate_{base_date}'.format(base_date=self.base_date)
        payment_pattern_loan_level_table = base_table + '_ep_fe_loan_level_{base_date}'.format(base_date=self.base_date)
        final_fe_table = base_table + '_ep_fe_final_table_{base_date}'.format(base_date=self.base_date)

        attribute_list = [
            "attr09_inc_ratio", "avg_agg5_24mon",
            "avg_agg8_24mon", "avg_bcpayr_24mon", "cv24_cv22", "delta_agg3_1_3MV",
            "delta_agg5_1_12MV", "delta_agg6_1_12MV", "desamt_inc_ratio", "max_dec12_agg5",
            "max_dec24_agg5", "max_inc24_agg1", "Mon_Since_Max_agg8", "mon_since_max_aggbcu",
            "mon_since_max_aggnmu", "num_dec10pct24_aggs1", "num_inc10pct24_agg3", "num_inc10pct24_aggbcu",
            "num_inc5pct12_agg1", "PF29_PF01", "rbal_damt_ratio", "avg_cre11_ccnpa9nopn_r720d",
            "avg_cre11_pa9_r360d", "avg_hig10_ins8nopn_o720d", "avg_hig10_ins8nopn_r180d",
            "avg_hig10_opn_o720d", "avg_sch23_cc_r720d", "avg_sch23_ccnpa9nopn_o720d", "max_cre11_pa9_r720d",
            "max_cur14_pa9_o360d", "max_hig10_pa9ncls_r720d", "min_cre11_ccnopn_o720d", "min_cre11_norm_o180d",
            "min_cre11_opn_r90d", "min_hig10_cc_r360d", "sum_cur14_ccnpa9nopn_o720d", "sum_hig10_rvlv_r720d",
            "sum_pas7_pa9_r360d", "sum_sch23_ins8npa9nopn_o720d", "sum_sch23_opn_o180d",
            "sum_sch23_opnnrvlv_o720d", "sum_sch23_pa9nopn_o360d", "sum_sch23_pa9nopnnrvlv_r360d",
            "lx_au13", "lx_au31", "lx_pmtpttn107", "lx_pmtpttn120", "lx_pmtpttn295",
            "lx_pmtpttn313", "lx_pmtpttn314", "tt_rto_ut_naut_rv", "tt_cnt_ut_naut_rv_p90"

            , "tt_rto_cb_hc_pl"
            # additional attribute
            , "tt_cnt_ut_naut_rv_p75"
            # additional attribute - 2
            , "tt_sum_cb_naut_pl"
            # additional attribute - 3
            , "tt_cnt_cb_naut_pl_gt0"
            # additional attribute - 4: tt_sum_hc_prmy_r90d
            , "sum_hig10_pa9_r90d"
            # added for backend G6
            , "tt_sum_cb_nautopn_pl"
        ]

        if step < 1:
            calc.retro_20201230_g6_calculation(base_table=base_table
                                               , case=substep
                                               , view_creation=view_creation
                                               , pp_view_creation=pp_view_creation
                                               , escape_statement_final=escape_statement_final
                                               , period_setting=period_setting
                                               , payment_pattern_base_table=payment_pattern_base_table
                                               , payment_pattern_loan_level_table=payment_pattern_loan_level_table
                                               , final_fe_table=final_fe_table
                                               , suffix=date
                                               , project_name=project_name
                                               , attribute_list=attribute_list
                                               , final_table_name_with_dti_other=final_table_name_with_dti_other
                                               , no_lr_attr=no_lr_attr
                                               )
        if no_lr_attr:
            return

        if step < 2:
            sql_query = """ drop table if exists {ita_trade_table};
            create table {ita_trade_table}
            as
            select ssn_enc, base.*
            from (
                select base.aid, la.ssn_enc,
                row_number() over (partition by la.ssn_enc order by la.create_d desc) as ranks

                from {final_table_name_with_dti_other} base
                join tlc_enc.lc_actor la on la.id=base.aid
            ) la
            join {final_table_name_with_dti_other} base on base.aid = la.aid
            where la.ranks = 1
            """.format(final_table_name_with_dti_other=final_table_name_with_dti_other,
                       ita_trade_table=self.ita_trade_table)

            utl.download_data(sql_query=sql_query, outprint=True)

    def createDMView(self):
        para_dict = {}
        replace_dict = {}

        case = ['dmGenerateAttrAll']

        para_dict.update(dict(
            ssn_field=self.ssn_field
            , snap_d_field=self.snap_d_field
            , base_candidate_table=self.base_candidate_table
            , base_attr_table=self.dm_table
        ))
        mc_ver.execute_cases(case=case,
                             replace_dict=replace_dict,
                             runVerison=self.runVerison,
                             para_dict=para_dict)

    def createModelDataSet(self, mode='stella', segment='bcq'):
        """

        :param mode:            'stella', 'eml', 'banner', 'prepayment'
        :param segment:
        :return:
        """
        replace_dict = {}
        para_dict = {}
        case = ['runSQLDefault']

        if mode == 'stella':
            sql_query = mc.SQL_PATH + 'presto_model_view_stella.sql'
        elif mode == 'prepay':
            sql_query = mc.SQL_PATH + 'presto_model_view_prepayment.sql'
        else:
            sql_query = mc.SQL_PATH + 'presto_model_view.sql'

        escape_statement = 0

        # TODO: need attention, can not automatically update
        # mkt_table = 'Marketing_temp.May_Uniq_User_Imps_Summary_RECO'
        # ita_trade_table = 'dept_risk.fyang_tu_dti_20210920_period_20210531_20210920'
        # ita_table = 'dept_risk.szhu_response_20210531_ita'
        if mode == 'stella':
            replace_dict.update(
                app_view=mc.APP_VIEW,
                base_candidate_table=self.base_candidate_table,
                issue_attr_table=self.issue_attr_table,
                app_attr_table=self.app_attr_table,
                nonpartner_table=self.nonpartner_table,
                partner_table=self.partner_table,
                dm_table=self.dm_table,
                mc_table=self.mc_table,
                mkt_table=self.mkt_table,
                ita_trade_table=self.ita_trade_table,
                ita_table=self.ita_table,
                response_table=self.response_table,
                model_table_stella=self.model_table_stella
            )
            para_dict.update(dict(raw_file=None, escape_statement=escape_statement, sql_query=sql_query))
            mc_ver.execute_cases(case=case,
                                 replace_dict=replace_dict,
                                 runVerison=self.runVerison,
                                 para_dict=para_dict)

            self.downloadModelDataSet(segment=segment)

        else:
            replace_dict.update(
                app_view=mc.APP_VIEW,
                base_candidate_table=self.base_candidate_table,
                issue_attr_table=self.issue_attr_table,
                app_attr_table=self.app_attr_table,
                nonpartner_table=self.nonpartner_table,
                partner_table=self.partner_table,
                dm_table=self.dm_table,
                mc_table=self.mc_table,
                mkt_table=self.mkt_table,
                ita_trade_table=self.ita_trade_table,
                ita_table=self.ita_table,
                response_table=self.response_table,
                model_table_stella=self.model_table_stella
            )
            para_dict.update(dict(raw_file=self.model_data_file,
                                  escape_statement=escape_statement,
                                  sql_query=sql_query))

            mc_ver.execute_cases(case=case,
                                 replace_dict=replace_dict,
                                 runVerison=self.runVerison,
                                 para_dict=para_dict)

    def downloadModelDataSet(self, segment=None):

        if not segment:
            segment = self.segment

        if segment == 'bcq':
            sql_query = "select * from {model_table_stella} where seg_bcq=1".format(
                model_table_stella=self.model_table_stella)

        elif segment == 'either':
            sql_query = "select * from {model_table_stella} where seg_bcq=0 and (seg_bnr=1 or seg_eml=1)".format(
                model_table_stella=self.model_table_stella)

        elif segment == 'none':
            sql_query = "select * from {model_table_stella} where seg_bcq=0 and seg_bnr=0 and seg_eml=0".format(
                model_table_stella=self.model_table_stella)

        elif segment == 'bcqeml':
            sql_query = """select base.*, eml_response_flg
                from {model_table_stella} base
                left join (
                select base.ssn
                , max(case when la.loan_app_create_dt >= date('2021-06-01') and la.loan_app_create_dt < date('2021-07-01') then
                case when funnel = 'Partner API' and channel='Email' and partner_come_flg = 1 then 1
                when funnel <> 'Partner API' and channel='Email' and la.actor_ssn_enc is not null then 1 else 0 end else 0 end) as eml_response_flg
                from dept_risk.fyang_mc_response_model_202112_20210601 base
                left join dept_risk.fyang_parnter_take_rate_base_202112_20210601 la on la.actor_ssn_enc=base.ssn
                group by base.ssn
                ) todo on base.ssn=todo.ssn
                where seg_bcq=1""".format(
                model_table_stella=self.model_table_stella)
            print(sql_query)

        elif segment == 'eithereml':
            sql_query = """select base.*, eml_response_flg
                            from {model_table_stella} base
                            left join (
                            select base.ssn
                            , max(case when la.loan_app_create_dt >= date('2021-06-01') and la.loan_app_create_dt < date('2021-07-01') then
                            case when funnel = 'Partner API' and channel='Email' and partner_come_flg = 1 then 1
                            when funnel <> 'Partner API' and channel='Email' and la.actor_ssn_enc is not null then 1 else 0 end else 0 end) as eml_response_flg
                            from dept_risk.fyang_mc_response_model_202112_20210601 base
                            left join dept_risk.fyang_parnter_take_rate_base_202112_20210601 la on la.actor_ssn_enc=base.ssn
                            group by base.ssn
                ) todo on base.ssn=todo.ssn
                            where seg_bcq=0 and (seg_bnr=1 or seg_eml=1)""".format(model_table_stella=self.model_table_stella)
            print(sql_query)

        elif segment == 'noneeml':
            sql_query = """select base.*, eml_response_flg
                                        from {model_table_stella} base
                                        left join (
                                        select base.ssn
                                        , max(case when la.loan_app_create_dt >= date('2021-06-01') and la.loan_app_create_dt < date('2021-07-01') then
                                        case when funnel = 'Partner API' and channel='Email' and partner_come_flg = 1 then 1
                                        when funnel <> 'Partner API' and channel='Email' and la.actor_ssn_enc is not null then 1 else 0 end else 0 end) as eml_response_flg
                                        from dept_risk.fyang_mc_response_model_202112_20210601 base
                                        left join dept_risk.fyang_parnter_take_rate_base_202112_20210601 la on la.actor_ssn_enc=base.ssn
                                        group by base.ssn
                            ) todo on base.ssn=todo.ssn

                                        where seg_bcq=0 and seg_bnr=0 and seg_eml=0""".format(
                model_table_stella=self.model_table_stella)
            print(sql_query)

        else:
            raise Exception('Invalid segment: {segment}'.format(segment=segment))

        utl.download_data(raw_file=self.model_data_file, sql_query=sql_query)

    def getIVfile(self, output_file=None, tag='response_flg', with_detail=True, not_in_check=None, var_list=None):
        df = pd.read_csv(self.model_data_file)
        iv_cal = iv.iv_inc()
        if not output_file:
            output_file = self.model_iv_file

        iv_cal.iv_main(df=df, tag=tag, output_file=output_file, inc_iv=False, var_list=var_list,
                       attr_method='tree', with_detail=with_detail, not_in_check=not_in_check)

    def modeling(self,
                 input_file=None,
                 model_step=2,
                 method=None,
                 pk='ssn',
                 tag='response_flg',
                 cate_converting_method='direct',
                 col_not_in_model=None,
                 cols_dropped_before_model=None,
                 **kw):
        case = [
            'Modeling'
                ]

        para_dict = {}
        replace_dict = {}

        if input_file is None:
            input_file = self.model_data_file

        if not method:
            method = self.ext

        result_file = input_file.rpartition('.')[0] + '_{ext}{col_version}_result.csv'.format(ext=self.ext,
                                                                                              col_version=self.col_version)

        analysis_result_file = result_file.rpartition('.')[0] + '_treelayout.csv'
        perf_train_file = result_file.rpartition('_')[0] + '_perf_both.csv'

        if not col_not_in_model:
            col_not_in_model = []

        if not cols_dropped_before_model:
            cols_dropped_before_model = []

        # if ('method' not in para_dict) and ('input_file' not in para_dict) and ('ext' not in para_dict):
        para_dict.update(dict(
            input_file=input_file
            , method=method
            , ext=self.ext
            , col_version=self.col_version
            , cols_dropped_before_model=cols_dropped_before_model
            , tag=tag
            , cate_converting_method=cate_converting_method
            , col_not_in_model=col_not_in_model
            , model_step=model_step
            , perf_train_file=perf_train_file
            , result_file=result_file
            , analysis_result_file=analysis_result_file
            , pk=pk
            , **kw
        ))

        # replace_dict.update(pk=pk)

        mc_ver.execute_cases(case=case,
                             replace_dict=replace_dict,
                             runVerison=self.runVerison,
                             para_dict=para_dict)

    def modelrescore(self,
                     model_pickle,
                     selected_col_pickle,
                     model_step=1,
                     method=None,
                     pk='ssn',
                     tag='response_flg',
                     cate_converting_method='direct',
                     col_not_in_model=None,
                     mode='predictOnly',
                     input_file=None,
                     segment=None,
                     cols_dropped_before_model=None,
                     save_input_file=False,
                     append_cols=[]
                     ):
        """

        :param model_pickle:
        :param selected_col_pickle:
        :param model_step:
        :param method:
        :param pk:
        :param tag:
        :param cate_converting_method:
        :param col_not_in_model:
        :param mode:                        'normal' (with performance), 'predictOnly' (with prob only), 'resultOnly' (with prob, and tag), append_cols
        :param cols_dropped_before_model:
        :param segment:                     'bcq', 'either', 'none'
        :param save_input_file:             bool, False by default
        :param append_cols:                 list, for 'append_cols' mode
        :return:
        """
        if input_file:
            print('input file is: {input_file}'.format(input_file=input_file))
        else:
            input_file = self.model_data_file
            print('Use {input_file} as input file'.format(input_file=input_file))

        if not method:
            method = self.ext

        if segment:
            if segment != self.segment:
                raise Exception('Given segment:{segment} does not align with segment from '
                                'model data file name: {input_file}'.format(segment=segment, input_file=input_file))

        self.model_rescore_result_file = input_file.rpartition('.')[0] + '_{ext}{col_version}_result.csv'.format(ext=self.ext,
                                                                                              col_version=self.col_version)

        analysis_result_file = self.model_rescore_result_file.rpartition('.')[0] + '_treelayout.csv'
        perf_train_file = self.model_rescore_result_file.rpartition('_')[0] + '_perf_both.csv'

        if not col_not_in_model:
            col_not_in_model = []

        if not cols_dropped_before_model:
            cols_dropped_before_model = []

        para_dict = {}
        para_dict.update(dict(
            input_file=input_file
            , method=method
            , ext=self.ext
            , col_version=self.col_version
            , cols_dropped_before_model=cols_dropped_before_model
            , tag=tag
            , cate_converting_method=cate_converting_method
            , col_not_in_model=col_not_in_model
            , model_step=model_step
            , perf_train_file=perf_train_file
            # , result_file=self.model_rescore_result_file
            , analysis_result_file=analysis_result_file
            , pk=pk
        ))

        if os.path.isfile(input_file):
            pass
        else:
            self.downloadModelDataSet(segment=self.segment)

        gm = gb.gbm(**para_dict)

        gm.model_process_no_split(ready_file_only=True, **para_dict)

        gm.model_test(result_file=self.model_rescore_result_file, mode=mode,
                      input_file=gm.model_ready_file, pk=para_dict['pk']
                      , method=para_dict['method']
                      , model_pickle=model_pickle
                      , selected_col_pickle=selected_col_pickle
                      , append_cols=append_cols
                      # , model_pickle=MODEL_PATH + 'mc_response_100k_dt_v2_dt_model.pickle'
                      # , selected_col_pickle=MODEL_PATH + 'mc_response_100k_dt_v2_dt_cols.pickle'
                      )

        if save_input_file:
            if self.segment == 'bcq':
                cols = [
                    'tot_cnt_app_due_6m',
                    'nonactv_app',
                    'tot_cnt_app_due_15d',
                    'mc_cnt_web_lgn_due_evr',
                    'ep_ssn_mr_issueintrate_issue',
                    'mc_bool_lgn_due_evr',
                    'tot_cnt_issued_due_6m',
                    'mc_cnt_eml_opn_due_30d',
                    'bc34s',
                    's043s',
                    'mc_rct_lgn_byd',
                    'g205s',
                    'ssn'
                ]
            else:
                raise Exception('segment input not given yet')

            pd.read_csv(gm.model_ready_file)[cols].to_csv(self.model_rescore_result_file.rpartition('.')[0] + '_input.csv', index=False)

    def model_lst_mismatch(self,
                           output_excel,
                           escape_statement=0,
                           input_table_for_lst=None,
                           replace_dict={},
                           samples=None,
                           mode='lst',
                           sql_for_input=None
                  ):
        """

        :param output_excel:        str(PATH), a excel file for the output
        :param escape_statement:    int, skip some sql for the mismatch query

        :param input_table_for_lst: str, at least one of input_table_for_lst or replace_dict need to
                                        contain input_table_for_lst.
        :param replace_dict:        dict, default to be null, but require input_table_for_lst
        :param sql_for_input:       str, sql or sql location
        :param samples:             int, if given, will only a sample.
        :param mode:                str, lst or audit
        :return:
        """

        writer1 = pd.ExcelWriter(output_excel, engine='xlsxwriter')

        if input_table_for_lst:
            replace_dict.update({'input_table_for_lst': input_table_for_lst})

        df = utl.download_data(sql_query=sql_for_input, replace_dict=replace_dict, outprint=True,
                               escape_statement=escape_statement)

        if samples:
            mismatch_score = set(df[df['attr'] == 'score']['globalid'])
            mismatch_attr = set(df[df['attr'] == 'tot_cnt_app_due_6m']['globalid'])

            mismatch_list = sample(list(mismatch_score & mismatch_attr), samples)
        else:
            mismatch_list = set(df[df['attr'] == 'score']['globalid'])

        df[df.globalid.isin(mismatch_list)].set_index(['globalid', 'ssn']).to_excel(writer1, sheet_name='scoremismatch', index=True, encoding='utf-8')

        if mode == 'lst':
            df.set_index(['globalid', 'ssn']).to_excel(writer1, sheet_name='mismatch', index=True, encoding='utf-8')
            utl.download_data(sql_query=mc.SQL_PATH + 'presto_lst_20220301_v2_mismatch_stats.sql',outprint=True,
                              replace_dict=replace_dict).to_excel(writer1, sheet_name='allstats', index=True, encoding='utf-8')
            utl.download_data(sql_query=mc.SQL_PATH + 'presto_lst_20220301_v2_mismatch_stats_conditional.sql', outprint=True,
                              replace_dict=replace_dict).to_excel(writer1, sheet_name='scorestats', index=True, encoding='utf-8')
        else:
            utl.download_data(sql_query=mc.SQL_PATH + 'presto_liveaudit_mismatch_stats.sql',outprint=True,
                              replace_dict=replace_dict).to_excel(writer1, sheet_name='allstats', index=True,
                                                                  encoding='utf-8')
            utl.download_data(sql_query=mc.SQL_PATH + 'presto_liveaudit_mismatch_stats_conditional.sql', outprint=True,
                              replace_dict=replace_dict).to_excel(writer1, sheet_name='scorestats', index=True,
                                                                  encoding='utf-8')

        writer1.close()

    def model_lst(self,
                  pmml,
                  input_file,
                  output_file,
                  replace_dict=None,
                  sql_for_input=None,
                  ):
        """
        :param pmml:
        :param output_file:
        :param input_file:

        :param replace_dict:
        :param sql_for_input:
        :return:
        """

        # import subprocess
        # from openscoring import Openscoring
        # import numpy as np
        # p = subprocess.Popen('java -jar openscoring-server-executable-1.4.3.jar',
        #                      shell=True)
        # os = Openscoring("http://localhost:8080/openscoring")

        # Deploying a PMML document DecisionTreeIris.pmml as an Iris model:
        # os.deployFile("MCresponse", pmml)
        if os.path.isfile(input_file):
            df = pd.read_csv(input_file)

        elif sql_for_input:
            df = utl.download_data(sql_query=sql_for_input, outprint=True, replace_dict=replace_dict)
            df.to_csv(input_file, index=False)

        else:
            raise Exception('No valid sql_for_input or input_file is given')

        from pypmml import Model
        model = Model.fromFile(pmml)

        def getScore(pmml_cols):
            result = model.predict(pmml_cols.to_dict())
            return result['probability(1)']

        df['result'] = df.apply(getScore, axis=1)
        df['mismatch'] = df.apply(lambda x: abs(x['result'] - x['score']) > 0.001, axis=1)
        df.to_csv(output_file, index=False)

    def model2pmml(self,
                    model_pickle,
                    col_pickle,
                    pk='ssn',
                    tag='response_flg',
                    pmml_name="MCDecisionTree.pmml"
                     ):
        model = pd.read_pickle(model_pickle)
        selected_var_list_rf = pd.read_pickle(col_pickle)
        if tag:
            if tag in selected_var_list_rf:
                selected_var_list_rf.remove(tag)
        if pk:
            if pk in selected_var_list_rf:
                selected_var_list_rf.remove(pk)

        mapping = [(selected_var_list_rf, [])]
        mapper = DataFrameMapper(mapping)

        pipeline = PMMLPipeline([
            ("mapper", mapper),
            ("estimator", model)
        ])

        from sklearn2pmml import sklearn2pmml

        sklearn2pmml(pipeline, pmml_name, with_repr=True)

    def setValues(self, runVerison, ssn_field, snap_d_field, snap_d, **kw):
        regex = r'(?P<runType>[^.]+)(\.(?P<base_date>[^.]+))(\.(?P<snap_d>[^.]+))(\.(?P<col_version>[^.]+))?(\.(?P<ext>[^.]+))?(\.(?P<segment>[^.]+))?'
        m_dict = re.search(regex, runVerison).groupdict()
        m_dict['snapd'] = m_dict['snap_d'].replace('-', '')
        m_dict['project'] = mc.PROJECT

        self.runVerison = runVerison
        self.base_date = m_dict['base_date']

        if snap_d == 'NANNAN':
            self.snap_d = m_dict['snap_d']

        if 'period' not in kw:
            self.period = (datetime.datetime.strptime(self.snap_d, '%Y-%m-%d').replace(day=1) - datetime.timedelta(days=1)).strftime(
                '%Y-%m-%d')
        else:
            self.period = kw['period']

        self.period_in_table = self.period.replace('-', '')
        m_dict.update(period_in_table=self.period_in_table)

        # ------------------------------- #
        # m_dict update session completed
        # ------------------------------- #

        self.ssn_field = ssn_field
        self.snap_d_field = snap_d_field
        self.runType = m_dict['runType']
        self.col_version = '_v' + m_dict['col_version']
        self.ext = m_dict['ext']

        if 'segment' in m_dict:
            self.segment = m_dict['segment']
        else:
            self.segment = None

        if self.segment:
            self.model_data_file = mc.MODEL_PATH + '{project}_{base_date}_{snapd}_{segment}.csv'.format(**m_dict)
        else:
            self.model_data_file = mc.MODEL_PATH + '{project}_{base_date}_{snapd}.csv'.format(**m_dict)

        self.model_iv_file = self.model_data_file.rpartition('.')[0] + '_iv.xlsx'

        self.model_rescore_result_file = self.model_data_file.rpartition('.')[0] + '_{ext}{col_version}_result.csv'.format(ext=self.ext,
                                                                                              col_version=self.col_version)

        _table_dict = {}

        _table_dict['base_candidate_table'] = 'dept_risk.fyang_{project}_{base_date}_{snapd}'.format(**m_dict)

        _table_dict['app_attr_table'] = 'dept_risk.fyang_{project}_{base_date}_{snapd}_base_appstats_attr'.format(
            **m_dict)

        _table_dict['issue_attr_table'] = 'dept_risk.fyang_{project}_{base_date}_{snapd}_base_lastissued_attr'.format(
            **m_dict)

        _table_dict['nonpartner_table'] = 'dept_risk.fyang_{project}_{base_date}_{snapd}_base_nonpartner_attr'.format(
            **m_dict)

        _table_dict['partner_table'] = 'dept_risk.fyang_{project}_{base_date}_{snapd}_base_partner_attr'.format(
            **m_dict)

        _table_dict['dm_table'] = 'dept_risk.fyang_{project}_{base_date}_{snapd}_base_dm_attr'.format(**m_dict)

        _table_dict['mc_table'] = 'dept_risk.fyang_{project}_{base_date}_{snapd}_base_attr_final'.format(**m_dict)

        _table_dict['partner_come_table'] = 'dept_risk.fyang_parnter_take_rate_base_{base_date}_{snapd}'.format(
            **m_dict)

        _table_dict['response_table'] = 'risk_temp.fyang_{project}_{base_date}_{snapd}_response_flg'.format(
            **m_dict)

        if 'model_table_stella' in kw:
            _table_dict['model_table_stella'] = kw['model_table_stella']
        else:
            _table_dict['model_table_stella'] = 'dept_risk.fyang_{project}_{base_date}_{snapd}_model_attr_stella'.format(**m_dict)

        _table_dict['model_table'] = 'dept_risk.fyang_{project}_{base_date}_{snapd}_model_attr'.format(**m_dict)

        if 'ita_table' in kw:
            _table_dict['ita_table'] = kw['ita_table']
        else:
            _table_dict['ita_table'] = 'dept_risk.fyang_response_{period_in_table}_ita'.format(**m_dict)

        if 'ita_trade_table' in kw:
            _table_dict['ita_trade_table'] = kw['ita_trade_table']
        else:
            _table_dict['ita_trade_table'] = 'dept_risk.fyang_tu_dti_{base_date}_period_{period_in_table}'.format(**m_dict)

        if 'upload_table_input_file' in kw:
            _table_dict['upload_table_input_file'] = kw['upload_table_input_file']
        else:
            _table_dict['upload_table_input_file'] = 'dept_risk.fyang_mc_response_result_{snapd}'.format(**m_dict)

        if 'upload_table_result_file' in kw:
            _table_dict['upload_table_result_file'] = kw['upload_table_result_file']
        else:
            _table_dict['upload_table_result_file'] = 'dept_risk.fyang_mc_response_result_{snapd}'.format(**m_dict)

        # self.upload_table_result_file = .format(self.snapd)
        self.replace_dict.update(**_table_dict)
        self.__dict__.update(**_table_dict)

        # ssn_field=ssn_field
        # snap_d_field=snap_d_field
        # base_candidate_table=base_candidate_table
        # base_attr_table=app_attr_table


if __name__ == '__main__':
    utl.keep_to_log_file(log_path=mc.LOG_PATH)
    # a = mcResponse(runVerison='OOT.202112.2021-10-01.4.dt.bcq')
    a = mcResponse(runVerison='OOT.202112.2021-06-01.4.dt.bcq')
    # a = mcResponse(runVerison='OOT.202109.2021-06-01.4.dt.bcq')
    # a.createBaseView()
    # a.createResponseView()
    # a.createAppstatsView()
    # a.createLastissuedView()
    # a.createMcView()
    # a.createPartnerView()
    # a.createNonpartnerView()
    a.createITAView()
    # a.createDMView()
    a.createITAtradeView(substep=2)


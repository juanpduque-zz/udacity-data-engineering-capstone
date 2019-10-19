from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import pandas as pd
import re
import os


class ConvertCsv(BaseOperator):

    ui_color = '#FFC0CB'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):

        super(ConvertCsv, self).__init__(*args, **kwargs)

    def execute(self, context):

       with open("/airflow/data/I94_SAS_Labels_Descriptions.SAS", "r", encoding='utf-8') as main_file:
            file = main_file.read()

            sas_label_ext = {}
            temp_data = []
            attr_name = ''

            
            for line in file.split("\n"):
                line = re.sub(r"\s+|\t+|\r+", " ", line)

                if "/*" in line and "-" in line:
                    attr_name, attr_desc = [item.strip(" ") for item in
                                            line.split("*")[1].split(
                                                "-",
                                                1)]
                    attr_name = attr_name.replace(' & ', '&').lower()
                    if attr_name != '':
                        sas_label_ext[attr_name] = {'desc': attr_desc}
                elif '=' in line:
                    temp_data.append(
                        [item.strip(';').strip(" ").replace(
                            '\'', '').lstrip().rstrip().title() for item
                         in
                         line.split('=')])
                elif len(temp_data) > 0:
                    if attr_name != '':
                        sas_label_ext[attr_name]['data'] = temp_data
                        temp_data = []

            # country
            
            sas_label_ext['i94cit&i94res']['df'] = pd.DataFrame(
                sas_label_ext['i94cit&i94res']['data'],
                columns=['country_code', 'country_name'])

            # port
           
            tempdf = pd.DataFrame(sas_label_ext['i94port']['data'],
                                  columns=['port_code', 'port_name'])
            tempdf['port_code'] = tempdf['port_code'].str.upper()
            tempdf[['port_city', 'port_state']] = tempdf[
                'port_name'].str.rsplit(',', 1, expand=True)
            tempdf['port_state'] = tempdf['port_state'].str.upper()
            sas_label_ext['i94port']['df'] = tempdf

            # mode
            
            sas_label_ext['i94mode']['df'] = pd.DataFrame(
                sas_label_ext['i94mode']['data'],
                columns=['trans_code', 'trans_name'])
            tempdf = pd.DataFrame(sas_label_ext['i94addr']['data'],
                                  columns=['state_code', 'state_name'])
            tempdf['state_code'] = tempdf['state_code'].str.upper()

            # address
           
            sas_label_ext['i94addr']['df'] = tempdf

            # visa
           
            sas_label_ext['i94visa']['df'] = pd.DataFrame(
                sas_label_ext['i94visa']['data'],
                columns=['reason_code', 'reason_travel'])

            # write to csv
           
            for table in sas_label_ext.keys():
                if 'df' in sas_label_ext[table].keys():
                    with open(os.path.join("/airflow/data", table +
                                           ".csv"),
                              "w") as output_file:
                        sas_label_ext[table]['df'].to_csv(output_file,
                                                          index=False)
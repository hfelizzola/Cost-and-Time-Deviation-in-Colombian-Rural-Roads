"""
Utility funtions for ETL process
"""
# Author: Heriberto Felizzola Jimenez <ing.heriberto.felizzola@gmail.com>

import os
from sodapy import Socrata
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
pd.set_option('display.max_columns', None)

def extract_data(url='www.datos.gov.co', id_data='xvdy-vvsk', api_key='SzZ4759qpp2oHfKaHnlgR5T1n'):
    """
    Extract data from the Socrata API
    """
    
    # Create client
    socrata_token = os.environ.get(api_key)
    client = Socrata(url, socrata_token)

    # Query data
    query = """
    SELECT 
        uid,
        nombre_de_la_entidad,
        departamento_entidad,
        orden_entidad,
        tipo_de_proceso,
        objeto_a_contratar,
        UPPER(detalle_del_objeto_a_contratar) AS detalle_objeto,
        cuantia_proceso,
        cuantia_contrato,
        valor_total_de_adiciones,
        valor_contrato_con_adiciones,
        anno_firma_del_contrato AS anno_firma,
        fecha_de_firma_del_contrato AS fecha_fima,
        fecha_ini_ejec_contrato,
        plazo_de_ejec_del_contrato,
        rango_de_ejec_del_contrato,
        tiempo_adiciones_en_dias,
        tiempo_adiciones_en_meses,
        fecha_fin_ejec_contrato
    WHERE
        anno_firma IS NOT NULL
        AND fecha_fima IS NOT NULL
        AND detalle_objeto IS NOT NULL

        AND id_familia = '9511'
        AND estado_del_proceso = 'Liquidado'
        AND anno_firma NOT IN ('2010','2011','2012','2013','2021','2022')
        AND cuantia_proceso > 20000000
        AND cuantia_contrato > 20000000
        AND regimen_de_contratacion != 'Régimen Especial'
        AND tipo_de_proceso IN ('Licitación obra pública','Licitación Pública')

        AND detalle_objeto NOT LIKE '%MEZCLA ASFÁLTICA%'
        AND detalle_objeto NOT LIKE '%REDUCTORES DE VELOCIDAD%'
        AND detalle_objeto NOT LIKE '%MUROS DE CONTENCIÓN%'
        AND detalle_objeto NOT LIKE '%DEMOLICIÓN%'
        AND detalle_objeto NOT LIKE '%RESTAURACIÓN ESTACIÓN FÉRREA%'
        AND detalle_objeto NOT LIKE '%CONSTRUCCIÓN DE REDUCTORES DE VELOCIDAD%'
        AND detalle_objeto NOT LIKE '%REDUCTORES DE VELOCIDAD%'
        AND detalle_objeto NOT LIKE '%CONSTRUCCIÓN DE REDUCTORES DE VELOCIDAD%'
        AND detalle_objeto NOT LIKE '%BARANDAS%'
        AND detalle_objeto NOT LIKE '%MUROS DE CONTENCIÓN%'
        AND detalle_objeto NOT LIKE '%DEMOLICIÓN%'
        AND detalle_objeto NOT LIKE '%RESTAURACIÓN ESTACIÓN FÉRREA%'
        AND detalle_objeto NOT LIKE '%SEÑALIZACIÓN%'
        AND detalle_objeto NOT LIKE '%REHABILITACIÓN Y CONSERVACIÓN PUENTE%'
        AND detalle_objeto NOT LIKE '%MANTENIMIENTO TÚNEL%'
        AND detalle_objeto NOT LIKE '%ALCANTARILLA%'
        AND detalle_objeto NOT LIKE '%MANO DE OBRA%'
        AND detalle_objeto NOT LIKE '%BOX CULVERT%'
        AND detalle_objeto NOT LIKE '%PUENTES COLGANTES%'
        AND detalle_objeto NOT LIKE '%CASCO URBANO%'
        AND detalle_objeto NOT LIKE '%VÍAS URBANAS%'
        AND detalle_objeto NOT LIKE '%EJERCITO%'
        AND detalle_objeto NOT LIKE '%SEMAFORIZACIÓN%'
        AND detalle_objeto NOT LIKE '%DEMOLICIONES%'
        AND detalle_objeto NOT LIKE '%MURO%'
        AND detalle_objeto NOT LIKE '%CICLOVÍAS%'
        AND detalle_objeto NOT LIKE '%CICLORUTA%'
        AND detalle_objeto NOT LIKE '%RESIDUOS SÓLIDOS%'
        AND detalle_objeto NOT LIKE '%DESMONTE Y LIMPIEZA%'
        AND detalle_objeto NOT LIKE '%AULAS%'
        
    LIMIT
        1000
    """
    query_results = client.get(id_data, query=query)
    query_results = pd.DataFrame.from_dict(query_results)
    print("El numero de contratos extraidos: {}".format(query_results.shape[0]))
    return query_results


def process_data(df):
    """
    Process data
    """
    
    # Set data type 
    df = (df
     .astype({'cuantia_proceso':'float',
              'cuantia_contrato':'float',
              'valor_total_de_adiciones':'float',
              'valor_contrato_con_adiciones':'float',
              'anno_firma':'int32',
              'plazo_de_ejec_del_contrato':'int32',
              'tiempo_adiciones_en_dias':'int32',
              'tiempo_adiciones_en_meses':'int32',
              'fecha_fima':'datetime64[ns]',
              'fecha_ini_ejec_contrato':'datetime64[ns]',
              'fecha_fin_ejec_contrato':'datetime64[ns]'}))
    
    # Rename columns
    df.rename(columns={'cuantia_proceso':'ESTIMATED_COST',
                       'plazo_de_ejec_del_contrato':'ORIGINAL_DEADLINE',
                       'cuantia_contrato':'CONTRACT_VALUE',
                       'valor_contrato_con_adiciones':'FINAL_COST',
                       'valor_total_de_adiciones':'ADDITIONAL_COST',
                       'anno_firma':'YEAR',
                       'orden_entidad': 'MUNICIPALITY_TYPE',
                       'departamento_entidad': 'DEPARTMENT',
                       'fecha_fima':'CONTRACT_DATE',
                       'fecha_ini_ejec_contrato':'START_DATE',
                       'fecha_fin_ejec_contrato':'END_DATE',
                       'nombre_de_la_entidad':'ENTITY_NAME',
                       'municipio_entidad':'MUNICIPALITY',
                       'tipo_de_proceso':'PROCESS_TYPE',
                       'objeto_a_contratar':'CONTRACT_OBJECT',
                       'detalle_objeto':'OBJETC_DETAIL',
                       'uid':'CONTRACT_ID'}, 
              inplace=True)
    
    # Scale all values of contracto to monthly legal minimimun salary 
    cuantia_col = ['ESTIMATED_COST', 'CONTRACT_VALUE', 'ADDITIONAL_COST', 'FINAL_COST']
    
    salario_minimo = {2014:616000, 
                      2015:644350,
                      2016:689455,
                      2017:737717,
                      2018:781242,
                      2019:828116,
                      2020:877803}
    
    for col in cuantia_col:
        temp = [val/salario_minimo[anno] for val,anno in zip(df[col],df['YEAR'])]
        df[col] = temp
        del temp
    
    # Convert all duration to days 
    df.loc[df['rango_de_ejec_del_contrato'] == 'M', 'ORIGINAL_DEADLINE'] = df['ORIGINAL_DEADLINE']*30
    df.drop(columns=['rango_de_ejec_del_contrato'], inplace=True)

    # Unify additional time to days
    df['ADDITIONAL_TIME'] = df['tiempo_adiciones_en_dias'] + df['tiempo_adiciones_en_meses']*30
    df.drop(columns=['tiempo_adiciones_en_dias', 'tiempo_adiciones_en_meses'], inplace=True)

    # Calculate duration of project
    df['FINAL_DEADLINE'] = df['ORIGINAL_DEADLINE'] + df['ADDITIONAL_TIME']
    
    # Calculate project intensity
    df['PROJECT_INTENSITY'] = df['CONTRACT_VALUE']/df['ORIGINAL_DEADLINE']

    # Calculate award growth
    df['AWARD_GROWTH'] = ((df['CONTRACT_VALUE'] - df['ESTIMATED_COST'])/df['ESTIMATED_COST'])*100

    # Calculate cost deviation
    df['COST_DEVIATION'] = (df['FINAL_COST'] - df['CONTRACT_VALUE'])/df['CONTRACT_VALUE']

    # Calculate cost deviation 
    df['TIME_DEVIATION'] = (df['FINAL_DEADLINE'] - df['ORIGINAL_DEADLINE'])/df['ORIGINAL_DEADLINE']

    
    # Standardize owner
    df['OWNER'] = df['MUNICIPALITY_TYPE']
    original_municipality_type = ['TERRITORIAL DISTRITAL MUNICIPAL NIVEL 1',
                                  'TERRITORIAL DISTRITAL MUNICIPAL NIVEL 2',
                                  'TERRITORIAL DISTRITAL MUNICIPAL NIVEL 3',
                                  'TERRITORIAL DISTRITAL MUNICIPAL NIVEL 4',
                                  'TERRITORIAL DISTRITAL MUNICIPAL NIVEL 5',
                                  'TERRITORIAL DISTRITAL MUNICIPAL NIVEL 6',
                                  'TERRITORIAL DEPARTAMENTAL CENTRALIZADO',
                                  'TERRITORIAL DEPARTAMENTAL DESCENTRALIZADO',
                                  'DISTRITO CAPITAL',
                                  'NACIONAL CENTRALIZADO']

    new_municipality_type = ['TYPE_1', 'TYPE_2', 'TYPE_3', 'TYPE_4', 'TYPE_5', 'TYPE_6', 'OTHER', 'OTHER', 'OTHER', 'OTHER']
    df['MUNICIPALITY_TYPE'] = df['MUNICIPALITY_TYPE'].replace(original_municipality_type, new_municipality_type)

    # Reassign municipality type
    new_owner = ['MUNICIPALITY', 'MUNICIPALITY', 'MUNICIPALITY', 'MUNICIPALITY', 'MUNICIPALITY', 'MUNICIPALITY', 'DEPARTMENT_GOVERNMENT', 'DEPARTMENT_GOVERNMENT', 'OTHER', 'OTHER']
    df['OWNER'] =  df['OWNER'].replace(original_municipality_type, new_owner)

    # Assign regions
    AMAZONIA = ['Amazonas', 'Caquetá', 'Putumayo', 'Guainía', 'Guaviare', 'Vaupés']
    ORINOQUIA = ['Meta', 'Arauca', 'Casanare', 'Vichada']
    ANDINA = ['Antioquia', 'Boyacá', 'Caldas', 'Cundinamarca', 'Huila', 'Norte De Santander', 'Quindío', 'Risaralda', 'Santander', 'Tolima', 'Bogotá D.C.']
    CARIBE = ['Atlántico', 'Bolívar', 'Cesar', 'Córdoba', 'La Guajira', 'Magdalena', 'Sucre', 'San Andrés, Providencia y Santa Catalina']
    PACIFICA = ['Cauca', 'Valle del Cauca', 'Chocó', 'Nariño']

    df['REGION'] = df['DEPARTMENT'].apply(lambda x: 'AMAZONIA' if x in AMAZONIA else
                                            'ORINOQUIA' if x in ORINOQUIA else
                                            'ANDINA' if x in ANDINA else
                                            'CARIBE' if x in CARIBE else
                                            'PACIFICA' if x in PACIFICA else
                                            'OTRA')

    # Some columns to upper case
    df['DEPARTMENT'] = df['DEPARTMENT'].str.upper()
    df['PROCESS_TYPE'] = df['PROCESS_TYPE'].str.upper()
    df['CONTRACT_OBJECT'] = df['CONTRACT_OBJECT'].str.upper()   
    df['OBJETC_DETAIL'] = df['OBJETC_DETAIL'].str.upper()

    return df
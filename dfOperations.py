import pandas as pd
import re
from datetime import datetime, timedelta
from datetime import date
import math
import string


class dfOps:
    
    def ano_mes_col(df, ano_col_name='ano', mes_col_name='mes'):
        """
        Function to be applied on a Pandas DataFrame to create a YearMonth Column.

        Parameters
        ----------
        ano_col_name : str, optional
            Year column name present on the Data Frame
        mes_col_name : str, optional
            Year column name present on the Data Frame

        Returns
        -------
        Pandas Series
            inplace dataframe with a new str column
        """
        df['ano_mes'] = (df[ano_col_name].apply(str) + df[mes_col_name].apply(lambda mes: str(mes) if mes > 9 else '0' + str(mes))).apply(int)
        return 
        
    def try_force_col_types(df, date_match = None, skip_pattern = None,inplace = None, print_progress = None):
        """ 
        Função que força a conversão das colunas de um pd.DataFrame para número ou data(caso a coluna esteja no padrão 'date|data|Date|Data').
        Importante que as colunas de data possuam o ano primeiro.

        Parameters
        ----------
        df : pd.DataFrame, 
            List of date colnames to be converted
        date_match : str, 
            Regex pattern to identify date colnames to be converted; (defaut: 'date|data|Date|Data|dia|Dia|day|Day|timestamp')
        skip_pattern : str, 
            Regex pattern to identify colnames not to be converted; (defaut: '_id$|^id_|_id_|^ $')
        inplace : bool, 
            If function will be applied on the same object;(defaut: False)
        print_progress : bool, 
            Print for debug

        Returns
        -------
        Pandas Data Frame
            pd.DataFrame com suas colunas tratadas
        """

        if inplace is None:
            df = df.copy()
        if date_match is None:
            date_match = 'date|data|Date|Data|dia|Dia|day|Day|timestamp'
        if skip_pattern is None:
            skip_pattern = '_id$|^id_|_id_|^ $'
        if print_progress is None:
            print_progress = False
            
        for coluna in df.columns:
            try:
                if re.search(date_match, coluna):
                    df[coluna] = pd.to_datetime(df[coluna], yearfirst= True)
                else:
                    #Condicional que pula colunas identificadas no skip_pattern
                    if re.search(skip_pattern, coluna): 
                        if print_progress:
                            print('[try_force_col_types] Coluna_pulada_skip_pattern: ', coluna)#pulando colunas que possam ser IDs
                        continue
                    df[coluna] = pd.to_numeric(df[coluna])
            except:
                if print_progress:
                    print('[try_force_col_types] Coluna_pulada_tipo: ', coluna)
        return df
    
    def value_and_date_conv(df, colunas_datas = None, colunas_valor = None):
        """
        Function to be applied on a Pandas DataFrame that iterates over columns to try to convert them.

        Parameters
        ----------
        colunas_datas : list, 
            List of date colnames to be converted
        colunas_datas : list, 
            List of values colnames to be converted

        Returns
        -------
        Pandas Data Frame
            inplace dataframe with a new column
        """
        if colunas_datas is None:
            colunas_datas = []
        if colunas_valor is None:
            colunas_valor = []
        
        for col_datas in colunas_datas:
            df[col_datas] = pd.to_datetime(df[col_datas])
            
        for col_datas, col_valor in zip(colunas_datas, colunas_valor):
            try:
                df[col_valor] = df[col_valor].str.replace('.', '')
                df[col_valor] = df[col_valor].str.replace(',', '.')
            except:
                pass
            df[col_valor] = pd.to_numeric(df[col_valor])
        return 

    def dividir_parcelas_periodo(df,
                                date_col = 'data',
                                n_parcelas_col = 'parcelas',
                                value_col = 'valor',
                                print_progress = False):
        """
        Function to be applied on a Pandas DataFrame that generate new rows dividing the value by the number of parcelas.

        Parameters
        ----------
        df : Pandas Data Frame, 
            Database
        date_col : str, 
            List of values colnames to be converted
        n_parcelas_col : str, 
            List of values colnames to be converted
        value_col : str, 
            Total value colname to be divided by parcela values 
        print_progress : bool, 
            Print for debug

        Returns
        -------
        Pandas Data Frame
            dataframe
        """
    
        def add_months(mydate, plus_n_months, print_progress = False):
            """
            Sub-function used by dividir_parcelas_periodo.

            Parameters
            ----------
            mydate : datetime Obj, 
                datetime Obj
            plus_n_months : int, 
                Months to be added
            print_progress : bool, 
                Print for debug

            Returns
            -------
            datetime
                datetime value
            """
            day = mydate.day
            month = ((mydate.month + plus_n_months) % 12)
            year = mydate.year + ((mydate.month + plus_n_months) / 12.01)

            if (month == 2) & (day > 28):
                day = 28
            if day > 30:
                day = 30
            if month == 0:
                month = 12

            data_final = datetime(int(math.floor(year)), month, day)
            if print_progress:
                print('[add_months]', year, '/',month, '/',day)
                print('[add_months] output:',data_final)
            return data_final
    

        df = df.copy().reset_index()
        res = []
        for id_, date_value, parcela_value, valor_value  in zip(df.index, df[date_col],df[n_parcelas_col], df[value_col]) :
            
            final_value = valor_value / parcela_value
            if print_progress:
                print('########################')
                print('[dividir_parcelas_periodo]data: ', date_value)
                print('[dividir_parcelas_periodo]parcelas: ', parcela_value)
                print('[dividir_parcelas_periodo]valor: ', valor_value)
                print('[dividir_parcelas_periodo]valor da parcela: ', final_value)

            for parcelas in range(int(parcela_value)):
                res.append([id_, add_months(date_value, parcelas, print_progress = print_progress), parcelas, final_value])

        res_df = pd.DataFrame(res, columns=['id', date_col, n_parcelas_col, value_col]) 
        df.drop([date_col, n_parcelas_col, value_col], axis=1, inplace=True)
        df.index.name = 'id'
        df.reset_index(inplace=True)
        df = pd.merge(df, res_df, how='left', on='id')
        return df
        
    def pivot_calendario(df, 
                     key_colname_list, 
                     pivoted_colmns = ['mes'], 
                     value_colum = 'valor', 
                     sum_colmns = True, 
                     sum_colmns_cum = False, 
                     fill_na = 0 ):
        """
        Função pivota um DF agrupado pelo groupby e calcula um total da linha se necessário. 

        Parameters
        ----------
        df : datetime Obj, 
            datetime Obj
        key_colname_list : list, 
            Colnames to be used as key to group data
        pivoted_colmns : bool, 
            Print for debug
        value_colum : bool, 
            Value to be aggregated
        sum_colmns : bool, 
            Show sum of columns and rows
        sum_colmns_cum : bool, 
            Show comsum
        fill_na : int, 
            Fillna value

        Returns
        -------
        pd DataFrame
        """

        data = df.groupby(key_colname_list + pivoted_colmns,as_index=False)[value_colum].sum()

        #Adcionando coluna de total
        if sum_colmns:
            total = df.groupby(pivoted_colmns,as_index=False)[value_colum].sum()
            total_cum = total.copy()
            total_cum[value_colum] = total_cum[value_colum].cumsum()
            for item in key_colname_list:
                total[item] = '~Total'  
                total_cum[item] = '~Total_cum'
            if sum_colmns_cum:      
                data = pd.concat([data, total, total_cum],sort=True)
            else:
                data = pd.concat([data, total],sort=True)

            x = pd.pivot_table(data, values= value_colum , index = key_colname_list, columns = pivoted_colmns, aggfunc=np.sum, fill_value=0).round(2).reset_index()
            x.replace(0,0)
            x.sort_values(key_colname_list[0], inplace=True)
            x = x.set_index(key_colname_list)

            x['Total'] = x.sum(axis= 1)

        return x

    def tratamento_caracteres(texto_list, lista_replace = None):
        """
        Função:
            - Retira os espaços por '_';
            - Substitúi caracteres especiais;
            - Remove espaços em branco do final;
        Atualmente troca o seguinte padrão:
            (' ', '_'),
            (r'à|á|ã', 'a'),
            (r'ç', 'c'),
            (r'õ|ó|ò', 'o'),
            (r'é|ê', 'e'),
            (r'í|ì', 'i'),
            (r'ú|ù', 'u')
        Parameters
        ----------
        texto_list : str or list, 
            datetime Obj
        lista_replace: list
            List of tuples (defaut:
                            [(' ', '_'), (r'à|á|ã', 'a'), (r'ç', 'c'),
                            (r'õ|ó|ò', 'o'), (r'é|ê', 'e'), (r'í|ì', 'i'),
                            (r'ú|ù', 'u')])

        Returns
        -------
        datetime
            datetime value
        """

        #Lista que será substituída
        if lista_replace is None:
            lista_replace = [ (' ', '_'),
                            (r'à|á|ã', 'a'),
                            (r'ç', 'c'),
                            (r'õ|ó|ò', 'o'),
                            (r'é|ê', 'e'),
                            (r'í|ì', 'i'),
                            (r'ú|ù', 'u')]
        res = []

        if isinstance(texto_list, str):
            texto_list = [texto_list]
            is_str = True
        else:
            is_str = False

        for texto in texto_list:
            #Removendo caracteres
            for termo in lista_replace:
                texto = re.sub(termo[0], termo[1], texto.lower())
            try:
                while texto[-1] == '_':
                    texto = texto[0:-1]
            except:
                pass

            res.append(texto)
        #Convertendo para string caso o termo passado seja str
        if is_str:
            res = res[0]
        return res

        def a_day_in_previous_month(dt, months=1, format_str=True):
            for period in range(months):
                dt = dt.replace(day=1) - timedelta(days=1)
                dt = dt.replace(day=1)

            if format_str:
                dt = dt.strftime('%Y-%m-%d')

            return dt
    
    
    # def calculo_de_parcelas(df, parcelas_colname='numero_de_parcelas', valor_colname='valor' , div_valor_parcelas=True):

    # df = df.copy()

    # df['contagem_parcelas'] = df[parcelas_colname]

    # #Separando DF entre 1 parcela e mais do que 1
    # df_parcelas = df[(df[parcelas_colname] > 1)].copy()
    # df_parcelas_1 = df[(df[parcelas_colname] == 1)].copy()

    # #Divisão do montante pelas parcelas, caso True
    # if div_valor_parcelas:
    #     df_parcelas[valor_colname] = df_parcelas[valor_colname] / df_parcelas[parcelas_colname]

    # #Loop no DF de parcelas
    # df_temp = df_parcelas.copy()
    # for i in range(len(df_temp)):

    #     #Selecionando a linha e copiando
    #     df_copy_parcelas = df_temp.iloc[i:i+1,:].copy()

    #     #Loop que altera a data da linha
    #     for e in range(int(df_copy_parcelas[parcelas_colname][:1])-1):
    #     if int(df_copy_parcelas.mes) == 12:
    #         df_copy_parcelas.mes = 1
    #         df_copy_parcelas.ano = df_copy_parcelas.ano +  1
    #         df_copy_parcelas['contagem_parcelas'] = df_copy_parcelas[parcelas_colname][:1] - e - 1
    #     else:
    #         df_copy_parcelas.mes = df_copy_parcelas.mes + 1
    #         df_copy_parcelas['contagem_parcelas'] = df_copy_parcelas[parcelas_colname][:1] - e - 1
    #     df_parcelas = pd.concat([df_parcelas, df_copy_parcelas])

    # df_parcelas.dia = df_parcelas.dia.apply(lambda x: 28 if x > 28 else x)
    # #Juntando DF Parcelado com o Não parcelado    
    # df = pd.concat([df_parcelas, df_parcelas_1])  
    # df['Data'] = pd.to_datetime(df['ano'].apply(str) + '-' + df['mes'].apply(str) + '-' + df['dia'].apply(str))
    # df = df.set_index('Data')

    # return df

    # def pivot_calendario(df, col1, list_col_datas = ['mes'], value_colum = 'valor', Total = True, fill_na = 0 ):
    # """
    # Função pivota um DF agrupado pelo groupby e calcula um total da linha se necessário. 
    # """

    # data = df.groupby(col1 + list_col_datas,as_index=False)[value_colum].sum()

    # #Adcionando coluna de total
    # if Total:
    #     total = df.groupby(list_col_datas,as_index=False)[value_colum].sum()
    #     for item in col1:
    #     total[item] = '~Total'      
    #     data = pd.concat([data, total],sort=True)

    # x = pd.pivot_table(data, values= value_colum , index = col1, columns = list_col_datas, aggfunc=np.sum, fill_value=0).round(2).reset_index()
    # x.replace(0,fill_na)
    # x.sort_values(col1[0], inplace=True)
    # x = x.set_index(col1)

    # return x

    # def tratamento_caracteres(texto_list):
    # """
    # Função:
    #     - Retira os espaços por '_';
    #     - Substitúi caracteres especiais;
    #     - Remove espaços em branco do final;
    # """

    # #Lista que será substituída
    # lista_replace = [ (' ', '_'),
    #                 (r'à|á|ã', 'a'),
    #                 (r'ç', 'c'),
    #                 (r'õ|ó|ò', 'o'),
    #                 (r'é|ê', 'e'),
    #                 (r'í|ì', 'i'),
    #                 (r'ú|ù', 'u')]
    # res = []

    # if isinstance(texto_list, str):
    #     texto = texto_list.lower()

    #     for termo in lista_replace:
    #     texto = re.sub(termo[0], termo[1], texto)
        
    #     #Removendo último caracterte em branco
    #     while texto[-1] == '_':
    #     texto = texto[0:-1]

    #     res.append(texto)

    # else:

    #     for texto in texto_list:
    #     texto = texto.lower()

    #     for termo in lista_replace:
    #         texto = re.sub(termo[0], termo[1], texto)

    #     if texto[-1] == '_':
    #         texto = texto[0:-1]

    #     res.append(texto)
    # return res


    # def juros_cumulativo_mensal(juros, parcelas, montante):
    # amortizacao = montante / parcelas
    # res = []
    # juros_list = []
    # amort_list = [amortizacao] * parcelas
    # for parcela in range(parcelas):
    #     if parcela == 0:
    #     res.append(amortizacao)
    #     else:
    #     res.append(res[parcela - 1] * (1 + juros))

    # juros_list = np.array(res) - np.array(amort_list)
    # return  {'amortizacao': amort_list, 'juros' : juros_list, 'valor_parcela' : res}

    # def amortizacao_tabela_sac(juros, parcelas, montante):
    # amortizacao = montante / parcelas
    # res = []
    # juros_list = []
    # amort_list = []

    # for i in range(parcelas):
    #     juros_periodo = (juros * (montante - (i * amortizacao)) )
    #     juros_list.append(juros_periodo)
    #     amort_list.append(amortizacao)
    #     res.append(amortizacao + juros_periodo)
        
    # #1666,67 + 0,68%*(200000-1*1666,67) = 1.348,66
    # return {'amortizacao': amort_list, 'juros' : juros_list, 'valor_parcela' : res}

    # def amortizacao_tabela_price(juros, parcelas, montante):
    # res = [np.pmt(juros, parcelas, montante) * -1] * parcelas
    # juros_list = np.ipmt(juros, range(1, parcelas + 1), parcelas, montante) * -1
    # amort_list = np.ppmt(juros, range(1, parcelas + 1), parcelas, montante) * -1


    # return {'amortizacao': amort_list, 'juros' : juros_list, 'valor_parcela' : res} 



from __future__ import print_function

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Planilha de presencas
PRINCIPAL_SPREADSHEET_ID = '1bsAcaKQ7GZ6mvNhOor1NpPrt5LgTWVRWu8QDZGliPMo'
MEMBER_SPREADSHEET_ID = '1WHy58hFqdfAwBeoPeZVBw8Ouarsjwowc0wixdukkkx0'

FINAL = 'Planilha Final'
METRICS = 'Métricas'
# SAMPLE_RANGE_NAME = 'A1:F8'

# Possíveis valores da planilha
TIPOS = ['F','P','S','A']

# Cria um dicionário com as colunas nome e dentro dela as colunas F,P,S,A
# Exemplo {'Thais':{'F':0,P':0,S':0,A':0,}}
def init(nomes):

    dict_presencas = {}
    for nome in nomes:
        dict_presencas[nome] = {}

    return dict_presencas

def conecta_API():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds

def calculate_metrics(df):
    lista = []


    dia_mais_presentes = pd.NA
    percentual_mais_presentes = 0

    dia_menos_presentes = pd.NA
    percentual_menos_presentes = 1


    for col in df.columns[1:]:

        resumo = df[col].value_counts()
        presencas = 0
        total = 0
        atrasos = 0
        cedo = 0

        for r in resumo.keys():
            if r!='F':
                presencas += resumo[r]

            if r == 'A':
                atrasos += resumo[r]

            if r == 'S':
                cedo += resumo[r]

            total += resumo[r]

        percentual_presentes = presencas/total

    #     print(percentual_presentes,percentual_mais_presentes)
        if percentual_presentes > percentual_mais_presentes:
            percentual_mais_presentes = percentual_presentes
            dia_mais_presentes = col

        if percentual_presentes < percentual_menos_presentes:
            percentual_menos_presentes = percentual_presentes
            dia_menos_presentes = col

        percentual_atrasos = atrasos/total
        percentual_cedo = cedo/total

        lista.append({'Dia': col,'Presentes %':percentual_presentes,'Atrasos %':percentual_atrasos,'Saidas Cedo %':percentual_cedo})

    df_dados_por_dia =  pd.DataFrame.from_records(lista)


    teste = {'Período': 'Semestral',
     '% Médio de presencas':df_dados_por_dia['Presentes %'].mean(),
     'Média % Atrasos':df_dados_por_dia['Atrasos %'].mean(),
     'Média % Saidas Cedo':df_dados_por_dia['Saidas Cedo %'].mean(),
     'Dia mais participativo': dia_mais_presentes,
     'Max participacao':percentual_mais_presentes,
     'Dia menos participativo': dia_menos_presentes,
     'Min participacao':percentual_menos_presentes
    }


    df_dados_por_semestre =  pd.DataFrame.from_dict(teste, orient='index')
    return df_dados_por_dia, df_dados_por_semestre.T


def creat_tab(title,sheet,sheet_id):
    batch_update_spreadsheet_request_body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': title,
                    }
                }
            }]
        }
    sheet.batchUpdate(spreadsheetId=sheet_id, body=batch_update_spreadsheet_request_body).execute()
    
def metrics_per_member(df_non_transpose,nome):
    
    sheet = df_non_transpose[nome]
    resumo = sheet.value_counts()
    presencas = 0
    total = 0
    atrasos = 0
    cedo = 0

    for r in resumo.keys():
        if r!='F':
            presencas += resumo[r]

        if r == 'A':
            atrasos += resumo[r]

        if r == 'S':
            cedo += resumo[r]

        total += resumo[r]
    percentual_presentes = presencas/total
    percentual_atrasos = atrasos/total
    percentual_cedo = cedo/total
    
    teste = {'Presentes %':percentual_presentes,'Atrasos %':percentual_atrasos,'Saidas Cedo %':percentual_cedo}
    oi = pd.DataFrame.from_dict(teste, orient='index').T
    return oi

def write(df,range_,sheet,sheet_id):
    data = [df.columns.values.tolist()]
    data.extend(df.values.tolist())
    value_range_body = {"values": data}
    request = sheet.values().update(spreadsheetId=sheet_id, range=range_,valueInputOption='USER_ENTERED',body=value_range_body).execute()
    

def main():
    
    
    creds = conecta_API()

    try:
        # Com as credenciais cria o serviço para conecater API do sheets
        service = build('sheets', 'v4', credentials=creds)

        # Chama a API do cheets
        sheet = service.spreadsheets()


        # Acessa a planilha passada como parâmetro
        spreadsheet = sheet.get(spreadsheetId=PRINCIPAL_SPREADSHEET_ID, includeGridData=True).execute()
        
        all_tabs  = []
        # Itera por todas as abas da planilha e salva o nome 
        for s in spreadsheet['sheets']:
            # print(s['note'])
            all_tabs.append(s['properties']['title'])

        
        
        tabs = [FINAL,METRICS]
        for t in tabs:
            if t not in all_tabs:
                creat_tab(t,sheet,PRINCIPAL_SPREADSHEET_ID)
            else:
                all_tabs.remove(t)




        # checar se tem mais e add (vamos dizer que adicionou um nome dps...)

        # Para cada aba pega os valores e salva
        for i,tab in enumerate(all_tabs):
            
            # Pega os dados da aba
            result = sheet.values().get(spreadsheetId=PRINCIPAL_SPREADSHEET_ID,
                                    range=tab).execute()

#             print(result)
            values = result.get('values', [])
            

            # Na primeira planilha pega os nomes dos Petianos e inicia o dict
            if i == 0:
                nomes = []
                for row in values:
                    for i in range(0,len(row),2):
                        nomes.append(row[i])
        
                presenças_geral = init(nomes)

            for row in values:
                for j in range(0,len(row),2):
                    reuniao = tab.split('-')[1].strip()
                    try:
                        presenças_geral[row[j]][reuniao] = row[j+1]
                    except:
                        presenças_geral[row[j]] = {}
                        presenças_geral[row[j]][reuniao] = row[j+1]

        df = pd.DataFrame.from_records(presenças_geral)
        df_non_transpose = df
        df = df.T
        df = df.reset_index()
        df = df.rename(columns = {'index':'Nome'})

        df = df.replace(pd.NA,'')

        write(df,FINAL,sheet,PRINCIPAL_SPREADSHEET_ID)  
    
        df_dados_por_dia, df_dados_por_semestre = calculate_metrics(df)
        
        write(df_dados_por_semestre,METRICS,sheet,PRINCIPAL_SPREADSHEET_ID)
        write(df_dados_por_dia,f'{METRICS}!A4',sheet,PRINCIPAL_SPREADSHEET_ID)
        
        
        spreadsheet_member = sheet.get(spreadsheetId=MEMBER_SPREADSHEET_ID, includeGridData=True).execute()
        
        all_tabs_member  = []
        # Itera por todas as abas da planilha e salva o nome 
        for s in spreadsheet_member['sheets']:
            all_tabs_member.append(s['properties']['title'])

            

        for name in df['Nome']:
            metricas_membro = metrics_per_member(df_non_transpose,name)
            if name not in all_tabs_member:
                creat_tab(name,sheet,MEMBER_SPREADSHEET_ID)
            write(metricas_membro,name,sheet,MEMBER_SPREADSHEET_ID)
            df_member = df_non_transpose[[name]].reset_index()
            df_member = df_member.rename(columns = {'index':'Data',name:'Situação'})
            write(df_member,f'{name}!A4',sheet,MEMBER_SPREADSHEET_ID)
            
        print('cabou')

    except HttpError as err:
        print(err)


if __name__ == '__main__':
    main()


